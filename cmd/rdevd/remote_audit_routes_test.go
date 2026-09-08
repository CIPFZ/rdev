package main

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestRemoteBrokerAuditRoutes(t *testing.T) {
	d, namespace, _ := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "audit-route-owner", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	policy := broker.NewPolicy()
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"status", "audit_query", "mutation.status"} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, op := range []string{"pool.health", "audit.health", "job.events", "policy.grant", "approval.create", proto.OpExec, proto.OpWriteFile, proto.OpJobStatus, proto.OpPing} {
		if err := policy.Grant(a.Key(), op); err != nil {
			t.Fatal(err)
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	wires := map[broker.Owner]*runtimeWire{}
	connect := func() {
		for _, owner := range []broker.Owner{a, b} {
			wires[owner] = d.dial(owner, d.token(owner, "5m"), true)
		}
	}
	connect()
	type expectation struct {
		owner             broker.Owner
		operation, digest string
		results           []string
	}
	expected := map[string]expectation{}
	const canary = "audit-raw-request-and-output-canary"
	call := func(owner broker.Owner, req broker.Request, ok bool, results ...string) broker.Response {
		t.Helper()
		req.Owner = owner
		req.ID = canary
		w := wires[owner]
		_ = w.SetDeadline(time.Now().Add(10 * time.Second))
		if err := w.enc.Encode(req); err != nil {
			t.Fatal(err)
		}
		var response broker.Response
		if err := w.dec.Decode(&response); err != nil {
			t.Fatal(err)
		}
		if response.OK != ok {
			t.Fatalf("unexpected response state for %s: %s", req.Operation, response.Error)
		}
		if len(response.RequestRef) != 64 || len(response.PolicyDigest) != 64 {
			t.Fatalf("missing server audit correlation for %s", req.Operation)
		}
		if _, exists := expected[response.RequestRef]; exists {
			t.Fatal("reused client ID conflated broker request traces")
		}
		expected[response.RequestRef] = expectation{owner, req.Operation, response.PolicyDigest, results}
		return response
	}
	call(a, broker.Request{Operation: "status"}, true, "accepted")
	call(a, broker.Request{Operation: "status"}, true, "accepted")
	for _, op := range []string{"pool.health", "audit.health"} {
		call(a, broker.Request{Operation: op}, true, "completed")
	}
	call(a, broker.Request{Operation: "mutation.status", MutationID: "op_0000000000000000"}, false, "request_rejected")
	call(a, broker.Request{Operation: "job.events"}, false, "dispatch_error")
	call(a, broker.Request{Operation: "policy.grant", GrantOperation: "status"}, false, "policy_update_failed")
	call(a, broker.Request{Operation: "approval.create"}, false, "request_rejected")
	call(a, broker.Request{Operation: proto.OpJobStatus, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: "unknown-job"}}}, false, "request_rejected")
	call(a, broker.Request{Operation: proto.OpWriteFile, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpWriteFile, OperationID: "bad-identity"}}, false, "request_rejected")
	call(a, broker.Request{Operation: proto.OpExec, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpExec, ClientID: "foreign"}}, false, "request_rejected")
	call(a, broker.Request{Operation: proto.OpPing, Wire: &proto.Request{Op: proto.OpPing}}, false, "route_rejected")
	call(a, broker.Request{Operation: "secret.use", Host: "runtime-host"}, false, "denied")
	call(a, broker.Request{Operation: "policy.grant", GrantOwner: b, GrantOperation: "job.events"}, true, "policy_updated")
	wire := &proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: "~/" + namespace + "/audit-route-proof", Content: canary, Append: true}}
	call(a, broker.Request{Operation: wire.Op, Host: "runtime-host", Wire: wire}, false, "approval_invalid")
	call(a, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: b, Operation: wire.Op, Host: "runtime-host", Wire: wire}}, false, "request_rejected")
	approval := call(a, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: a, Operation: wire.Op, Host: "runtime-host", Wire: wire}}, true, "approval_issued").Approval
	if approval == nil {
		t.Fatal("approval response missing")
	}
	mutation := call(a, broker.Request{Operation: wire.Op, Host: "runtime-host", Wire: wire, Approval: approval.Token}, true, "approval_used", "admitted", "completed")
	if mutation.Mutation == nil {
		t.Fatal("real append has no mutation intent")
	}
	query := call(a, broker.Request{Operation: "mutation.status", MutationID: mutation.Mutation.OperationID}, true, "completed")
	foreign := call(b, broker.Request{Operation: "mutation.status", MutationID: mutation.Mutation.OperationID}, false, "request_rejected")
	mutationRef := broker.OperationReference(broker.Request{Operation: "mutation.status", MutationID: mutation.Mutation.OperationID})
	verify := func() {
		t.Helper()
		for _, owner := range []broker.Owner{a, b} {
			response := call(owner, broker.Request{Operation: "audit_query"}, true, "completed")
			seen := map[string][]broker.AuditEvent{}
			for _, event := range response.Audit {
				if event.Owner != broker.AuditOwnerID(owner.Key()) {
					t.Fatal("request traces crossed exact project")
				}
				seen[event.RequestRef] = append(seen[event.RequestRef], event)
			}
			for ref, want := range expected {
				events := seen[ref]
				if want.owner != owner {
					if len(events) != 0 {
						t.Fatal("other project trace exposed")
					}
					continue
				}
				if len(events) != len(want.results) {
					t.Fatalf("%s trace has %d events want %d", want.operation, len(events), len(want.results))
				}
				for i, event := range events {
					if event.Result != want.results[i] || event.Operation != want.operation || event.PolicyDigest != want.digest {
						t.Fatalf("audit chain mismatch for %s: result %s", want.operation, event.Result)
					}
					if ref == mutation.RequestRef || ref == query.RequestRef || ref == foreign.RequestRef {
						if event.OperationRef != mutationRef {
							t.Fatal("mutation execution/query reference mismatch")
						}
					}
					if ref == mutation.RequestRef && (event.ApprovalID != broker.ApprovalReference(approval.Token) || event.RequestDigest != approval.Plan.RequestDigest || event.TargetDigest != approval.Plan.TargetDigest) {
						t.Fatal("approval/admission/result trace drift")
					}
				}
			}
		}
	}
	verify()
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	verify()
	for _, suffix := range []string{".audit", ".audit.1"} {
		data, err := os.ReadFile(d.socket + suffix)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), canary) || strings.Contains(string(data), approval.Token) {
			t.Fatal("audit persisted raw client ID/output/approval token")
		}
	}
	t.Log("actual daemon/SSH: repeated caller IDs have unique response/audit request references; local outcomes and pre-dispatch rejections traced; approval/admission/append/query share exact digests; all traces survive SIGKILL; same-client project isolation and payload/token privacy passed")
}
