package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func policyRuntimeRequest(t *testing.T, w *runtimeWire, req broker.Request) broker.Response {
	t.Helper()
	_ = w.SetDeadline(time.Now().Add(10 * time.Second))
	req.ID = "policy-runtime"
	if err := w.enc.Encode(req); err != nil {
		t.Fatal(err)
	}
	var response broker.Response
	if err := w.dec.Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.PolicyDigest) != 64 {
		t.Fatalf("decision has no policy snapshot: %+v", response)
	}
	return response
}

func TestRemoteBrokerPolicyIsolation(t *testing.T) {
	d, _, sshRun := newRemoteRuntime(t)
	names := []string{"policy-admin", "policy-holder-a", "policy-holder-b", "policy-queued", "policy-scoped", "policy-confused"}
	owners := make([]broker.Owner, len(names))
	p := broker.NewPolicy()
	for i, name := range names {
		owners[i] = broker.Owner{ClientID: name, ProjectID: "phase5-lifecycle"}
	}
	admin, queued, scoped, confused := owners[0], owners[3], owners[4], owners[5]
	if err := p.Grant(admin.Key(), "policy.grant"); err != nil {
		t.Fatal(err)
	}
	if err := p.Grant(admin.Key(), "audit_query"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range owners[1:5] {
		if err := p.GrantHost(owner.Key(), "runtime-host", "ping", "ping"); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.GrantHost(scoped.Key(), "runtime-host", "exec", "exec"); err != nil {
		t.Fatal(err)
	}
	if err := p.GrantCapability(confused.Key(), "file.read", "exec"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	before := p.DecideRequest(queued.Key(), "ping", "runtime-host")
	hostsPath := filepath.Join(d.dir, "hosts.json")
	data, err := os.ReadFile(hostsPath)
	if err != nil {
		t.Fatal(err)
	}
	var hosts struct {
		Hosts []map[string]any `json:"hosts"`
	}
	if err := json.Unmarshal(data, &hosts); err != nil {
		t.Fatal(err)
	}
	denied := make(map[string]any)
	for k, v := range hosts.Hosts[0] {
		denied[k] = v
	}
	denied["name"] = "denied-host"
	hosts.Hosts = append(hosts.Hosts, denied)
	data, _ = json.Marshal(hosts)
	if err := os.WriteFile(hostsPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(d.dir, "dial-gate")
	t.Cleanup(func() { _ = os.Remove(gate) })
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(gatedSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_TEST_DIAL_GATE="+gate)
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	administrator := d.dial(admin, d.token(admin, "5m"), true)
	first := startLifecycleProcess(t, d, owners[1], &proto.Request{Op: proto.OpPing}, false)
	awaitRuntime(t, 10*time.Second, "first real SSH startup barrier", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
	second := startLifecycleProcess(t, d, owners[2], &proto.Request{Op: proto.OpPing}, false)
	pending := startLifecycleProcess(t, d, queued, &proto.Request{Op: proto.OpPing}, false)
	awaitRuntime(t, time.Second, "queued request immutable decision", func() bool {
		data, _ := os.ReadFile(d.socket + ".audit")
		for _, line := range strings.Split(string(data), "\n") {
			var event broker.AuditEvent
			if json.Unmarshal([]byte(line), &event) == nil && event.Owner == broker.AuditOwnerID(queued.Key()) && event.Result == "admitted" {
				if event.PolicyDigest != before.Digest {
					t.Fatal("admission used wrong policy digest")
				}
				return true
			}
		}
		return false
	})
	if _, err := os.Stat(pending.path); !os.IsNotExist(err) {
		t.Fatal("request completed before blocked remote startup")
	}
	revoke := broker.Request{Owner: admin, Operation: "policy.grant", GrantOwner: queued, GrantHost: "runtime-host", GrantOperation: "ping", Revoke: true}
	if response := policyRuntimeRequest(t, administrator, revoke); !response.OK {
		t.Fatalf("revoke: %s", response.Error)
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	for _, child := range []*lifecycleProcess{first, second, pending} {
		result := child.result(t)
		if result.Ping == nil {
			t.Fatal("queued ping did not execute")
		}
		data, _ := os.ReadFile(child.path)
		var response broker.Response
		if err := json.Unmarshal(data, &response); err != nil {
			t.Fatal(err)
		}
		if response.PolicyDigest != before.Digest {
			t.Fatal("queued decision changed after revocation")
		}
	}
	deniedWire := d.dial(queued, d.token(queued, "5m"), true)
	response := policyRuntimeRequest(t, deniedWire, broker.Request{Owner: queued, Operation: "ping", Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if response.OK || response.PolicyDigest == before.Digest {
		t.Fatal("new request retained revoked permission")
	}
	afterRevoke := response.PolicyDigest
	// The successful revoke must already be durable before any graceful shutdown.
	d.stop(syscall.SIGKILL)
	d.start()
	deniedWire = d.dial(queued, d.token(queued, "5m"), true)
	response = policyRuntimeRequest(t, deniedWire, broker.Request{Owner: queued, Operation: "ping", Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if response.OK || response.PolicyDigest != afterRevoke {
		t.Fatal("SIGKILL lost acknowledged revoke")
	}
	administrator = d.dial(admin, d.token(admin, "5m"), true)
	grant := broker.Request{Owner: admin, Operation: "policy.grant", GrantOwner: queued, GrantHost: "runtime-host", GrantOperation: "ping"}
	if response = policyRuntimeRequest(t, administrator, grant); !response.OK {
		t.Fatalf("grant: %s", response.Error)
	}
	d.stop(syscall.SIGKILL)
	d.start()
	w := d.dial(queued, d.token(queued, "5m"), true)
	response = policyRuntimeRequest(t, w, broker.Request{Owner: queued, Operation: "ping", Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if !response.OK || response.Wire == nil || !response.Wire.OK || response.PolicyDigest != before.Digest {
		t.Fatal("SIGKILL lost acknowledged grant or stable digest")
	}
	administrator = d.dial(admin, d.token(admin, "5m"), true)
	scopedWire := d.dial(scoped, d.token(scoped, "5m"), true)
	for _, host := range []string{"runtime-host", "denied-host", "unregistered@host"} {
		response = policyRuntimeRequest(t, scopedWire, broker.Request{Owner: scoped, Operation: "ping", Host: host, Wire: &proto.Request{Op: proto.OpPing}})
		if response.OK != (host == "runtime-host") {
			t.Fatalf("host grant broadened or lost for %s: %+v", host, response)
		}
	}
	for _, op := range []string{"secret.use", "fleet.execute", "job_list"} {
		response = policyRuntimeRequest(t, scopedWire, broker.Request{Owner: scoped, Operation: op, Host: "runtime-host"})
		if response.OK {
			t.Fatalf("ungranted capability %s admitted", op)
		}
	}
	for _, id := range []string{"unknown-job", "another-owner-job"} {
		response = policyRuntimeRequest(t, scopedWire, broker.Request{Owner: scoped, Operation: proto.OpJobStatus, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: id}}})
		if response.OK || response.Error != "denied by default" {
			t.Fatal("ungranted job operation reached the registry or exposed job state")
		}
	}
	confusedWire := d.dial(confused, d.token(confused, "5m"), true)
	for _, hint := range []string{"", "file.read", "exec"} {
		response = policyRuntimeRequest(t, confusedWire, broker.Request{Owner: confused, Operation: "exec", Capability: hint, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"true"}}}})
		if response.OK {
			t.Fatalf("caller substituted capability %q", hint)
		}
	}
	otherProject := scoped
	otherProject.ProjectID = "different-project"
	otherWire := d.dial(otherProject, d.token(otherProject, "5m"), true)
	if response = policyRuntimeRequest(t, otherWire, broker.Request{Owner: otherProject, Operation: "ping", Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}}); response.OK {
		t.Fatal("same client ID inherited another project's host grant")
	}
	// A failed rename must neither acknowledge nor publish the attempted grant.
	path := d.socket + ".policy"
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	failGrant := broker.Request{Owner: admin, Operation: "policy.grant", GrantOwner: confused, GrantHost: "runtime-host", GrantOperation: "ping"}
	response = policyRuntimeRequest(t, administrator, failGrant)
	if response.OK {
		t.Fatal("policy persistence failure acknowledged")
	}
	response = policyRuntimeRequest(t, confusedWire, broker.Request{Owner: confused, Operation: "ping", Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if response.OK || response.PolicyDigest != before.Digest {
		t.Fatal("failed grant changed active policy")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	// Owner-scoped audit queries include their decision digest without exposing
	// any other principal's allowed/denied execution history.
	response = policyRuntimeRequest(t, administrator, broker.Request{Owner: admin, Operation: "audit_query"})
	for _, event := range response.Audit {
		if event.Owner != broker.AuditOwnerID(admin.Key()) || len(event.PolicyDigest) != 64 {
			t.Fatal("audit ownership/digest mismatch")
		}
	}
	if n := countDaemonSSHChildren(t, d.cmd.Process.Pid); n != 1 {
		t.Fatalf("unauthorized host opened a transport: ssh=%d", n)
	}
	data, err = sshRun(remoteAgentPIDScript)
	var remotePIDs []int
	if err != nil || json.Unmarshal(data, &remotePIDs) != nil || len(remotePIDs) != 1 {
		t.Fatalf("remote agent processes after crash recovery: %v %s", err, data)
	}
	t.Logf("real SSH policy: queued_decision=%s revoked_decision=%s grant_and_revoke_survived_SIGKILL=true exact_host_and_project_isolation=true caller_capability_substitution=denied persistence_failure=rolled_back audit_owner_and_digest=verified", before.Digest, afterRevoke)
}
