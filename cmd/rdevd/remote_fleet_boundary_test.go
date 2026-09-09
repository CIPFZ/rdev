package main

import (
	"encoding/json"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

// Fleet routes must reject malformed or substituted requests before touching a
// plan, approval, mutation ledger or SSH, including explicitly granted callers.
// Ordinary execution authority never implies Fleet authority in another project.
func TestRemoteBrokerFleetBoundary(t *testing.T) {
	d, namespace, ssh := newRemoteRuntime(t)
	granted := broker.Owner{ClientID: "fleet-boundary-client", ProjectID: "granted"}
	denied := broker.Owner{ClientID: granted.ClientID, ProjectID: "denied"}
	admin := runtimeApprovalAdmin()
	ops := []string{"fleet.plan", "fleet.execute", "fleet.approve"}
	p := broker.NewPolicy()
	for _, op := range ops {
		if err := p.Grant(granted.Key(), op); err != nil {
			t.Fatal(err)
		}
	}
	for _, owner := range []broker.Owner{granted, denied} {
		for _, op := range []string{proto.OpExec, proto.OpPing} {
			if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Grant(owner.Key(), "audit_query"); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Grant(admin.Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	run := func() {
		t.Helper()
		wires := map[broker.Owner]*runtimeWire{}
		for _, o := range []broker.Owner{granted, denied, admin} {
			wires[o] = d.dial(o, d.token(o, "5m"), true)
			defer wires[o].Close()
		}
		call := func(o broker.Owner, req broker.Request) broker.Response {
			req.Owner = o
			return policyRuntimeRequest(t, wires[o], req)
		}
		id, err := proto.NewOperationID()
		if err != nil {
			t.Fatal(err)
		}
		command := "from pathlib import Path; p=Path.home()/'" + namespace + "/fleet-boundary-marker'; p.write_text(p.read_text()+'x' if p.exists() else 'x')"
		valid := &proto.Request{Op: proto.OpExec, OperationID: id, Exec: &proto.ExecParams{Argv: []string{"python3", "-c", command}}}
		approval := call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: granted, Operation: proto.OpExec, Host: "runtime-host", Wire: valid, TTL: time.Minute}})
		if !approval.OK || approval.Approval == nil {
			t.Fatal("ordinary execution approval failed")
		}
		type rejection struct {
			result   string
			decision string
			digest   string
		}
		expected := map[broker.Owner]map[string]rejection{granted: {}, denied: {}}
		for _, o := range []broker.Owner{granted, denied} {
			for _, op := range ops {
				for _, wire := range []*proto.Request{nil, {Op: op}, valid} {
					for _, cap := range []string{"", "fleet", broker.CapabilityForOperation(proto.OpExec)} {
						r := call(o, broker.Request{Operation: op, Host: "runtime-host", Capability: cap, Wire: wire, Risk: false, Approval: approval.Approval.Token, OperationID: id})
						if r.OK || r.Error == "" || r.Wire != nil || r.Approval != nil || r.Fleet != nil || r.Fleets != nil || r.Inventory != nil || r.Mutation != nil || r.Scheduler != nil || r.Pool != nil || r.Ingress != nil || r.SharedWaits != nil || r.AuditHealth != nil || r.History != nil || len(r.Secrets) != 0 || len(r.Audit) != 0 || r.RequestRef == "" || r.PolicyDigest == "" {
							t.Fatal("malformed Fleet operation returned authority or data")
						}
						reason := "route_rejected"
						if cap != "" && cap != "fleet" {
							reason = "denied"
							if r.Error != "capability mismatch" {
								t.Fatal("client capability was not checked")
							}
						} else if o == denied {
							reason = "denied"
							if r.Error != "denied by default" {
								t.Fatal("other project inherited Fleet authority")
							}
						} else if r.Error == "unsupported broker operation" {
							t.Fatal("implemented Fleet route was not validated")
						}
						decision := "allow"
						if reason == "denied" {
							decision = r.Error
						}
						expected[o][r.RequestRef] = rejection{result: reason, decision: decision, digest: r.PolicyDigest}
					}
				}
				for _, wire := range []*proto.Request{nil, {Op: op}, valid} {
					r := call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: o, Operation: op, Host: "runtime-host", Wire: wire, TTL: time.Minute}})
					if r.OK || r.Approval != nil {
						t.Fatal("Fleet operation received an ordinary single-host approval")
					}
				}
			}
		}
		if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 {
			t.Fatal("Fleet rejection acquired SSH")
		}
		raw, err := os.ReadFile(d.socket + ".mutations")
		if err != nil {
			t.Fatal(err)
		}
		var state struct{ Records []json.RawMessage }
		if json.Unmarshal(raw, &state) != nil {
			t.Fatal("mutation state unreadable")
		}
		for _, record := range state.Records {
			if strings.Contains(string(record), id) {
				t.Fatal("rejected Fleet request created mutation state")
			}
		}
		fleetData, err := os.ReadFile(d.socket + ".fleet")
		var fleetState struct{ Plans []json.RawMessage }
		if err != nil || json.Unmarshal(fleetData, &fleetState) != nil || len(fleetState.Plans) != 0 {
			t.Fatal("rejected Fleet substitutions changed durable plans")
		}
		for _, o := range []broker.Owner{granted, denied} {
			r := call(o, broker.Request{Operation: "audit_query"})
			if !r.OK {
				t.Fatal("owner audit query failed")
			}
			seen := map[string]bool{}
			for _, event := range r.Audit {
				if event.Owner != broker.AuditOwnerID(o.Key()) {
					t.Fatal("Fleet denial audit crossed project")
				}
				if reason, ok := expected[o][event.RequestRef]; ok {
					if event.Result != reason.result || event.Decision != reason.decision || event.PolicyDigest != reason.digest {
						t.Fatal("Fleet decision/result correlation missing")
					}
					seen[event.RequestRef] = true
				}
			}
			if len(seen) != len(expected[o]) {
				t.Fatal("missing per-request Fleet rejection evidence")
			}
			data, _ := json.Marshal(r.Audit)
			if strings.Contains(string(data), approval.Approval.Token) || strings.Contains(string(data), command) {
				t.Fatal("Fleet audit disclosed approval or raw command")
			}
		}
		// The substituted requests must not consume the valid approval or reserve its
		// operation ID. Execute once, then refuse replay of the consumed approval.
		r := call(granted, broker.Request{Operation: proto.OpExec, Host: "runtime-host", Wire: valid, Approval: approval.Approval.Token})
		if !r.OK || r.Wire == nil || r.Wire.Exec == nil || r.Wire.Exec.ExitCode != 0 {
			t.Fatal("Fleet rejection consumed ordinary approval or operation ID")
		}
		if call(granted, broker.Request{Operation: proto.OpExec, Host: "runtime-host", Wire: valid, Approval: approval.Approval.Token}).OK {
			t.Fatal("consumed ordinary approval was replayed")
		}
	}
	run()
	out, err := ssh("from pathlib import Path\np=Path.home()/'" + namespace + "/fleet-boundary-marker'\nassert p.read_text()=='x'\n")
	if err != nil {
		t.Fatalf("first execution count: %v %s", err, out)
	}
	d.stop(syscall.SIGKILL)
	d.start()
	run()
	out, err = ssh("from pathlib import Path\np=Path.home()/'" + namespace + "/fleet-boundary-marker'\nassert p.read_text()=='xx'\n")
	if err != nil {
		t.Fatalf("post-restart execution count: %v %s", err, out)
	}
	t.Log("real daemon/SSH: fleet.plan/execute/approve default deny across projects; granted malformed routes reject before dispatch; capability/inner-wire/ordinary-approval substitutions create no SSH, plan or mutation; exact owner request/result audit; valid ordinary approval and stable operation ID remain usable exactly once; same boundary after SIGKILL")
}
