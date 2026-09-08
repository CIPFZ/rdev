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

func TestRemoteBrokerApprovalIsolation(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	admin := runtimeApprovalAdmin()
	a, b := broker.Owner{ClientID: "approval-a", ProjectID: "phase5"}, broker.Owner{ClientID: "approval-b", ProjectID: "phase5"}
	policy := broker.NewPolicy()
	for _, op := range []string{"approval.create", "policy.grant"} {
		if err := policy.Grant(admin.Key(), op); err != nil {
			t.Fatal(err)
		}
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{proto.OpWriteFile, proto.OpReadFile, proto.OpExec, "audit_query"} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	hostFile := filepath.Join(d.dir, "hosts.json")
	data, err := os.ReadFile(hostFile)
	if err != nil {
		t.Fatal(err)
	}
	var hosts struct {
		Hosts []map[string]any `json:"hosts"`
	}
	if err := json.Unmarshal(data, &hosts); err != nil {
		t.Fatal(err)
	}
	alias := make(map[string]any)
	for k, v := range hosts.Hosts[0] {
		alias[k] = v
	}
	alias["name"] = "other-host"
	hosts.Hosts = append(hosts.Hosts, alias)
	data, _ = json.Marshal(hosts)
	if err := os.WriteFile(hostFile, data, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	administrator := d.dial(admin, d.token(admin, "5m"), true)
	issue := func(owner broker.Owner, wire *proto.Request, ttl time.Duration) *broker.Approval {
		t.Helper()
		response := policyRuntimeRequest(t, administrator, broker.Request{Owner: admin, Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire, TTL: ttl}})
		if !response.OK || response.Approval == nil {
			t.Fatalf("approval issuance: %s", response.Error)
		}
		return response.Approval
	}
	call := func(owner broker.Owner, wire *proto.Request, host, token string) broker.Response {
		t.Helper()
		w := d.dial(owner, d.token(owner, "5m"), true)
		defer w.Close()
		return policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: wire.Op, Host: host, Wire: wire, Approval: token, Risk: false, Target: "unchanged-client-hint"})
	}
	const canary = "approval-private-content-canary"
	wire := &proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: "~/" + namespace + "/approved", Content: canary, Append: true}}
	if call(a, wire, "runtime-host", "").OK {
		t.Fatal("Risk=false bypassed approval")
	}
	if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 {
		t.Fatal("unapproved write acquired a transport")
	}
	unprivileged := d.dial(a, d.token(a, "5m"), true)
	response := policyRuntimeRequest(t, unprivileged, broker.Request{Owner: a, Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: a, Operation: wire.Op, Host: "runtime-host", Wire: wire}})
	if response.OK {
		t.Fatal("executor minted its own approval")
	}
	approval := issue(a, wire, time.Minute)
	if call(b, wire, "runtime-host", approval.Token).OK {
		t.Fatal("other owner used approval")
	}
	if call(a, wire, "other-host", approval.Token).OK {
		t.Fatal("other permitted host used approval")
	}
	for _, change := range []string{"path", "content", "append", "operation"} {
		data, _ := json.Marshal(wire)
		var changed proto.Request
		if err := json.Unmarshal(data, &changed); err != nil {
			t.Fatal(err)
		}
		switch change {
		case "path":
			changed.Cat.Path = "~/" + namespace + "/swapped"
		case "content":
			changed.Cat.Content = "swapped"
		case "append":
			changed.Cat.Append = false
		case "operation":
			changed = proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"true"}}}
		}
		if call(a, &changed, "runtime-host", approval.Token).OK {
			t.Fatalf("approval changed %s", change)
		}
	}
	response = call(a, wire, "runtime-host", approval.Token)
	if !response.OK || response.Wire == nil || !response.Wire.OK {
		t.Fatalf("correct approval: %s", response.Error)
	}
	if call(a, wire, "runtime-host", approval.Token).OK {
		t.Fatal("approval replayed")
	}
	read := &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: wire.Cat.Path, Limit: 1024}}
	response = call(a, read, "runtime-host", "")
	if !response.OK || response.Wire == nil || response.Wire.Read == nil || response.Wire.Read.Content != canary {
		t.Fatal("approved append did not execute exactly once")
	}
	expired := issue(a, wire, 20*time.Millisecond)
	time.Sleep(40 * time.Millisecond)
	if call(a, wire, "runtime-host", expired.Token).OK {
		t.Fatal("expired approval accepted")
	}
	oldPolicy := issue(a, wire, time.Minute)
	response = policyRuntimeRequest(t, administrator, broker.Request{Owner: admin, Operation: "policy.grant", GrantOwner: a, GrantOperation: "status"})
	if !response.OK {
		t.Fatal("policy mutation failed")
	}
	if call(a, wire, "runtime-host", oldPolicy.Token).OK {
		t.Fatal("approval outlived changed policy digest")
	}
	beforeCrash := issue(a, wire, time.Minute)
	d.stop(syscall.SIGKILL)
	d.start()
	if call(a, wire, "runtime-host", beforeCrash.Token).OK {
		t.Fatal("old approval revived after crash")
	}
	response = call(a, read, "runtime-host", "")
	if !response.OK || response.Wire.Read.Content != canary {
		t.Fatal("crash/rejected approvals duplicated mutation")
	}
	// The authenticated owner's history contains its own approval use/result and
	// matches the administrator's low-sensitivity issuance reference on disk.
	w := d.dial(a, d.token(a, "5m"), true)
	response = policyRuntimeRequest(t, w, broker.Request{Owner: a, Operation: "audit_query"})
	approvedRef := broker.ApprovalReference(approval.Token)
	used, completed := false, false
	for _, event := range response.Audit {
		if event.Owner != broker.AuditOwnerID(a.Key()) {
			t.Fatal("audit query crossed principals")
		}
		if event.ApprovalID == approvedRef {
			if event.RequestDigest != approval.Plan.RequestDigest || event.TargetDigest != approval.Plan.TargetDigest {
				t.Fatal("approval correlation drifted")
			}
			used = used || event.Result == "approval_used"
			completed = completed || event.Result == "completed"
		}
	}
	if !used || !completed {
		t.Fatal("approval/result correlation missing after restart")
	}
	audit, err := os.ReadFile(d.socket + ".audit")
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{canary, approval.Token, expired.Token, oldPolicy.Token, beforeCrash.Token} {
		if strings.Contains(string(audit), secret) {
			t.Fatal("audit retained payload or raw approval token")
		}
	}
	data, err = sshRun(remoteAgentPIDScript)
	var pids []int
	if err != nil || json.Unmarshal(data, &pids) != nil || len(pids) != 2 {
		t.Fatal("expected one base agent and one dedicated bulk agent after approved file I/O")
	}
	t.Log("real SSH approval: Risk=false denied; issuer separated from executor; owner/host/path/content/append/operation substitutions denied without consuming valid approval; one append; replay/expiry/policy-change/crash denied; owner audit correlation and payload/token privacy verified")
}
