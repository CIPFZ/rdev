package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func addRouteAlias(t *testing.T, d *runtimeDaemon) {
	t.Helper()
	path := filepath.Join(d.dir, "hosts.json")
	data, err := os.ReadFile(path)
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
	data, err = json.Marshal(hosts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteBrokerRoutes(t *testing.T) {
	t.Run("absent_and_malformed", func(t *testing.T) {
		d, _, _ := newRemoteRuntime(t)
		owner := broker.Owner{ClientID: "route-owner", ProjectID: "routes"}
		policy := broker.NewPolicy()
		operations := []string{"not-implemented", "secret.set", "secret.use", "sync.pull", "fleet.plan", proto.OpPing, "status", "pool.health", "audit.health", "audit_query", "mutation.status", "job.events"}
		for _, op := range operations {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
		if err := policy.Save(d.socket + ".policy"); err != nil {
			t.Fatal(err)
		}
		d.start()
		w := d.dial(owner, d.token(owner, "5m"), true)
		var rejected []broker.Request
		for _, op := range operations[:6] {
			rejected = append(rejected, broker.Request{Owner: owner, Operation: op, Host: "runtime-host"})
		}
		for _, op := range operations[6:] {
			rejected = append(rejected, broker.Request{Owner: owner, Operation: op, Host: "runtime-host", Wire: &proto.Request{Op: op}})
		}
		for _, op := range []string{"status", "pool.health", "audit.health", "audit_query"} {
			rejected = append(rejected, broker.Request{Owner: owner, Operation: op, Host: "runtime-host"})
		}
		rejected = append(rejected, broker.Request{Owner: owner, Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpExec}}, broker.Request{Owner: owner, Operation: proto.OpPing, Wire: &proto.Request{Op: proto.OpPing}})
		for _, req := range rejected {
			response := policyRuntimeRequest(t, w, req)
			if response.OK || response.Error == "" || response.Wire != nil || response.Scheduler != nil || response.Pool != nil || response.AuditHealth != nil || response.Mutation != nil || response.History != nil || len(response.Audit) > 0 {
				t.Errorf("malformed/absent route %s returned success or data", req.Operation)
			}
		}
		if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 {
			t.Fatal("rejected routes acquired SSH")
		}
		other := owner
		other.ProjectID = "different-project"
		denied := d.dial(other, d.token(other, "5m"), true)
		for _, op := range []string{"not-implemented", "status", "mutation.status", proto.OpJobStatus} {
			response := policyRuntimeRequest(t, denied, broker.Request{Owner: other, Operation: op, Host: "unregistered"})
			if response.OK || response.Error != "denied by default" {
				t.Fatal("default denial did not precede route/registry lookup")
			}
		}
		response := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "audit_query"})
		n := 0
		for _, event := range response.Audit {
			if event.Owner != broker.AuditOwnerID(owner.Key()) {
				t.Fatal("audit crossed project")
			}
			if event.Result == "route_rejected" {
				n++
			}
		}
		if n != len(rejected) {
			t.Errorf("route rejection audit count=%d want=%d", n, len(rejected))
		}
		response = policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
		if !response.OK || response.Wire == nil || response.Wire.Ping == nil {
			t.Fatal("valid wire ping failed")
		}
		if t.Failed() {
			return
		}
		t.Log("actual daemon: absent/malformed routes rejected before dial; policy denial precedes route lookup; owner-only rejection audit; valid remote ping preserved")
	})

	t.Run("scoped_policy", func(t *testing.T) {
		d, _, _ := newRemoteRuntime(t)
		addRouteAlias(t, d)
		admin := broker.Owner{ClientID: "route-admin", ProjectID: "routes"}
		target := broker.Owner{ClientID: "route-target", ProjectID: "routes"}
		policy := broker.NewPolicy()
		protected := broker.Owner{ClientID: "protected-grants", ProjectID: "routes"}
		if err := policy.Grant(protected.Key(), proto.OpPing); err != nil {
			t.Fatal(err)
		}
		if err := policy.GrantHost(protected.Key(), "other-host", "ping", proto.OpPing); err != nil {
			t.Fatal(err)
		}
		if err := policy.GrantHost(admin.Key(), "runtime-host", "policy.grant", "policy.grant"); err != nil {
			t.Fatal(err)
		}
		if err := policy.Save(d.socket + ".policy"); err != nil {
			t.Fatal(err)
		}
		d.start()
		w := d.dial(admin, d.token(admin, "5m"), true)
		before, err := os.ReadFile(d.socket + ".policy")
		if err != nil {
			t.Fatal(err)
		}
		for _, host := range []string{"", "other-host"} {
			for _, revoke := range []bool{false, true} {
				grantOwner := target
				if revoke {
					grantOwner = protected
				}
				response := policyRuntimeRequest(t, w, broker.Request{Owner: admin, Operation: "policy.grant", Host: "runtime-host", GrantOwner: grantOwner, GrantHost: host, GrantOperation: proto.OpPing, Revoke: revoke})
				if response.OK {
					t.Errorf("scoped policy changed target host=%q revoke=%v", host, revoke)
				}
			}
		}
		other := admin
		other.ProjectID = "other-project"
		otherWire := d.dial(other, d.token(other, "5m"), true)
		request := broker.Request{Owner: other, Operation: "policy.grant", Host: "runtime-host", GrantHost: "runtime-host", GrantOwner: target, GrantOperation: proto.OpPing}
		if policyRuntimeRequest(t, otherWire, request).OK {
			t.Fatal("project inherited admin authority")
		}
		request.Owner = admin
		request.Wire = &proto.Request{Op: "policy.grant"}
		if policyRuntimeRequest(t, w, request).OK {
			t.Error("local policy route accepted outer wire")
		}
		request.Wire = nil
		after, err := os.ReadFile(d.socket + ".policy")
		if err != nil {
			t.Fatal(err)
		}
		if string(before) != string(after) {
			t.Error("rejected administrative requests changed durable grants")
		}
		if !policyRuntimeRequest(t, w, request).OK {
			t.Fatal("same-host grant failed")
		}
		d.stop(syscall.SIGKILL)
		d.start()
		check := func(allowed bool) {
			t.Helper()
			client := d.dial(target, d.token(target, "5m"), true)
			defer client.Close()
			for _, host := range []string{"runtime-host", "other-host", "unregistered"} {
				r := policyRuntimeRequest(t, client, broker.Request{Owner: target, Operation: proto.OpPing, Host: host, Wire: &proto.Request{Op: proto.OpPing}})
				if r.OK != (allowed && host == "runtime-host") {
					t.Errorf("durable grant broadened/lost host=%s allowed=%v", host, allowed)
				}
			}
		}
		check(true)
		w = d.dial(admin, d.token(admin, "5m"), true)
		request.Revoke = true
		if !policyRuntimeRequest(t, w, request).OK {
			t.Fatal("same-host revoke failed")
		}
		d.stop(syscall.SIGKILL)
		d.start()
		check(false)
		if t.Failed() {
			return
		}
		t.Log("actual daemon: scoped admin cannot grant/revoke globally or another host; matching host grant/revoke survives SIGKILL; other project cannot administer; only intended host executes")
	})

	t.Run("scoped_approval_and_mutation", func(t *testing.T) {
		d, namespace, _ := newRemoteRuntime(t)
		addRouteAlias(t, d)
		admin := broker.Owner{ClientID: "approval-admin", ProjectID: "routes"}
		global := broker.Owner{ClientID: "global-approval-admin", ProjectID: "routes"}
		owner := broker.Owner{ClientID: "mutation-owner", ProjectID: "routes"}
		other := owner
		other.ProjectID = "other-project"
		policy := broker.NewPolicy()
		for _, grant := range []struct {
			owner    broker.Owner
			host, op string
		}{{admin, "runtime-host", "approval.create"}, {owner, "runtime-host", "mutation.status"}, {other, "runtime-host", "mutation.status"}} {
			if err := policy.GrantHost(grant.owner.Key(), grant.host, broker.CapabilityForOperation(grant.op), grant.op); err != nil {
				t.Fatal(err)
			}
		}
		for _, op := range []string{proto.OpWriteFile, proto.OpReadFile} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
		if err := policy.Grant(global.Key(), "approval.create"); err != nil {
			t.Fatal(err)
		}
		if err := policy.Save(d.socket + ".policy"); err != nil {
			t.Fatal(err)
		}
		d.start()
		w := d.dial(admin, d.token(admin, "5m"), true)
		gw := d.dial(global, d.token(global, "5m"), true)
		ow := d.dial(owner, d.token(owner, "5m"), true)
		wire := &proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: "~/" + namespace + "/route-append", Content: "route-canary", Append: true}}
		spec := &broker.ApprovalSpec{Owner: owner, Operation: wire.Op, Host: "other-host", Wire: wire}
		for _, outerWire := range []*proto.Request{nil, {Op: "approval.create"}} {
			r := policyRuntimeRequest(t, w, broker.Request{Owner: admin, Operation: "approval.create", Host: "runtime-host", Wire: outerWire, ApprovalSpec: spec})
			if r.OK || r.Approval != nil {
				t.Error("scoped admin issued another-host approval")
			}
		}
		otherAdmin := admin
		otherAdmin.ProjectID = "other-project"
		otherAdminWire := d.dial(otherAdmin, d.token(otherAdmin, "5m"), true)
		spec.Host = "runtime-host"
		if policyRuntimeRequest(t, otherAdminWire, broker.Request{Owner: otherAdmin, Operation: "approval.create", Host: "runtime-host", ApprovalSpec: spec}).OK {
			t.Error("approval authority crossed project")
		}
		if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 {
			t.Fatal("approval rejection acquired SSH")
		}
		var ids []string
		for _, host := range []string{"runtime-host", "other-host"} {
			issuer, issuerWire, scope := admin, w, "runtime-host"
			if host == "other-host" {
				issuer, issuerWire, scope = global, gw, ""
			}
			spec.Host = host
			issue := policyRuntimeRequest(t, issuerWire, broker.Request{Owner: issuer, Operation: "approval.create", Host: scope, ApprovalSpec: spec})
			if !issue.OK || issue.Approval == nil {
				t.Fatal("authorized approval failed")
			}
			// A malformed route cannot consume the valid approval token.
			malformed := broker.Request{Owner: owner, Operation: wire.Op, Host: host, Approval: issue.Approval.Token}
			if policyRuntimeRequest(t, ow, malformed).OK {
				t.Error("missing-wire mutation accepted")
			}
			malformed.Wire = wire
			r := policyRuntimeRequest(t, ow, malformed)
			if !r.OK || r.Wire == nil || !r.Wire.OK || r.Mutation == nil {
				t.Fatal("authorized append failed after route rejection")
			}
			ids = append(ids, r.Mutation.OperationID)
		}
		check := func() {
			t.Helper()
			client := d.dial(owner, d.token(owner, "5m"), true)
			defer client.Close()
			foreign := d.dial(other, d.token(other, "5m"), true)
			defer foreign.Close()
			query := broker.Request{Owner: owner, Operation: "mutation.status", Host: "runtime-host"}
			query.MutationID = ids[0]
			r := policyRuntimeRequest(t, client, query)
			if !r.OK || r.Mutation == nil || r.Mutation.Host != "runtime-host" {
				t.Fatal("same-host mutation query failed")
			}
			query.MutationID = ids[1]
			outside := policyRuntimeRequest(t, client, query)
			query.MutationID = "op-00000000000000000000000000000000"
			absent := policyRuntimeRequest(t, client, query)
			if outside.OK || outside.Mutation != nil || outside.Error != absent.Error {
				t.Error("host query exposed other-host mutation or existence")
			}
			query.Owner, query.MutationID = other, ids[0]
			r = policyRuntimeRequest(t, foreign, query)
			if r.OK || r.Mutation != nil || r.Error != absent.Error {
				t.Fatal("mutation query crossed project")
			}
		}
		check()
		d.stop(syscall.SIGKILL)
		d.start()
		check()
		ow = d.dial(owner, d.token(owner, "5m"), true)
		read := &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: wire.Cat.Path, Limit: 1024}}
		r := policyRuntimeRequest(t, ow, broker.Request{Owner: owner, Operation: read.Op, Host: "runtime-host", Wire: read})
		if !r.OK || r.Wire == nil || r.Wire.Read == nil || r.Wire.Read.Content != strings.Repeat("route-canary", 2) {
			t.Fatal("route rejection/approval/restart changed append count")
		}
		if t.Failed() {
			return
		}
		t.Log("actual daemon/SSH: host-scoped approval issuance bound to nested host; rejection preserves approval; two approved appends exactly once across crash; mutation outcomes filtered by host and project before/after SIGKILL, absent/out-of-scope indistinguishable")
	})
}
