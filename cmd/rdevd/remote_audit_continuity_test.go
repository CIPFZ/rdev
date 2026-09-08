package main

import (
	"encoding/json"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestRemoteBrokerAuditContinuity(t *testing.T) {
	d, _, _ := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "audit-continuity", ProjectID: "a"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "b"}
	admin := broker.Owner{ClientID: "audit-health", ProjectID: "operations"}
	p := broker.NewPolicy()
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"status", "audit_query"} {
			if err := p.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(proto.OpPing), proto.OpPing); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Grant(admin.Key(), "audit.health"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	tokens := map[broker.Owner]string{}
	for _, owner := range []broker.Owner{a, b, admin} {
		tokens[owner] = d.token(owner, "5m")
	}
	const canary = "audit-continuity-payload-must-never-be-persisted"
	marker := d.socket + ".audit.continuity"
	inspectMarker := func(active, incomplete bool, count uint64) {
		data, err := os.ReadFile(marker)
		var state struct {
			Schema     int    `json:"schema"`
			Active     bool   `json:"active"`
			Incomplete bool   `json:"incomplete"`
			Count      uint64 `json:"unclean_recoveries"`
		}
		if err != nil || len(data) > 1024 || json.Unmarshal(data, &state) != nil || state.Schema != 1 || state.Active != active || state.Incomplete != incomplete || state.Count != count {
			t.Fatal("durable audit continuity marker wrong")
		}
		st, err := os.Stat(marker)
		if err != nil || st.Mode().Perm() != 0600 || strings.Contains(string(data), canary) || strings.Contains(string(data), a.ClientID) {
			t.Fatal("audit marker permissions/privacy failed")
		}
	}
	check := func(incomplete bool, count uint64) {
		for _, owner := range []broker.Owner{a, b} {
			w := d.dial(owner, tokens[owner], true)
			r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}, Target: canary})
			if !r.OK || r.Wire == nil || r.Wire.Ping == nil {
				t.Fatal("actual remote ping failed")
			}
			r = policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "audit_query"})
			if !r.OK || len(r.Audit) == 0 || r.AuditIncomplete != incomplete {
				t.Fatal("owner query hid or invented audit gap")
			}
			for _, event := range r.Audit {
				if event.Owner != broker.AuditOwnerID(owner.Key()) {
					t.Fatal("audit recovery crossed project")
				}
			}
			r = policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "audit.health"})
			if r.OK || r.AuditHealth != nil {
				t.Fatal("ordinary owner read global audit recovery counts")
			}
			w.Close()
		}
		w := d.dial(admin, tokens[admin], true)
		r := policyRuntimeRequest(t, w, broker.Request{Owner: admin, Operation: "audit.health"})
		w.Close()
		if !r.OK || r.AuditHealth == nil || r.AuditHealth.Incomplete != incomplete || r.AuditHealth.UncleanRecoveries != count || r.AuditHealth.Errors != 0 || r.AuditHealth.Dropped != 0 {
			t.Fatal("audit health recovery state wrong")
		}
		for _, suffix := range []string{".audit", ".audit.continuity"} {
			data, err := os.ReadFile(d.socket + suffix)
			if err != nil || strings.Contains(string(data), canary) {
				t.Fatal("audit metadata retained request payload")
			}
		}
	}
	d.start()
	check(false, 0)
	d.stop(syscall.SIGTERM)
	inspectMarker(false, false, 0)
	d.start()
	check(false, 0)
	// SIGKILL cannot run the writer's seal. A complete final NDJSON line is not
	// grounds for claiming that the former in-memory queue was fully persisted.
	d.stop(syscall.SIGKILL)
	inspectMarker(true, false, 0)
	d.start()
	check(true, 1)
	d.stop(syscall.SIGTERM)
	inspectMarker(false, true, 1)
	d.start()
	check(true, 1)
	// Make only the final continuity publication fail. The prior open marker
	// must survive and force an explicit gap on the next successful startup.
	if err := os.Rename(marker, marker+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0700); err != nil {
		t.Fatal(err)
	}
	d.stop(syscall.SIGTERM)
	waitDaemonLog(t, d, "continuity_commit_failed")
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(marker+".saved", marker); err != nil {
		t.Fatal(err)
	}
	d.start()
	check(true, 2)
	d.stop(syscall.SIGTERM)
	inspectMarker(false, true, 2)
	t.Log("actual daemon/OpenSSH audit continuity: clean exit/restart remains complete; SIGKILL leaves durable open marker and explicit possible-tail-loss flag; later clean restart preserves gap; real marker publication failure recovered as another unclean run; exact-project audit history and separately authorized health; no payload/owner names in private bounded marker")
}

func TestRemoteBrokerAuditPredecessorRecovery(t *testing.T) {
	predecessor := os.Getenv("RDEV_TEST_PREDECESSOR_DAEMON_BINARY")
	if predecessor == "" {
		t.Skip("set RDEV_TEST_PREDECESSOR_DAEMON_BINARY for actual downgrade/upgrade coverage")
	}
	d, _, _ := newRemoteRuntime(t)
	current := d.bin
	a := broker.Owner{ClientID: "audit-upgrade", ProjectID: "a"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "b"}
	admin := broker.Owner{ClientID: "audit-upgrade-health", ProjectID: "operations"}
	p := broker.NewPolicy()
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"status", "audit_query"} {
			if err := p.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(proto.OpPing), proto.OpPing); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Grant(admin.Key(), "audit.health"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	tokens := map[broker.Owner]string{}
	for _, owner := range []broker.Owner{a, b, admin} {
		tokens[owner] = d.token(owner, "5m")
	}
	exercise := func(expectGap bool) {
		for _, owner := range []broker.Owner{a, b} {
			w := d.dial(owner, tokens[owner], true)
			r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
			if !r.OK || r.Wire == nil || r.Wire.Ping == nil {
				t.Fatal("upgrade remote ping failed")
			}
			r = policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "audit_query"})
			w.Close()
			if !r.OK || r.AuditIncomplete != expectGap || len(r.Audit) == 0 {
				t.Fatal("upgrade audit completeness wrong")
			}
			for _, event := range r.Audit {
				if event.Owner != broker.AuditOwnerID(owner.Key()) {
					t.Fatal("upgrade audit history crossed project")
				}
			}
		}
	}
	d.start()
	exercise(false)
	d.stop(syscall.SIGTERM)
	marker := d.socket + ".audit.continuity"
	sealed, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	d.bin = predecessor
	d.start()
	exercise(false)
	d.stop(syscall.SIGTERM)
	unchanged, err := os.ReadFile(marker)
	if err != nil || string(unchanged) != string(sealed) {
		t.Fatal("test predecessor unexpectedly understands continuity marker")
	}
	d.bin = current
	d.start()
	exercise(true)
	w := d.dial(admin, tokens[admin], true)
	r := policyRuntimeRequest(t, w, broker.Request{Owner: admin, Operation: "audit.health"})
	w.Close()
	if !r.OK || r.AuditHealth == nil || !r.AuditHealth.Incomplete || r.AuditHealth.UncleanRecoveries != 0 {
		t.Fatal("predecessor seal mismatch not distinguished from durable open-marker recovery")
	}
	d.stop(syscall.SIGTERM)
	t.Log("actual daemon downgrade/upgrade: current clean seal retained while predecessor wrote both projects' real remote requests; unchanged old marker cannot hide segment digest changes; re-upgrade reports incomplete history with exact-owner queries, without inventing an open-marker crash count")
}
