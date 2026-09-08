package broker

import (
	"github.com/CIPFZ/rdev/internal/proto"
	"testing"
)

func TestRouteEnvelopes(t *testing.T) {
	for _, op := range []string{"status", "pool.health", "audit.health", "audit_query", "mutation.status", "job.events", "policy.grant", "approval.create"} {
		t.Run(op, func(t *testing.T) {
			if err := ValidateRoute(Request{Operation: op}); err != nil {
				t.Fatal(err)
			}
			if err := ValidateRoute(Request{Operation: op, Wire: &proto.Request{Op: op}}); err == nil {
				t.Fatal("local handler accepted outer wire")
			}
		})
	}
	for _, d := range proto.Operations() {
		t.Run(d.Name, func(t *testing.T) {
			r := Request{Operation: d.Name, Host: "a", Wire: &proto.Request{Op: d.Name}}
			if err := ValidateRoute(r); err != nil {
				t.Fatal(err)
			}
			r.Wire = nil
			if ValidateRoute(r) == nil {
				t.Fatal("missing wire accepted")
			}
			r.Wire = &proto.Request{Op: "unknown"}
			if ValidateRoute(r) == nil {
				t.Fatal("mismatched wire accepted")
			}
			r.Wire.Op, r.Host = d.Name, ""
			if ValidateRoute(r) == nil {
				t.Fatal("unscoped wire accepted")
			}
		})
	}
	for _, op := range []string{"unknown", "secret.set", "secret.use", "sync.pull", "fleet.plan"} {
		if ValidateRoute(Request{Operation: op}) == nil || ValidateRoute(Request{Operation: op, Host: "a", Wire: &proto.Request{Op: op}}) == nil {
			t.Fatalf("absent handler %s accepted", op)
		}
	}
	for _, op := range []string{"status", "pool.health", "audit.health", "audit_query"} {
		if ValidateRoute(Request{Operation: op, Host: "a"}) == nil {
			t.Fatalf("global query %s accepted a scope it does not filter", op)
		}
	}
	for _, target := range []string{"", "a", "b"} {
		for _, revoke := range []bool{false, true} {
			err := ValidateRoute(Request{Operation: "policy.grant", Host: "a", GrantHost: target, Revoke: revoke})
			if (err == nil) != (target == "a") {
				t.Fatalf("policy scope %q revoke=%v: %v", target, revoke, err)
			}
		}
		err := ValidateRoute(Request{Operation: "approval.create", Host: "a", ApprovalSpec: &ApprovalSpec{Host: target}})
		if (err == nil) != (target == "a") {
			t.Fatalf("approval scope %q: %v", target, err)
		}
	}
}
