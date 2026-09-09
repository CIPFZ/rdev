package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestSupportAuthorizesOnlyOwnReadOnlyDiscovery(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	owner := Owner{ClientID: "a", ProjectID: "p"}
	other := Owner{ClientID: "b", ProjectID: "q"}
	if err := s.policy.GrantHost(owner.Key(), "target", "exec", "exec"); err != nil {
		t.Fatal(err)
	}
	if err := s.policy.GrantHost(other.Key(), "other-owner-private-host", "job", "job_list"); err != nil {
		t.Fatal(err)
	}
	if err := s.policy.GrantHost(owner.Key(), "target", "status", "status"); err != nil {
		t.Fatal(err)
	}
	if err := s.policy.GrantHost(owner.Key(), "target", "mutation.read", "mutation.status"); err != nil {
		t.Fatal(err)
	}
	if err := s.policy.GrantHost(owner.Key(), "target", "secret", "secret.use"); err != nil {
		t.Fatal(err)
	}
	if !s.DecideBrokerRequest(Request{Owner: owner, Operation: "support", Host: "unknown"}).Allow {
		t.Fatal("own discovery denied")
	}
	for _, host := range []string{"target", "unknown"} {
		out := s.Support(owner, host)
		if out.RuntimeStatus != "permission_denied" || out.Runtime != nil {
			t.Fatal("unauthorized discovery probed target")
		}
		for _, permission := range out.Permissions {
			requestHost := host
			if permission.Scope == "broker" {
				requestHost = ""
			}
			decision := s.DecideBrokerRequest(Request{Owner: owner, Operation: permission.Operation, Host: requestHost})
			if permission.Allowed != decision.Allow {
				t.Fatalf("%s discovery=%v enforcement=%v", permission.Operation, permission.Allowed, decision.Allow)
			}
			if permission.Operation == "exec" && !permission.ApprovalRequired {
				t.Fatal("exec grant advertised without approval")
			}
			if permission.Operation == "status" {
				if permission.Allowed || permission.Scope != "broker" || ValidateRoute(Request{Operation: "status", Host: requestHost}) != nil {
					t.Fatal("host-scoped status grant advertised as an executable route")
				}
			}
			if (permission.Operation == "secret.use" || permission.Operation == "sync.delete") && permission.Callable {
				t.Fatal("reference-only permission advertised as a callable route")
			}
		}
		data, _ := json.Marshal(out)
		if strings.Contains(string(data), "other-owner-private-host") || strings.Contains(string(data), other.Key()) {
			t.Fatal("other-owner discovery leakage")
		}
	}
	if s.DecideBrokerRequest(Request{Operation: "support"}).Allow {
		t.Fatal("invalid owner accepted")
	}
	for _, bad := range []Request{
		{Operation: "support", Wire: &proto.Request{Op: proto.OpPing}},
		{Operation: "support", Risk: true}, {Operation: "support", ApprovalSpec: &ApprovalSpec{}},
		{Operation: "support", Secret: &SecretParams{Name: "secret"}},
	} {
		if err := ValidateRoute(bad); err == nil {
			t.Fatalf("malformed discovery accepted: %+v", bad)
		}
	}
}
