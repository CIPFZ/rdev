package broker

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestAuditRequestDigestsProtectParametersAndDefineLifetime(t *testing.T) {
	a := NewAuditLog(16)
	req := Request{Owner: Owner{ClientID: "client", ProjectID: "project"}, Operation: "secret.set", Host: "private-host",
		Secret: &SecretParams{Name: "password", Value: "guessable-password"}, Approval: "private-token"}
	event := func(log *AuditLog, request Request) AuditEvent {
		t.Helper()
		e, err := log.RequestEvent(request, "", Decision{})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	first := event(a, req)
	req.ID, req.OperationID, req.Approval = "new-request", "new-operation", "another-private-token"
	second := event(a, req)
	if first.RequestDigest != second.RequestDigest || first.TargetDigest != second.TargetDigest {
		t.Fatal("transport identity or approval credential changed semantic audit digests")
	}
	req.Secret = &SecretParams{Name: "password", Value: "different-password"}
	if changed := event(a, req); changed.RequestDigest == first.RequestDigest || changed.TargetDigest != first.TargetDigest {
		t.Fatal("secret parameter substitution was not distinguished")
	}
	req.Host = "other-private-host"
	if event(a, req).TargetDigest == first.TargetDigest {
		t.Fatal("target substitution was not distinguished")
	}
	if event(NewAuditLog(16), req).RequestDigest == event(a, req).RequestDigest {
		t.Fatal("separate broker instances reused an audit digest key")
	}
	if first.DigestScope != "broker_instance" || first.TargetScope != "submitted" {
		t.Fatal("audit digest lifetime or target scope not declared")
	}
	data, _ := json.Marshal(first)
	for _, private := range []string{"guessable-password", "private-token", "private-host", "password"} {
		if strings.Contains(string(data), private) {
			t.Fatalf("audit exposed private parameter %q", private)
		}
	}
	a.digestKey = nil
	if _, err := a.RequestEvent(req, "", Decision{}); err == nil {
		t.Fatal("audit accepted a request without a private digest key")
	}
}

func TestAuditTargetDistinguishesSubmittedAndConfiguredSnapshot(t *testing.T) {
	s := NewService(nil)
	t.Cleanup(func() { _ = s.Close(t.Context()) })
	req := Request{Owner: Owner{ClientID: "client", ProjectID: "project"}, Operation: proto.OpReadFile, Host: "h"}
	e, err := s.Audit.RequestEvent(req, "", Decision{Allow: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuditTarget(&e, "unknown@host"); err != nil || e.TargetScope != "submitted" || len(s.Client().Hosts.Names()) != 0 {
		t.Fatal("unknown audit target was registered or represented as configured")
	}
	if err := s.Client().Hosts.Add(transport.Host{Name: "h", Addr: "h.invalid"}); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuditTarget(&e, "h"); err != nil || e.TargetScope != "configured" {
		t.Fatal("known target lacks configuration snapshot")
	}
	prior := e.TargetDigest
	s.Client().Hosts.Update("h", func(st *session.State) { st.Env = map[string]string{"PASSWORD": "low-entropy-value"} })
	if err := s.BindAuditTarget(&e, "h"); err != nil || e.TargetDigest == prior {
		t.Fatal("changed configuration kept the old target digest")
	}
}
