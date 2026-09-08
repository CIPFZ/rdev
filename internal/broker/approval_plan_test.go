package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

func approvalTestService(t *testing.T) (*Service, Owner, Owner) {
	t.Helper()
	s := NewService(nil)
	t.Cleanup(func() { _ = s.Close(t.Context()) })
	a, b := Owner{ClientID: "a", ProjectID: "p"}, Owner{ClientID: "b", ProjectID: "p"}
	for _, owner := range []Owner{a, b} {
		for _, op := range []string{proto.OpExec, proto.OpWriteFile} {
			if err := s.Grant(owner, op); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, host := range []string{"h", "other"} {
		if err := s.Client().Hosts.Add(transport.Host{Name: host, Addr: host + ".invalid"}); err != nil {
			t.Fatal(err)
		}
	}
	return s, a, b
}

func TestApprovalRejectsRequestAndOwnerSubstitutionWithoutConsuming(t *testing.T) {
	s, a, b := approvalTestService(t)
	wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "printf reviewed"}, Cwd: "/reviewed", Env: map[string]string{"MODE": "reviewed"}, Stdin: "reviewed-input", TimeoutSec: 10}}
	approval, err := s.IssueApproval(ApprovalSpec{Owner: a, Operation: wire.Op, Host: "h", Wire: wire})
	if err != nil {
		t.Fatal(err)
	}
	original := Request{Owner: a, Operation: wire.Op, Host: "h", Wire: wire, Approval: approval.Token}
	for name, mutate := range map[string]func(*Request){
		"owner":    func(r *Request) { r.Owner = b },
		"project":  func(r *Request) { r.Owner.ProjectID = "other" },
		"host":     func(r *Request) { r.Host = "other" },
		"argv":     func(r *Request) { r.Wire.Exec.Argv[2] = "printf swapped" },
		"cwd":      func(r *Request) { r.Wire.Exec.Cwd = "/swapped" },
		"env":      func(r *Request) { r.Wire.Exec.Env["MODE"] = "swapped" },
		"stdin":    func(r *Request) { r.Wire.Exec.Stdin = "swapped" },
		"deadline": func(r *Request) { r.Wire.DeadlineUnixMilli = 1234 },
		"login":    func(r *Request) { r.Wire.Exec.LoginShell = true },
		"operation": func(r *Request) {
			r.Operation = proto.OpWriteFile
			r.Wire = &proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: "/tmp/other"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			data, _ := json.Marshal(original)
			var changed Request
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			decision := s.DecideRequest(changed.Owner, changed.Operation, changed.Host)
			if _, err := s.AuthorizeApproval(changed, decision); err == nil {
				t.Fatal("approval substitution accepted")
			}
		})
	}
	decision := s.DecideRequest(a, original.Operation, original.Host)
	if _, err := s.AuthorizeApproval(original, decision); err != nil {
		t.Fatalf("other request consumed owner's token: %v", err)
	}
	if _, err := s.AuthorizeApproval(original, decision); err == nil {
		t.Fatal("approval reused")
	}
}

func TestApprovalRejectsChangedPolicyTargetAndExpiry(t *testing.T) {
	for _, scenario := range []string{"policy", "target", "expiry"} {
		t.Run(scenario, func(t *testing.T) {
			s, a, _ := approvalTestService(t)
			wire := &proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: "/tmp/approved", Content: "reviewed"}}
			ttl := time.Minute
			if scenario == "expiry" {
				ttl = 10 * time.Millisecond
			}
			approval, err := s.IssueApproval(ApprovalSpec{Owner: a, Operation: wire.Op, Host: "h", Wire: wire, TTL: ttl})
			if err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "policy":
				if err := s.Grant(a, "status"); err != nil {
					t.Fatal(err)
				}
			case "target":
				s.Client().Hosts.Update("h", func(st *session.State) { st.Env = map[string]string{"TARGET": "changed"} })
			case "expiry":
				time.Sleep(20 * time.Millisecond)
			}
			req := Request{Owner: a, Operation: wire.Op, Host: "h", Wire: wire, Approval: approval.Token}
			if _, err := s.AuthorizeApproval(req, s.DecideRequest(a, wire.Op, "h")); err == nil {
				t.Fatal("changed or expired approval accepted")
			}
		})
	}
}

func TestApprovalDigestCannotConfuseFieldBoundaries(t *testing.T) {
	approval, err := NewApproval("a", "b\x00c", "d", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := approval.Validate(approval.Token, "a", "b", "c\x00d", time.Now()); err == nil {
		t.Fatal("delimiter collision changed operation and target")
	}
	for _, ttl := range []time.Duration{0, -time.Second, 11 * time.Minute} {
		if _, err := NewApproval("a", "b", "c", ttl); err == nil {
			t.Fatal("invalid lifetime accepted")
		}
	}
}
