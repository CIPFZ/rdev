package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestBrokerAuditBindsReadDeniedAndPolicyRequests(t *testing.T) {
	s := broker.NewService(nil)
	t.Cleanup(func() { _ = s.Close(t.Context()) })
	owner := broker.Owner{ClientID: "audit-client", ProjectID: "audit-project"}
	const signing = "01234567890123456789012345678901"
	if err := s.Principals.Rotate(signing); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{proto.OpReadFile, proto.OpWriteFile, "policy.grant"} {
		if err := s.Grant(owner, op); err != nil {
			t.Fatal(err)
		}
	}
	for _, host := range []string{"host-a", "host-b"} {
		if err := s.Client().Hosts.Add(transport.Host{Name: host, Addr: host + ".invalid"}); err != nil {
			t.Fatal(err)
		}
	}
	auditPath := filepath.Join(t.TempDir(), "audit")
	if err := s.Audit.ConfigureFile(auditPath, 32<<10); err != nil {
		t.Fatal(err)
	}
	defer s.Audit.Close()
	s.SetDispatcher(func(_ context.Context, _ string, req *proto.Request) (*proto.Response, error) {
		if req.Op == proto.OpReadFile {
			return &proto.Response{OK: true, Read: &proto.ReadResult{Content: "private-output"}}, nil
		}
		return &proto.Response{OK: true}, nil
	})
	socket := privateTestSocket(t)
	listener, err := broker.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			serveConn(conn, s)
		}
	}()
	t.Setenv("RDEV_PRINCIPAL_TOKEN", broker.PrincipalToken(signing, owner))
	t.Setenv("RDEV_APPROVAL_TOKEN", "")
	t.Setenv("RDEV_OPERATION_ID", "")
	c, err := broker.DialClient(t.Context(), socket, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	call := func(req broker.Request, allowed bool) broker.Response {
		t.Helper()
		response, err := c.DoContext(t.Context(), req)
		if err != nil || response.OK != allowed {
			t.Fatalf("request failed: %v %+v", err, response)
		}
		return response
	}
	var reads []broker.Response
	for _, target := range []struct{ host, path string }{{"host-a", "/private-path-a"}, {"host-a", "/private-path-b"}, {"host-b", "/private-path-a"}} {
		reads = append(reads, call(broker.Request{Operation: proto.OpReadFile, Host: target.host, Wire: &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: target.path}}}, true))
	}
	grant := broker.Request{Operation: "policy.grant", GrantOwner: broker.Owner{ClientID: "other-client", ProjectID: "other-project"}, GrantHost: "host-c", GrantOperation: proto.OpExec}
	granted := call(grant, true)
	grant.Revoke = true
	revoked := call(grant, true)
	denied := call(broker.Request{Operation: "secret.set", Host: "unregistered@denied.invalid", Secret: &broker.SecretParams{Name: "password", Value: "private-low-entropy"}}, false)
	if len(s.Client().Hosts.Names()) != 2 {
		t.Fatal("denied request registered its target")
	}
	wire := &proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: "/private-write-path", Content: "private-write-content"}}
	approval, err := s.IssueApproval(broker.ApprovalSpec{Owner: owner, Operation: wire.Op, Host: "host-a", Wire: wire})
	if err != nil {
		t.Fatal(err)
	}
	written := call(broker.Request{Operation: wire.Op, Host: "host-a", Wire: wire, Approval: approval.Token}, true)
	if err := s.Audit.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	byRef := make(map[string][]broker.AuditEvent)
	for _, event := range s.Audit.QueryOwner(time.Time{}, owner.Key()) {
		if len(event.RequestDigest) != 64 || len(event.TargetDigest) != 64 {
			t.Fatalf("%s/%s lacks target/request digest", event.Operation, event.Result)
		}
		byRef[event.RequestRef] = append(byRef[event.RequestRef], event)
	}
	readEvents := make([]broker.AuditEvent, len(reads))
	for i, response := range reads {
		events := byRef[response.RequestRef]
		if len(events) != 2 || events[0].Result != "admitted" || events[1].Result != "completed" {
			t.Fatalf("read trace incomplete: %+v", events)
		}
		if events[0].RequestDigest != events[1].RequestDigest || events[0].TargetDigest != events[1].TargetDigest || events[1].DigestScope != "broker_instance" || events[1].TargetScope != "configured" {
			t.Fatal("read trace changed digest or target scope")
		}
		readEvents[i] = events[1]
	}
	if readEvents[0].RequestDigest == readEvents[1].RequestDigest || readEvents[0].TargetDigest != readEvents[1].TargetDigest || readEvents[0].TargetDigest == readEvents[2].TargetDigest {
		t.Fatal("audit cannot distinguish changed path and changed host")
	}
	grantEvents, revokeEvents, deniedEvents := byRef[granted.RequestRef], byRef[revoked.RequestRef], byRef[denied.RequestRef]
	if len(grantEvents) != 1 || len(revokeEvents) != 1 || grantEvents[0].RequestDigest == revokeEvents[0].RequestDigest {
		t.Fatal("policy grant/revoke action was lost in audit")
	}
	if len(deniedEvents) != 1 || deniedEvents[0].TargetScope != "submitted" || deniedEvents[0].Result != "denied" {
		t.Fatal("denied target is missing or claims a configuration snapshot")
	}
	if events := byRef[written.RequestRef]; len(events) != 3 {
		t.Fatalf("approval trace incomplete: %+v", events)
	} else {
		for _, event := range events {
			if event.RequestDigest != approval.Plan.RequestDigest || event.TargetDigest != approval.Plan.TargetDigest || event.ApprovalID != broker.ApprovalReference(approval.Token) || event.DigestScope != "approval" || event.TargetScope != "approval" {
				t.Fatal("exact approval digest correlation changed")
			}
		}
	}
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"private-output", "private-path", "private-low-entropy", "private-write", approval.Token} {
		if strings.Contains(string(data), private) {
			t.Fatalf("audit persisted private request/output data %q", private)
		}
	}
}
