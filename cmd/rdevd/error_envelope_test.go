package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestDaemonProjectsTypedSetupErrorsToBrokerClient(t *testing.T) {
	socket := privateTestSocket(t)
	listener, err := broker.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	s := broker.NewService(nil)
	defer s.Close(context.Background())
	owner := broker.Owner{ClientID: "typed-client", ProjectID: "typed-project"}
	if err := s.Client().Hosts.Add(transport.Host{Name: "h", Addr: "u@h"}); err != nil {
		t.Fatal(err)
	}
	if err := s.GrantHost(owner, "h", broker.CapabilityForOperation(proto.OpPing), proto.OpPing, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Client().Secrets.Set(secrets.OutputKey("registry-word"), "release"); err != nil {
		t.Fatal(err)
	}
	s.SetDispatcher(func(context.Context, string, *proto.Request) (*proto.Response, error) {
		return nil, fmt.Errorf("unregistered-private-path: %w", proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent))
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err == nil {
			serveConn(conn, s)
		}
	}()
	c, err := broker.DialClient(t.Context(), socket, owner)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	const id = "op_daemon_setup_error"
	r, err := c.DoContext(t.Context(), broker.Request{Operation: proto.OpPing, Host: "h", Wire: &proto.Request{Op: proto.OpPing, OperationID: id}})
	var envelope *proto.ErrorEnvelope
	if err != nil || !errors.As(r.Failure(), &envelope) || envelope.Code != proto.CodeReleaseUntrusted || envelope.Validate() != nil || envelope.OperationID != id || envelope.ExecutionState != proto.StateNotSent || strings.Contains(r.Error, "private-path") || r.RequestRef == "" {
		t.Fatalf("daemon lost error identity/redaction: %+v %v", r, err)
	}
	c.Close()
	<-done
}
