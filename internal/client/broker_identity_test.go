package client

import (
	"context"
	"errors"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestBrokerPrincipalIdentitySurvivesRetryAndClientRestart(t *testing.T) {
	var observed []string
	for restart := 0; restart < 2; restart++ {
		c := newTestClient()
		if err := c.Hosts.Add(transport.Host{Name: "host", Addr: "test.invalid"}); err != nil {
			t.Fatal(err)
		}
		dials := 0
		c.dial = func(_ context.Context, host transport.Host, _ AgentLookup) (remoteConnection, error) {
			dials++
			attempt := dials
			return &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
				observed = append(observed, req.ClientID)
				if proto.ValidateOperationID(req.ClientID) != nil {
					t.Fatal("broker owner did not become a protocol-valid identity")
				}
				if restart == 0 && attempt == 1 {
					return nil, errors.New("closed transport")
				}
				return &proto.Response{OK: true, Ping: &proto.PingResult{}}, nil
			}}, nil
		}
		request := &proto.Request{Op: proto.OpPing, ClientID: "owner with spaces", ProjectID: "project/a"}
		if _, err := c.DoProtocol(t.Context(), "host", request); err != nil {
			t.Fatal(err)
		}
		if request.ClientID != "owner with spaces" || request.ProjectID != "project/a" || request.OperationID != "" {
			t.Fatal("broker request was mutated by transport dispatch")
		}
		c.Close()
	}
	if len(observed) != 3 || observed[0] != observed[1] || observed[1] != observed[2] {
		t.Fatalf("principal changed across retry/restart: %v", observed)
	}
}

func TestBrokerPrincipalSeparatesClientAndProject(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "host", Addr: "test.invalid"}); err != nil {
		t.Fatal(err)
	}
	identities := make(map[string]bool)
	c.dial = func(_ context.Context, host transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
			if identities[req.ClientID] {
				t.Fatal("distinct principal tuples shared a wire identity")
			}
			identities[req.ClientID] = true
			return &proto.Response{OK: true, Ping: &proto.PingResult{}}, nil
		}}, nil
	}
	for _, pair := range [][2]string{{"a b", "c"}, {"a", "b c"}, {"a", "c"}, {"b", "c"}} {
		if _, err := c.DoProtocol(t.Context(), "host", &proto.Request{Op: proto.OpPing, ClientID: pair[0], ProjectID: pair[1]}); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range []*proto.Request{nil, {Op: proto.OpPing}, {Op: proto.OpPing, ClientID: "a"}} {
		if _, err := c.DoProtocol(t.Context(), "host", request); err == nil {
			t.Fatal("unscoped broker request accepted")
		}
	}
}

type legacyBrokerConn struct{ *fakeRemoteConn }

func (*legacyBrokerConn) NegotiatedVersion() int { return 2 }

func TestBrokerDoesNotDropPrincipalForLegacyPeer(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "host", Addr: "test.invalid"}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	c.dial = func(_ context.Context, host transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &legacyBrokerConn{&fakeRemoteConn{host: host, handler: func(*proto.Request) (*proto.Response, error) { calls++; return &proto.Response{OK: true}, nil }}}, nil
	}
	_, err := c.DoProtocol(t.Context(), "host", &proto.Request{Op: proto.OpExec, ClientID: "a", ProjectID: "p", Exec: &proto.ExecParams{Argv: []string{"true"}}})
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeUnsupportedFeature || calls != 0 {
		t.Fatalf("broker downgraded owner identity: err=%v remote_calls=%d", err, calls)
	}
}
