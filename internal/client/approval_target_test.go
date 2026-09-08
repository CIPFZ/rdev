package client

import (
	"context"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestApprovedTargetCannotDialChangedIdentity(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "h", Addr: "original.invalid"}); err != nil {
		t.Fatal(err)
	}
	target, err := c.ProtocolTargetIdentity("h")
	if err != nil {
		t.Fatal(err)
	}
	c.Hosts.Update("h", func(st *session.State) { st.Cwd = "/changed-target" })
	dials := 0
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		dials++
		t.Error("changed target dialed before approval check")
		return nil, context.Canceled
	}
	req := &proto.Request{Op: proto.OpExec, ClientID: "a", ProjectID: "p", Exec: &proto.ExecParams{Argv: []string{"true"}}}
	if _, err := c.DoProtocolApproved(t.Context(), "h", req, target); err == nil {
		t.Fatal("changed approval target accepted")
	}
	if dials != 0 {
		t.Fatal("unapproved target touched before dispatch")
	}
	if _, err := c.ProtocolTargetIdentity("unregistered@host"); err == nil {
		t.Fatal("approval inspection registered an arbitrary SSH destination")
	}
}

func TestApprovedDeadlineIsNotExtendedByContext(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "h", Addr: "h.invalid"}); err != nil {
		t.Fatal(err)
	}
	target, err := c.ProtocolTargetIdentity("h")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second).UnixMilli()
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.DeadlineUnixMilli != deadline {
				t.Error("context extended approved semantic deadline")
			}
			return &proto.Response{OK: true}, nil
		}}, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, err := c.DoProtocolApproved(ctx, "h", &proto.Request{Op: proto.OpExec, ClientID: "a", ProjectID: "p", DeadlineUnixMilli: deadline, Exec: &proto.ExecParams{Argv: []string{"true"}}}, target); err != nil {
		t.Fatal(err)
	}
}
