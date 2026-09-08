package client

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestBulkPoolReaperPreservesActiveAndReplacementTransport(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	var conns []*fakeRemoteConn
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		conn := &fakeRemoteConn{host: h}
		conns = append(conns, conn)
		return conn, nil
	}
	_, _, releaseA, err := c.leasedBulkConn(t.Context(), "u@h", "")
	if err != nil {
		t.Fatal(err)
	}
	_, _, releaseB, err := c.leasedBulkConn(t.Context(), "u@h", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(conns) != 2 {
		t.Fatalf("base+shared bulk dials=%d", len(conns))
	}
	releaseA()
	if n := c.ReapBulkIdle(time.Now().Add(time.Hour), time.Second); n != 0 {
		t.Fatal("active bulk reaped")
	}
	releaseB()
	if n := c.ReapBulkIdle(time.Now(), time.Second); n != 0 {
		t.Fatal("bulk reaped before TTL")
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	conns[1].closeFn = func() { close(entered); <-finish }
	done := make(chan int, 1)
	go func() { done <- c.ReapBulkIdle(time.Now().Add(time.Hour), time.Second) }()
	<-entered
	_, _, releaseC, err := c.leasedBulkConn(t.Context(), "u@h", "")
	if err != nil {
		t.Fatal(err)
	}
	close(finish)
	if <-done != 1 || len(conns) != 3 || conns[0].closed || conns[2].closed {
		t.Fatal("bulk cleanup affected base or replacement")
	}
	if c.ConnectionSecurity("u@h").State != observe.SecurityReady {
		t.Fatal("bulk eviction reset base security")
	}
	releaseC()
	if !c.Disconnect("u@h") || !conns[0].closed || !conns[2].closed {
		t.Fatal("host disconnect left bulk transport open")
	}
}

func TestBulkRetryCannotReplaceBaseOrReplayMutation(t *testing.T) {
	for _, op := range []string{proto.OpReadFile, proto.OpWriteFile} {
		t.Run(op, func(t *testing.T) {
			c := newTestClient()
			defer c.Close()
			var conns []*fakeRemoteConn
			c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
				conn := &fakeRemoteConn{host: h}
				if len(conns) == 1 {
					conn.handler = func(*proto.Request) (*proto.Response, error) { return nil, errors.New("bulk link lost") }
				}
				conns = append(conns, conn)
				return conn, nil
			}
			req := &proto.Request{Op: op, ClientID: "a", ProjectID: "p", Read: &proto.ReadParams{Path: "file"}}
			if op == proto.OpWriteFile {
				req.Read = nil
				req.Cat = &proto.WriteParams{Path: "file", Content: "mutation", Append: true}
			}
			_, err := c.DoProtocolBulk(t.Context(), "u@h", req, "")
			if op == proto.OpReadFile {
				if err != nil || len(conns) != 3 {
					t.Fatalf("safe bulk retry: %v dials=%d", err, len(conns))
				}
			} else {
				if err == nil || len(conns) != 2 {
					t.Fatalf("ambiguous bulk mutation replayed: %v dials=%d", err, len(conns))
				}
			}
			if conns[0].closed || !conns[1].closed {
				t.Fatal("bulk failure closed base or retained broken bulk")
			}
		})
	}
}

func TestBulkUsesInitializedSecretsAndExactApprovedTarget(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "h", Addr: "original.invalid"}); err != nil {
		t.Fatal(err)
	}
	c.Hosts.Update("h", func(st *session.State) { st.Secrets = map[string]string{"token": "secret-path"} })
	var secretReads atomic.Int32
	var dials atomic.Int32
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		dials.Add(1)
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Read != nil && req.Read.Path == "secret-path" {
				secretReads.Add(1)
			}
			return &proto.Response{OK: true, Read: &proto.ReadResult{Content: "bulk-secret-canary", EOF: true}}, nil
		}}, nil
	}
	target, err := c.ProtocolTargetIdentity("h")
	if err != nil {
		t.Fatal(err)
	}
	req := &proto.Request{Op: proto.OpReadFile, ClientID: "a", ProjectID: "p", Read: &proto.ReadParams{Path: "file"}}
	response, err := c.DoProtocolBulk(t.Context(), "h", req, target)
	if err != nil || response.Read.Content == "bulk-secret-canary" {
		t.Fatal("bulk response escaped shared redaction")
	}
	req.ClientID = "other-owner"
	if _, err := c.DoProtocolBulk(t.Context(), "h", req, target); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 2 || secretReads.Load() != 1 {
		t.Fatal("bulk transport independently reloaded secret state")
	}
	c.Hosts.Update("h", func(st *session.State) { st.Cwd = "changed" })
	if _, err := c.DoProtocolBulk(t.Context(), "h", req, target); err == nil {
		t.Fatal("changed approved bulk target accepted")
	}
	if dials.Load() != 2 {
		t.Fatal("changed approved target dialed before rejection")
	}
}
