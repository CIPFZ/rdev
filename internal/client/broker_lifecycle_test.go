package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestConnectionWaiterCancelsBeforeSharedDialCompletes(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	started, release := make(chan struct{}), make(chan struct{})
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		close(started)
		<-release
		return &fakeRemoteConn{host: h}, nil
	}
	done := make(chan error, 1)
	go func() { _, err := c.conn(t.Context(), "u@h"); done <- err }()
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	waiter := make(chan error, 1)
	go func() { _, err := c.conn(ctx, "u@h"); waiter <- err }()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("canceled waiter is still blocked on another caller's dial")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !c.IsConnected("u@h") {
		t.Fatal("waiter canceled shared initialization")
	}
}

func TestCanceledInitializationDoesNotPoisonOtherOwners(t *testing.T) {
	for _, phase := range []string{"dial", "secret_read"} {
		t.Run(phase, func(t *testing.T) {
			c := newTestClient()
			defer c.Close()
			if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
				t.Fatal(err)
			}
			c.Hosts.Update("dev", func(st *session.State) { st.Secrets = map[string]string{"tok": "~/token"} })
			ctx, cancel := context.WithCancel(t.Context())
			c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
				cancel()
				if phase == "dial" {
					return nil, context.Canceled
				}
				return &fakeRemoteConn{host: h}, nil
			}
			if _, err := c.conn(ctx, "dev"); err == nil {
				t.Fatal("canceled initialization succeeded")
			}
			if state := c.ConnectionSecurity("dev").State; state != observe.SecurityCold {
				t.Fatalf("cancellation poisoned shared host: %s", state)
			}
			// A second owner is allowed to try secure initialization. A genuine secret
			// validation failure remains fail-closed, independently of the first caller.
			called := false
			c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
				called = true
				return nil, errors.New("second caller reached initialization")
			}
			_, _ = c.conn(t.Context(), "dev")
			if !called {
				t.Fatal("other owner denied without an initialization attempt")
			}
		})
	}
}

func TestDetachedPoolCleanupCannotCloseNewConnection(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	var connections []*fakeRemoteConn
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		conn := &fakeRemoteConn{host: h}
		connections = append(connections, conn)
		return conn, nil
	}
	if _, err := c.conn(t.Context(), "u@h"); err != nil {
		t.Fatal(err)
	}
	cleanup := c.DetachConnections()
	if c.IsConnected("u@h") {
		t.Fatal("old pool still published after detachment")
	}
	if _, err := c.conn(t.Context(), "u@h"); err != nil {
		t.Fatal(err)
	}
	cleanup()
	if len(connections) != 2 || !connections[0].closed || connections[1].closed || !c.IsConnected("u@h") {
		t.Fatal("cleanup touched the replacement pool")
	}
	if state := c.ConnectionSecurity("u@h").State; state != observe.SecurityReady {
		t.Fatalf("stale cleanup published %s", state)
	}
}
