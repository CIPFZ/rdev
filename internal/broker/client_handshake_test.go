package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func handshakeServer(t *testing.T, handler func(net.Conn)) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rdev-hello-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "broker.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}()
	return path
}

func TestClientHandshakeValidatesPeerRange(t *testing.T) {
	for _, test := range []struct {
		version, minimum int
		valid            bool
	}{{1, 1, true}, {2, 1, true}, {0, 0, false}, {2, 2, false}, {1, 2, false}} {
		t.Run(fmt.Sprintf("%d_%d", test.minimum, test.version), func(t *testing.T) {
			path := handshakeServer(t, func(conn net.Conn) {
				var hello proto.BrokerHello
				if json.NewDecoder(conn).Decode(&hello) == nil {
					_ = json.NewEncoder(conn).Encode(proto.BrokerHelloResponse{OK: true, Version: test.version, MinVersion: test.minimum})
				}
			})
			c, err := DialClient(t.Context(), path, Owner{ClientID: "client", ProjectID: "project"})
			if c != nil {
				_ = c.Close()
			}
			if (err == nil) != test.valid {
				t.Fatalf("peer range validity=%v, error=%v", test.valid, err)
			}
		})
	}
}

func TestClientHandshakeCancellationAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			helloRead := make(chan struct{})
			peerClosed := make(chan struct{})
			path := handshakeServer(t, func(conn net.Conn) {
				defer close(peerClosed)
				var hello proto.BrokerHello
				if json.NewDecoder(conn).Decode(&hello) != nil {
					return
				}
				close(helloRead)
				var b [1]byte
				_, _ = conn.Read(b[:])
			})
			ctx, cancel := context.WithCancel(t.Context())
			want := context.Canceled
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
				want = context.DeadlineExceeded
			}
			defer cancel()
			done := make(chan error, 1)
			go func() {
				c, err := DialClient(ctx, path, Owner{ClientID: "client", ProjectID: "project"})
				if c != nil {
					_ = c.Close()
				}
				done <- err
			}()
			select {
			case <-helloRead:
			case <-time.After(time.Second):
				t.Fatal("hello not delivered")
			}
			if !deadline {
				cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("handshake cancellation error=%v want=%v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("handshake ignored cancellation")
			}
			select {
			case <-peerClosed:
			case <-time.After(time.Second):
				t.Fatal("canceled handshake retained its connection")
			}
		})
	}
}

func TestClientHandshakeTransfersConnectionOwnership(t *testing.T) {
	path := handshakeServer(t, func(conn net.Conn) {
		dec, enc := json.NewDecoder(conn), json.NewEncoder(conn)
		var hello proto.BrokerHello
		if dec.Decode(&hello) != nil {
			return
		}
		_ = enc.Encode(proto.BrokerHelloResponse{OK: true, Version: 1, MinVersion: 1})
		var req Request
		if dec.Decode(&req) == nil {
			_ = enc.Encode(Response{ID: req.ID, OK: true})
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	c, err := DialClient(ctx, path, Owner{ClientID: "client", ProjectID: "project"})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	response, err := c.DoContext(t.Context(), Request{Operation: "status"})
	if err != nil || !response.OK {
		t.Fatalf("completed handshake retained cancellation ownership: %v", err)
	}
}
