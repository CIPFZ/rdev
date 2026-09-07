package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestClientDoContextCancellationClosesOnlyFrontendConnection(t *testing.T) {
	path := fmt.Sprintf("%s/rdev-broker-client-%d.sock", os.TempDir(), os.Getpid())
	_ = os.Remove(path)
	t.Cleanup(func() { _ = os.Remove(path) })
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var hello proto.BrokerHello
		if json.NewDecoder(conn).Decode(&hello) != nil {
			return
		}
		_ = json.NewEncoder(conn).Encode(proto.BrokerHelloResponse{OK: true, Version: 1, MinVersion: 1})
		var req Request
		_ = json.NewDecoder(conn).Decode(&req)
		// Hold the response until the frontend cancellation closes this socket.
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
	}()
	owner := Owner{ClientID: "client", ProjectID: "project"}
	c, err := DialClient(context.Background(), path, owner)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, callErr := c.DoContext(ctx, Request{Owner: owner, Operation: "status"})
		done <- callErr
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case callErr := <-done:
		if callErr != context.Canceled {
			t.Fatalf("DoContext error=%v, want context canceled", callErr)
		}
	case <-time.After(time.Second):
		t.Fatal("DoContext did not return after cancellation")
	}
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("server connection remained open after cancellation")
	}
}
