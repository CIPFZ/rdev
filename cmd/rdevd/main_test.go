package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestUnixBrokerMultipleClientsShareService(t *testing.T) {
	socket := filepath.Join("/tmp", "rdevd-it-"+fmt.Sprint(os.Getpid())+".sock")
	_ = os.Remove(socket)
	defer os.Remove(socket)
	listener, err := broker.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	service := broker.NewService(nil)
	ownerA := broker.Owner{ClientID: "a", ProjectID: "p"}
	ownerB := broker.Owner{ClientID: "b", ProjectID: "p"}
	if err := service.Grant(ownerA, "status"); err != nil {
		t.Fatal(err)
	}
	if err := service.Grant(ownerB, "status"); err != nil {
		t.Fatal(err)
	}
	go func() {
		for i := 0; i < 2; i++ {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go serveConn(conn, service)
		}
	}()
	a, err := broker.DialClient(context.Background(), socket, ownerA)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := broker.DialClient(context.Background(), socket, ownerB)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, c := range []*broker.Client{a, b} {
		resp, callErr := c.Do(broker.Request{Operation: "status"})
		if callErr != nil || !resp.OK {
			t.Fatalf("shared broker request failed: %v %s", callErr, resp.Error)
		}
	}
}

func TestServeConnCancelsInFlightDispatchOnDisconnect(t *testing.T) {
	a, b := net.Pipe()
	service := broker.NewService(nil)
	owner := broker.Owner{ClientID: "cancel", ProjectID: "p"}
	if err := service.Grant(owner, "ping"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	canceled := make(chan struct{})
	service.SetDispatcher(func(ctx context.Context, _ string, _ *proto.Request) (*proto.Response, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	})
	go serveConn(b, service)
	if err := json.NewEncoder(a).Encode(proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}); err != nil {
		t.Fatal(err)
	}
	var hello proto.BrokerHelloResponse
	if err := json.NewDecoder(a).Decode(&hello); err != nil || !hello.OK {
		t.Fatal(err)
	}
	if err := json.NewEncoder(a).Encode(broker.Request{ID: "cancel", Owner: owner, Operation: "ping", Host: "h", Wire: &proto.Request{Op: proto.OpPing}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dispatch did not start")
	}
	_ = a.Close()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("dispatch was not canceled")
	}
}

func TestServeConnNegotiates(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go serveConn(b, broker.NewService(nil))
	if err := json.NewEncoder(a).Encode(proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}); err != nil {
		t.Fatal(err)
	}
	var got proto.BrokerHelloResponse
	if err := json.NewDecoder(a).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if !got.OK {
		t.Fatalf("unexpected rejection: %s", got.Error)
	}
}

func TestServeConnConsumesRiskApprovalOnce(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	service := broker.NewService(nil)
	owner := broker.Owner{ClientID: "risk", ProjectID: "p"}
	if err := service.Grant(owner, "delete"); err != nil {
		t.Fatal(err)
	}
	approval, err := service.CreateApproval(owner, "delete", "target-1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	go serveConn(b, service)
	_ = json.NewEncoder(a).Encode(proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion})
	var hello proto.BrokerHelloResponse
	if err := json.NewDecoder(a).Decode(&hello); err != nil || !hello.OK {
		t.Fatal(err)
	}
	req := broker.Request{ID: "risk-1", Owner: owner, Operation: "delete", Target: "target-1", Approval: approval.Token, Risk: true}
	_ = json.NewEncoder(a).Encode(req)
	var first broker.Response
	if err := json.NewDecoder(a).Decode(&first); err != nil || !first.OK {
		t.Fatalf("first approval failed: %v %s", err, first.Error)
	}
	req.ID = "risk-2"
	_ = json.NewEncoder(a).Encode(req)
	var second broker.Response
	if err := json.NewDecoder(a).Decode(&second); err != nil {
		t.Fatal(err)
	}
	if second.OK || second.Error == "" {
		t.Fatal("replayed approval accepted")
	}
}

func TestServeConnRejectsIncompatible(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	go serveConn(b, broker.NewService(nil))
	_ = json.NewEncoder(a).Encode(proto.BrokerHello{Version: 99, MinVersion: 99})
	var got proto.BrokerHelloResponse
	if err := json.NewDecoder(a).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.OK || got.Error == "" {
		t.Fatal("incompatible hello accepted")
	}
	var ignored broker.Request
	if err := json.NewDecoder(a).Decode(&ignored); err == nil {
		t.Fatal("incompatible connection remained in request loop")
	}
}

func TestServeConnAppliesPolicy(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	service := broker.NewService(nil)
	go serveConn(b, service)
	_ = json.NewEncoder(a).Encode(proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion})
	var hello proto.BrokerHelloResponse
	_ = json.NewDecoder(a).Decode(&hello)
	owner := broker.Owner{ClientID: "c", ProjectID: "p"}
	_ = json.NewEncoder(a).Encode(broker.Request{ID: "1", Owner: owner, Operation: "exec"})
	var denied broker.Response
	_ = json.NewDecoder(a).Decode(&denied)
	if denied.OK {
		t.Fatal("default policy allowed request")
	}
	if err := service.Grant(owner, "exec"); err != nil {
		t.Fatal(err)
	}
	_ = json.NewEncoder(a).Encode(broker.Request{ID: "2", Owner: owner, Operation: "exec"})
	var allowed broker.Response
	_ = json.NewDecoder(a).Decode(&allowed)
	if !allowed.OK {
		t.Fatalf("granted request denied: %s", allowed.Error)
	}
}

func TestServeConnSharesServiceAcrossClients(t *testing.T) {
	service := broker.NewService(nil)
	owner := broker.Owner{ClientID: "shared", ProjectID: "project"}
	if err := service.Grant(owner, "status"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		a, b := net.Pipe()
		go serveConn(b, service)
		if err := json.NewEncoder(a).Encode(proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}); err != nil {
			t.Fatal(err)
		}
		var hello proto.BrokerHelloResponse
		if err := json.NewDecoder(a).Decode(&hello); err != nil || !hello.OK {
			t.Fatalf("hello %v", err)
		}
		if err := json.NewEncoder(a).Encode(broker.Request{ID: "status", Owner: owner, Operation: "status"}); err != nil {
			t.Fatal(err)
		}
		var resp broker.Response
		if err := json.NewDecoder(a).Decode(&resp); err != nil || !resp.OK {
			t.Fatalf("request %v", resp.Error)
		}
		a.Close()
	}
}
