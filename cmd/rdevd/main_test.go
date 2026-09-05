package main

import (
	"encoding/json"
	"net"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

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
