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
