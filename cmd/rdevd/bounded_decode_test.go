package main

import (
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
)

func TestBoundedBrokerDecoderPreservesPipelinedPrettyJSON(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	budget := broker.NewIngress()
	lease, err := budget.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	owner := broker.Owner{ClientID: "decode", ProjectID: "p"}
	if err := lease.Bind(owner); err != nil {
		t.Fatal(err)
	}
	d := &boundedBrokerDecoder{conn: server, lease: lease}
	data := "{\n\"value\":\"first\"\n}" + strings.Repeat(" ", 25) + `{"value":"` + strings.Repeat("x", 1800) + `"}{"value":"last"}`
	written := make(chan error, 1)
	go func() { _, err := client.Write([]byte(data)); written <- err }()
	for _, want := range []string{"first", strings.Repeat("x", 1800), "last"} {
		var got struct {
			Value string `json:"value"`
		}
		release, err := d.Decode(&got, 4096)
		if err != nil || got.Value != want {
			t.Fatalf("lost pipelined document: %v", err)
		}
		release()
		release()
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	lease.Close()
	if budget.Snapshot(owner.Key()) != (broker.IngressSnapshot{}) {
		t.Fatal("decode budget was not released")
	}
}

func TestBoundedBrokerDecoderRejectsOversizeBeforeReadingWholeStream(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	budget := broker.NewIngress()
	lease, _ := budget.Open()
	defer lease.Close()
	if err := lease.Bind(broker.Owner{ClientID: "decode", ProjectID: "p"}); err != nil {
		t.Fatal(err)
	}
	d := &boundedBrokerDecoder{conn: server, lease: lease}
	written := make(chan error, 1)
	go func() { _, err := client.Write([]byte(`{"value":"` + strings.Repeat("x", 8192))); written <- err }()
	var value any
	if _, err := d.Decode(&value, 1024); err == nil {
		t.Fatal("oversize unclosed JSON accepted")
	}
	select {
	case err := <-written:
		t.Fatalf("read whole oversize stream: %v", err)
	default:
	}
	server.Close()
	<-written
}

func TestBoundedBrokerDecoderTrailingNewlineDoesNotExpireIdleSession(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	budget := broker.NewIngress()
	lease, _ := budget.Open()
	defer lease.Close()
	if err := lease.Bind(broker.Owner{ClientID: "decode", ProjectID: "p"}); err != nil {
		t.Fatal(err)
	}
	d := &boundedBrokerDecoder{conn: server, lease: lease}
	// Write the delimiter separately so it cannot rely on decoder prefetch.
	go func() { _, _ = client.Write([]byte(`{"first":true}`)) }()
	var value any
	release, err := d.Decode(&value, 1024)
	if err != nil {
		t.Fatal(err)
	}
	release()
	done := make(chan error, 1)
	go func() { _, _ = client.Write([]byte("\n")) }()
	go func() {
		release, err := d.Decode(&value, 1024)
		if err == nil {
			release()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("idle delimiter triggered read timeout: %v", err)
	case <-time.After(brokerFrameTimeout + 100*time.Millisecond):
	}
	go func() { _ = json.NewEncoder(client).Encode(map[string]bool{"second": true}) }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
