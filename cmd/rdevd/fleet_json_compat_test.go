package main

import (
	"encoding/json"
	"github.com/CIPFZ/rdev/internal/broker"
	"net"
	"reflect"
	"testing"
)

func TestIndependentFleetStrictDeltaAndLegacyPipeline(t *testing.T) {
	for _, raw := range []string{
		`{"operation":"fleet.plan","fleet":{"spec":{"selector":"all","operation":"job_start","job":{"spec":{"argv":["first"],"argv":["second"]}}}}}`,
		`{"operation":"fleet.list","unknown":true,"fleet":{}}`,
		`{"operation":"status","operation":"fleet.list","fleet":{}}`,
		`{"operation":"fleet.list","fleet":null}`,
	} {
		var got broker.Request
		if json.Unmarshal([]byte(raw), &got) == nil {
			t.Fatalf("ambiguous Fleet accepted: %s", raw)
		}
	}
	for _, raw := range []string{`{"operation":"status","unknown":true}`, `{"operation":"status","wire":null}`, `{"operation":"unknown","operation":"status"}`} {
		type legacyRequest broker.Request
		var old legacyRequest
		var got broker.Request
		a := json.Unmarshal([]byte(raw), &old)
		b := json.Unmarshal([]byte(raw), &got)
		if (a == nil) != (b == nil) || !reflect.DeepEqual(broker.Request(old), got) {
			t.Fatalf("nonFleet compatibility changed: %s", raw)
		}
	}
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	budget := broker.NewIngress()
	lease, err := budget.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	owner := broker.Owner{ClientID: "review", ProjectID: "delta"}
	if err = lease.Bind(owner); err != nil {
		t.Fatal(err)
	}
	d := &boundedBrokerDecoder{conn: server, lease: lease}
	raw := `{"operation":"fleet.list","fleet":{}} {"operation":"status","ignored":true}`
	go func() { _, _ = client.Write([]byte(raw)) }()
	for _, op := range []string{"fleet.list", "status"} {
		var req broker.Request
		release, err := d.Decode(&req, 4096)
		if err != nil {
			t.Fatal(err)
		}
		release()
		if req.Operation != op {
			t.Fatal("pipeline changed")
		}
	}
	if budget.Snapshot(owner.Key()).Bytes != 0 {
		t.Fatal("ingress request reservation leaked")
	}
}
