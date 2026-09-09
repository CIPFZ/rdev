package main

import (
	"github.com/CIPFZ/rdev/internal/broker"
	"net"
	"testing"
)

func TestIndependentFleetRawJSONRejectsAmbiguousInventory(t *testing.T) {
	for name, raw := range map[string]string{
		"duplicate_records": `{"operation":"fleet.inventory.update","fleet":{"revision":1,"inventory":{"schema":1,"revision":1,"records":[{"host_id":"unreviewed"}],"records":[],"retired_ids":[]}}}`,
		"unknown_fields":    `{"operation":"fleet.inventory.update","fleet":{"revision":1,"inventory":{"schema":1,"revision":1,"records":[],"retired_ids":[],"unreviewed":true}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			server, client := net.Pipe()
			defer server.Close()
			defer client.Close()
			budget := broker.NewIngress()
			lease, err := budget.Open()
			if err != nil {
				t.Fatal(err)
			}
			defer lease.Close()
			d := &boundedBrokerDecoder{conn: server, lease: lease}
			go func() { _, _ = client.Write([]byte(raw)) }()
			var req broker.Request
			release, err := d.Decode(&req, 4096)
			if err != nil {
				return
			}
			release()
			if err = broker.ValidateRoute(req); err != nil {
				return
			}
			t.Fatalf("raw malformed Fleet document accepted and routed; records=%d", len(req.Fleet.Inventory.Records))
		})
	}
}
