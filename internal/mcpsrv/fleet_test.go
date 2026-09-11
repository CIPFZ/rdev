package mcpsrv

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestFleetSDKStandaloneRejectsAndSharedPreservesContracts(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	standalone := connect(t, c)
	if isErr, _ := callTool(t, standalone, "rdev_fleet", map[string]any{"action": "list", "request": map[string]any{}}, nil); !isErr {
		t.Fatal("standalone Fleet accepted")
	}
	// macOS TMPDIR plus the test name exceeds sockaddr_un.sun_path.
	dir, err := os.MkdirTemp("/tmp", "rdev-mcp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "broker.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	requests := make(chan broker.Request, 1)
	serverErr := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		dec, enc := json.NewDecoder(conn), json.NewEncoder(conn)
		var hello proto.BrokerHello
		if err := dec.Decode(&hello); err != nil {
			serverErr <- err
			return
		}
		if err := enc.Encode(proto.BrokerHelloResponse{OK: true, Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}); err != nil {
			serverErr <- err
			return
		}
		var q broker.Request
		if err := dec.Decode(&q); err != nil {
			serverErr <- err
			return
		}
		requests <- q
		serverErr <- enc.Encode(broker.Response{OK: true, ID: q.ID, Fleet: &broker.FleetPlan{PlanID: q.Fleet.PlanID, State: "paused", Total: 35, NextOffset: 32, Counts: map[string]int{"ambiguous": 1}, ExitStatus: 2}})
	}()
	owner := broker.Owner{ClientID: "sdk-fleet", ProjectID: "project"}
	srv, err := NewBroker(socket, owner)
	if err != nil {
		t.Fatal(err)
	}
	shared := connectServer(t, srv)
	var out fleetOut
	if isErr, text := callTool(t, shared, "rdev_fleet", map[string]any{"action": "results", "request": map[string]any{"plan_id": "plan", "offset": 0, "limit": 32}}, &out); isErr {
		t.Fatal(text)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	q := <-requests
	if q.Operation != "fleet.results" || q.Owner != owner || q.Fleet == nil || q.Fleet.Limit != 32 || q.Fleet.PlanID != "plan" || q.Wire != nil {
		t.Fatalf("shared request changed: %+v", q)
	}
	if out.Plan == nil || out.Plan.ExitStatus != 2 || out.Plan.Counts["ambiguous"] != 1 || out.Plan.NextOffset != 32 || out.Plan.Total != 35 {
		t.Fatalf("lost aggregate/page semantics: %+v", out)
	}
	if isErr, _ := callTool(t, shared, "rdev_fleet", map[string]any{"action": "invented", "request": map[string]any{}}, nil); !isErr {
		t.Fatal("unknown Fleet action accepted")
	}
}
