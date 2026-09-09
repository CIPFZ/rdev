package mcpsrv

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSharedMCPOldBrokerPoolPreservesAbsentTelemetry(t *testing.T) {
	dir, err := os.MkdirTemp("", "rdev-old-pool-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		d, e := json.NewDecoder(conn), json.NewEncoder(conn)
		var hello proto.BrokerHello
		if err := d.Decode(&hello); err != nil {
			done <- err
			return
		}
		if err := e.Encode(proto.BrokerHelloResponse{OK: true, Version: 1, MinVersion: 1}); err != nil {
			done <- err
			return
		}
		var req broker.Request
		if err := d.Decode(&req); err != nil {
			done <- err
			return
		}
		// Actual Phase7 pool shape: no dial/goroutine/observer measurements.
		// Keep this literal independent of the current Go PoolHealth type.
		var pool any
		err = json.Unmarshal([]byte(`{"closing_bulk":0,"limit":16,"reserved_hosts":1,"active_hosts":1,"active_leases":0,"closing_hosts":0,"queued":0,"base_transports":1,"bulk_transports":0,"evictions":{}}`), &pool)
		if err == nil {
			err = e.Encode(map[string]any{"id": req.ID, "ok": true, "pool": pool})
		}
		done <- err
	}()
	srv, err := NewBroker(socket, broker.Owner{ClientID: "mcp", ProjectID: "old-pool"})
	if err != nil {
		t.Fatal(err)
	}
	cs := connectServer(t, srv)
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_broker_pool", Arguments: struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("old broker pool rejected by SDK: %+v", result.Content)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{"dial_admission", "goroutines", "observer_metric_series", "observer_metric_series_bound"} {
		if _, ok := fields[absent]; ok {
			t.Fatalf("fabricated old broker telemetry %s", absent)
		}
	}
	var health broker.PoolHealth
	if json.Unmarshal(raw, &health) != nil || health.Limit != 16 || health.BaseTransports != 1 {
		t.Fatal("lost authorized predecessor pool measurements")
	}
}
