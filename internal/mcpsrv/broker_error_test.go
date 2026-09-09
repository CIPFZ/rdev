package mcpsrv

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestSharedMCPReleaseErrorUsesStructuredEnvelope(t *testing.T) {
	dir, err := os.MkdirTemp("", "rdev-mcp-error-")
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
	ids := make(chan string, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
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
		ids <- req.Wire.OperationID
		err = e.Encode((broker.Response{ID: req.ID}).WithError(proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent), req.Wire.OperationID))
		done <- err
	}()
	srv, err := NewBroker(socket, broker.Owner{ClientID: "mcp", ProjectID: "project"})
	if err != nil {
		t.Fatal(err)
	}
	cs := connectServer(t, srv)
	result, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_exec", Arguments: ExecIn{Host: "h", Argv: []string{"true"}}})
	if err != nil {
		t.Fatal(err)
	}
	if serverErr := <-done; serverErr != nil {
		t.Fatal(serverErr)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var envelope proto.ErrorEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if !result.IsError || envelope.Validate() != nil || envelope.Code != proto.CodeReleaseUntrusted || envelope.ExecutionState != proto.StateNotSent || envelope.OperationID != <-ids || envelope.OperationID == "" {
		t.Fatalf("MCP flattened typed broker error: %s", raw)
	}
}

func TestMCPRejectsForgedStructuredError(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	srv := New(c)
	mcp.AddTool(srv, &mcp.Tool{Name: "test_forged_envelope", Description: "regression"}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, struct{}, error) {
		e := proto.NewError(proto.CodeReleaseUntrusted, "op_forged_mcp_error", proto.StateNotSent)
		e.Message = "unregistered-private-value"
		return nil, struct{}{}, e
	})
	cs := connectServer(t, srv)
	r, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: "test_forged_envelope", Arguments: struct{}{}})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(r.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var envelope proto.ErrorEnvelope
	if json.Unmarshal(raw, &envelope) != nil || !r.IsError || envelope.Validate() != nil || envelope.Code != proto.CodeInternalFailure || envelope.ExecutionState != proto.StatePossiblyExecuted || strings.Contains(string(raw), "private-value") {
		t.Fatalf("MCP trusted invalid envelope: %s", raw)
	}
}
