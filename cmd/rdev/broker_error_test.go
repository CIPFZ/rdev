package main

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestCLISharedErrorRetainsCodeAndOperationIdentity(t *testing.T) {
	dir, err := os.MkdirTemp("", "rdev-cli-error-")
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
	t.Setenv("RDEV_BROKER_SOCKET", socket)
	t.Setenv("RDEV_CLIENT_ID", "cli")
	t.Setenv("RDEV_PROJECT_ID", "project")
	done := make(chan error, 1)
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
		err = e.Encode((broker.Response{ID: req.ID}).WithError(proto.NewError(proto.CodeReleaseChannel, req.Wire.OperationID, proto.StateNotSent), req.Wire.OperationID))
		done <- err
	}()
	const id = "op_cli_release_error"
	t.Setenv("RDEV_OPERATION_ID", id)
	err = brokerExec(t.Context(), []string{"h", "--", "true"})
	if serverErr := <-done; serverErr != nil {
		t.Fatal(serverErr)
	}
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeReleaseChannel || envelope.OperationID != id {
		t.Fatal("CLI flattened broker envelope", err)
	}
	line := cliErrorLine(nil, envelope)
	for _, field := range []string{"code=release.channel_denied", "execution_state=not_sent", "operation_id=" + id} {
		if !strings.Contains(line, field) {
			t.Fatal("CLI omitted structured error field", line)
		}
	}
	forged := *envelope
	forged.Message = "private-value"
	if line := cliErrorLine(nil, &forged); strings.Contains(line, "private-value") || !strings.Contains(line, "execution_state=possibly_executed") {
		t.Fatal("CLI trusted forged envelope", line)
	}
}
