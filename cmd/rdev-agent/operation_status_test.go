package main

import (
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestOperationCacheStatusReturnsFinalWithoutReplay(t *testing.T) {
	c := newOperationCache(nil, 8, 0)
	req := &proto.Request{Op: proto.OpExec, OperationID: "op_0123456789abcdef", ClientID: "client_0123456789abcdef"}
	begin := c.begin(req, nil)
	if begin.record == nil {
		t.Fatalf("begin returned no record: %+v", begin)
	}
	final := &proto.Response{OperationID: req.OperationID, Terminal: true, Execution: proto.StateCompleted, OK: true, Exec: &proto.ExecResult{ExitCode: 0}}
	if !c.finish(begin.record, final) {
		t.Fatal("finish did not publish final response")
	}
	status, err := c.status(req.ClientID, req.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Terminal || !status.OK || status.Final == nil || status.Operation != proto.OpExec {
		t.Fatalf("status = %+v", status)
	}
	if _, err := c.status(req.ClientID, "op_fedcba9876543210"); err == nil {
		t.Fatal("missing operation status unexpectedly succeeded")
	}
}

func TestOperationCacheStatusLoadsPersistedMutation(t *testing.T) {
	dir := t.TempDir()
	c := newOperationCache(nil, 8, 0)
	c.journalDir = dir
	req := &proto.Request{Op: proto.OpEditFile, OperationID: "op_0123456789abcdee", ClientID: "client_0123456789abcdef", Edit: &proto.EditParams{Path: "/tmp/x", Kind: "replace", BaseDigest: "digest"}}
	begin := c.begin(req, nil)
	final := &proto.Response{OperationID: req.OperationID, Terminal: true, Execution: proto.StateCompleted, OK: true, Edit: &proto.EditResult{OperationID: req.OperationID, Terminal: true, Execution: proto.StateCompleted, Committed: true}}
	if !c.finish(begin.record, final) {
		t.Fatal("finish did not publish final response")
	}
	c.persist(begin.record, final)
	restarted := newOperationCache(nil, 8, 0)
	restarted.journalDir = dir
	status, err := restarted.status("client_ffffffffffffffff", req.OperationID)
	if err != nil || !status.Terminal || !status.OK || status.Final == nil {
		t.Fatalf("persisted status=%+v err=%v", status, err)
	}
}
