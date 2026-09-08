package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestDurableJobStartReplaySurvivesCacheLossAndRemoval(t *testing.T) {
	state := t.TempDir()
	proof := filepath.Join(state, "proof")
	req := &proto.Request{Op: proto.OpJobStart, ClientID: "principal_job_identity", OperationID: "op_job_identity_123", Job: &proto.JobParams{DurableStart: true, Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf once >> "$1"`, "proof", proof}}}}
	first, err := jobStartRequest(req, state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobWait(&proto.JobParams{ID: first.Info.ID, WaitTimeoutSec: 5}, state); err != nil {
		t.Fatal(err)
	}
	req.Replay = true
	// A fresh operation cache must admit only the durable recovery path.
	cache := newOperationCache(nil, 0, 0)
	if b := cache.begin(req, func() {}); b.envelope != nil {
		t.Fatal(b.envelope)
	}
	second, err := jobStartRequest(req, state)
	if err != nil || second.Info.ID != first.Info.ID || second.Info.PID != first.Info.PID {
		t.Fatal("cache loss replaced job", err)
	}
	changed := *req
	params := *req.Job
	spec := *params.Spec
	spec.Argv = []string{"false"}
	params.Spec = &spec
	changed.Job = &params
	if _, err := jobStartRequest(&changed, state); err == nil {
		t.Fatal("durable start digest changed")
	}
	if _, err := jobRm(&proto.JobParams{ID: first.Info.ID}, state); err != nil {
		t.Fatal(err)
	}
	for _, replay := range []bool{true, false} {
		req.Replay = replay
		if _, err := jobStartRequest(req, state); err == nil {
			t.Fatal("removed job executed again")
		}
	}
	data, _ := os.ReadFile(proof)
	if string(data) != "once" {
		t.Fatalf("execution proof=%q", data)
	}
	req.OperationID = "op_missing_identity_123"
	req.Replay = true
	if _, err := jobStartRequest(req, state); err == nil {
		t.Fatal("unknown replay created job")
	}
}

func TestDurableJobStartFailedMetadataCannotRunOnRetry(t *testing.T) {
	state := t.TempDir()
	proof := filepath.Join(state, "proof")
	req := &proto.Request{Op: proto.OpJobStart, ClientID: "principal_job_identity", OperationID: "op_job_identity_fail", Job: &proto.JobParams{DurableStart: true, Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf forbidden > "$1"`, "proof", proof}}}}
	previous := writeJobMeta
	writeJobMeta = func(string, any) error { return errors.New("metadata failure") }
	defer func() { writeJobMeta = previous }()
	if _, err := jobStartRequest(req, state); err == nil {
		t.Fatal("metadata failure acknowledged")
	}
	writeJobMeta = previous
	if _, err := jobStartRequest(req, state); err == nil {
		t.Fatal("failed identity executed on retry")
	}
	if _, err := os.Stat(proof); !os.IsNotExist(err) {
		t.Fatal("failed start executed command")
	}
}
