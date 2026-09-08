package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestJobStopTermRetainsSupervisorOutputAndChildIdentity(t *testing.T) {
	state := t.TempDir()
	ready := filepath.Join(state, "ready")
	job, err := jobStart(&proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `trap 'printf stopped; exit 0' TERM; printf ready; touch "$1"; while :; do sleep 1; done`, "job-test", ready}}}, state)
	if err != nil {
		t.Fatal(err)
	}
	defer jobStop(&proto.JobParams{ID: job.Info.ID, Signal: "KILL"}, state)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	var child struct {
		PID      int    `json:"child_pid"`
		Identity string `json:"process_identity"`
	}
	if err := readJSON(filepath.Join(jobDir(state, job.Info.ID), "child.json"), &child); err != nil {
		t.Fatal(err)
	}
	if child.PID <= 0 || child.Identity == "" {
		t.Fatal("child lacks persisted process identity")
	}
	if _, err := jobStop(&proto.JobParams{ID: job.Info.ID, Signal: "TERM", GraceSec: 3}, state); err != nil {
		t.Fatal(err)
	}
	result, err := jobWait(&proto.JobParams{ID: job.Info.ID, WaitTimeoutSec: 5, TailOnExit: 1}, state)
	if err != nil {
		t.Fatal(err)
	}
	if result.TimedOut || result.Info.State == proto.JobRunning || result.Logs != "readystopped" {
		t.Fatalf("TERM lost output: %+v", result)
	}
	logs, err := jobLogs(&proto.JobParams{ID: job.Info.ID, Stream: "stdout"}, state)
	if err != nil || logs.LogLedger.OriginalBytes != int64(len("readystopped")) || strings.Count(logs.Logs, "stopped") != 1 {
		t.Fatalf("TERM failed durable ledger or relayed twice: %+v %v", logs, err)
	}
}
