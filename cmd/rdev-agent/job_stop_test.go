package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// This helper models the old supervisor contract: it has no signal relay,
// records only the child's PID, and publishes buffered output after Wait.
func TestLegacyStopSupervisorHelper(t *testing.T) {
	dir := os.Getenv("RDEV_LEGACY_STOP_HELPER")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	child := exec.Command("sh", "-c", `trap 'printf stopped; exit 0' TERM; printf ready; touch "$1"; while :; do sleep 1; done`, "legacy-stop", filepath.Join(dir, "ready"))
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var output bytes.Buffer
	child.Stdout = &output
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer syscall.Kill(-child.Process.Pid, syscall.SIGKILL)
	identity, err := processIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: filepath.Base(dir), PID: os.Getpid(), ProcessIdentity: identity}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "child.json"), map[string]int{"child_pid": child.Process.Pid}); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "stdout"), output.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "status.json"), map[string]any{"exit_code": 0, "ended_at": time.Now().UTC().Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
}

func TestJobStopTermPreservesLegacySupervisorFlush(t *testing.T) {
	state := t.TempDir()
	dir := jobDir(state, "legacy-stop")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	supervisor := exec.Command(os.Args[0], "-test.run=^TestLegacyStopSupervisorHelper$")
	supervisor.Env = append(os.Environ(), "RDEV_LEGACY_STOP_HELPER="+dir)
	supervisor.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := supervisor.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Wait() }()
	t.Cleanup(func() {
		if child := readChildPID(dir); child > 0 {
			_ = syscall.Kill(-child, syscall.SIGKILL)
		}
		_ = supervisor.Process.Kill()
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := os.Stat(filepath.Join(dir, "ready"))
		if err == nil && readChildPID(dir) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("legacy child did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := jobStop(&proto.JobParams{ID: "legacy-stop", Signal: "TERM", GraceSec: 3}, state); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("legacy supervisor was killed before flushing: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("legacy supervisor did not exit after child termination")
	}
	result, err := jobWait(&proto.JobParams{ID: "legacy-stop", TailOnExit: 1}, state)
	if err != nil || result.Logs != "readystopped" || result.Info.State == proto.JobRunning {
		t.Fatalf("legacy final output lost: %+v %v", result, err)
	}
}

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
