//go:build !windows

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const blockedPipeHelperEnv = "RDEV_BLOCKED_PIPE_HELPER"

func TestBlockedPipeAgentHelper(t *testing.T) {
	if os.Getenv(blockedPipeHelperEnv) == "" {
		return
	}
	state := os.Getenv("RDEV_BLOCKED_PIPE_STATE")
	serveAgentWithWriteTimeout(state, os.Stdin, os.Stdout, os.Exit, 20*time.Millisecond)
	os.Exit(0)
}

func startBlockedPipeHelper(t *testing.T, state string) (*exec.Cmd, *bufio.Writer, io.ReadCloser, *bytes.Buffer) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestBlockedPipeAgentHelper$")
	cmd.Env = append(os.Environ(), blockedPipeHelperEnv+"=1", "RDEV_BLOCKED_PIPE_STATE="+state)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, bufio.NewWriter(stdin), stdout, stderr
}

func writeHelperRequest(t *testing.T, writer *bufio.Writer, request *proto.Request) {
	t.Helper()
	if err := json.NewEncoder(writer).Encode(request); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
}

func waitForHelperExit(t *testing.T, cmd *exec.Cmd, stderr *bytes.Buffer) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			t.Fatalf("blocked-pipe helper exit = %v stderr=%q, want status 1", err, stderr.String())
		}
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("blocked-pipe helper did not take bounded exit path; stderr=%q", stderr.String())
	}
}

// One hundred real child processes prove that a kernel-blocked stdout write
// cannot strand the serving agent even when closing that same descriptor does
// not wake the write. Running this test under -race repeats the same 100-cycle
// process-level witness with an instrumented binary.
func TestBlockedPipeProcessExitIsBounded100Cycles(t *testing.T) {
	for i := 0; i < 100; i++ {
		state := filepath.Join(t.TempDir(), "state")
		if err := os.Mkdir(state, 0o700); err != nil {
			t.Fatal(err)
		}
		cmd, stdin, stdout, stderr := startBlockedPipeHelper(t, state)
		// Retain the descriptor but deliberately never read it. This is a real OS
		// pipe/window, not a fake Writer that cooperates with Close.
		_ = stdout
		writeHelperRequest(t, stdin, &proto.Request{
			ID: "blocked", OperationID: fmt.Sprintf("op_%016x", i+1), ClientID: "client_0123456789abcdef",
			Op: proto.OpExec, Exec: &proto.ExecParams{
				Argv: []string{"head", "-c", strconv.Itoa(512 << 10), "/dev/zero"}, MaxOutputBytes: 512 << 10,
			},
		})
		waitForHelperExit(t, cmd, stderr)
	}
}

func TestBlockedPipeExitReapsAttachedGroup(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(state, "attached-pgid")
	cmd, stdin, stdout, stderr := startBlockedPipeHelper(t, state)
	_ = stdout
	writeHelperRequest(t, stdin, &proto.Request{
		ID: "attached", OperationID: "op_0000000000007001", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, StreamWindowBytes: 1 << 20,
		Exec: &proto.ExecParams{Argv: []string{
			"sh", "-c", `echo $$ > "$1"; head -c 524288 /dev/zero; sleep 30 & wait`, "sh", pidFile,
		}, MaxOutputBytes: 512 << 10},
	})
	pgid := waitForPIDFile(t, pidFile)
	waitForHelperExit(t, cmd, stderr)
	if err := syscall.Kill(-pgid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("attached process group %d survived blocked-pipe exit: %v", pgid, err)
	}
}

func TestBlockedPipeExitDoesNotKillDetachedSupervisor(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	cmd, stdin, stdout, stderr := startBlockedPipeHelper(t, state)
	writeHelperRequest(t, stdin, &proto.Request{
		ID: "detached", OperationID: "op_0000000000007101", ClientID: "client_0123456789abcdef",
		Op: proto.OpJobStart, Job: &proto.JobParams{
			Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "sleep 30"}},
		},
	})
	decoder := json.NewDecoder(stdout)
	var supervisor int
	deadline := time.Now().Add(3 * time.Second)
	for supervisor == 0 && time.Now().Before(deadline) {
		var response proto.Response
		if err := decoder.Decode(&response); err != nil {
			t.Fatalf("read detached start response: %v", err)
		}
		if response.ID == "detached" && response.Terminal && response.Job != nil && response.Job.Info != nil {
			supervisor = response.Job.Info.PID
		}
	}
	if supervisor <= 0 {
		t.Fatal("detached start returned no supervisor pid")
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-supervisor, syscall.SIGKILL)
		waitProcessGroupGone(supervisor)
	})

	writeHelperRequest(t, stdin, &proto.Request{
		ID: "blocked", OperationID: "op_0000000000007102", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, Exec: &proto.ExecParams{
			Argv: []string{"head", "-c", strconv.Itoa(512 << 10), "/dev/zero"}, MaxOutputBytes: 512 << 10,
		},
	})
	waitForHelperExit(t, cmd, stderr)
	if err := syscall.Kill(-supervisor, 0); err != nil {
		t.Fatalf("detached supervisor group %d was killed by connection teardown: %v", supervisor, err)
	}
}
