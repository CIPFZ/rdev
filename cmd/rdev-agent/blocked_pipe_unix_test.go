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
	"strings"
	"sync"
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

type blockedPipeHelper struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	writer   *bufio.Writer
	stdout   io.ReadCloser
	stderr   *bytes.Buffer
	state    string
	waitDone chan struct{}
	waitErr  error

	mu               sync.Mutex
	attachedPIDFiles []string
	expectedDetached int
	cleanupOnce      sync.Once
}

func startBlockedPipeHelper(t *testing.T, state string) *blockedPipeHelper {
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
	helper := &blockedPipeHelper{
		cmd: cmd, stdin: stdin, writer: bufio.NewWriter(stdin), stdout: stdout,
		stderr: stderr, state: state, waitDone: make(chan struct{}),
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Register as the very first action after Start: every later Decode,
	// barrier, or assertion failure must still reap this exact helper and its
	// test-owned process groups.
	t.Cleanup(helper.cleanup)
	go func() {
		helper.waitErr = cmd.Wait()
		close(helper.waitDone)
	}()
	return helper
}

func (h *blockedPipeHelper) trackAttachedProcessGroup(pidFile string) {
	h.mu.Lock()
	h.attachedPIDFiles = append(h.attachedPIDFiles, pidFile)
	h.mu.Unlock()
}

func (h *blockedPipeHelper) cleanup() {
	h.cleanupOnce.Do(func() {
		h.mu.Lock()
		pidFiles := append([]string(nil), h.attachedPIDFiles...)
		expectedDetached := h.expectedDetached
		h.mu.Unlock()

		// Detached supervisors publish their exact group IDs in this helper's
		// private state directory before replying. Stop them while the helper and
		// its cmd.Wait goroutine are still alive, so supervisors are reaped by their
		// real parent rather than becoming slow-to-reap init-owned zombies.
		jobDirs := h.waitForDetachedRecords(expectedDetached, 3*time.Second)
		for _, dir := range jobDirs {
			meta, err := readMeta(dir)
			if err != nil {
				continue
			}
			if child := readChildPID(dir); child > 0 {
				// Let the supervisor reap its direct child before killing any
				// remaining group members. Killing both simultaneously can leave a
				// short-lived orphan zombie that makes exact liveness checks flaky.
				_ = syscall.Kill(child, syscall.SIGKILL)
				waitForProcessTargetGone(child, false, 3*time.Second)
			}
			killExactProcessGroup(meta.PID)
		}

		_ = h.stdin.Close()
		_ = h.stdout.Close()
		select {
		case <-h.waitDone:
		case <-time.After(3 * time.Second):
			if h.cmd.Process != nil {
				_ = h.cmd.Process.Kill()
			}
			select {
			case <-h.waitDone:
			case <-time.After(3 * time.Second):
			}
		}

		for _, pidFile := range pidFiles {
			if pgid := readPIDFile(pidFile); pgid > 0 {
				killExactProcessGroup(pgid)
			}
		}
	})
}

func (h *blockedPipeHelper) waitForDetachedRecords(want int, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for {
		jobDirs, _ := filepath.Glob(filepath.Join(h.state, "jobs", "*"))
		recorded := jobDirs[:0]
		ready := 0
		for _, dir := range jobDirs {
			if _, err := readMeta(dir); err != nil {
				continue
			}
			recorded = append(recorded, dir)
			// meta.json is written by the agent just after starting the
			// supervisor; child.json is the supervisor's publication barrier.
			// Waiting for it closes the fork-vs-group-signal window. A status
			// record is the equivalent barrier for a child that already exited.
			if readChildPID(dir) > 0 {
				ready++
				continue
			}
			if _, err := os.Stat(filepath.Join(dir, "status.json")); err == nil {
				ready++
			}
		}
		if ready >= want || want == 0 || time.Now().After(deadline) {
			// On timeout, still return every exact meta record. The publication
			// barrier is preferred because it closes the fork window, but omitting
			// a known supervisor entirely would be a worse cleanup fallback.
			return recorded
		}
		runtimeYield()
	}
}

func readPIDFile(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func killExactProcessGroup(pgid int) {
	if pgid <= 0 {
		return
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	waitForProcessTargetGone(pgid, true, 10*time.Second)
}

func waitForProcessTargetGone(pid int, group bool, timeout time.Duration) bool {
	target := pid
	if group {
		target = -pid
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(target, 0); errors.Is(err, syscall.ESRCH) {
			return true
		}
		runtimeYield()
	}
	return false
}

func writeHelperRequest(t *testing.T, helper *blockedPipeHelper, request *proto.Request) {
	t.Helper()
	if request.Op == proto.OpJobStart {
		helper.mu.Lock()
		helper.expectedDetached++
		helper.mu.Unlock()
	}
	if err := json.NewEncoder(helper.writer).Encode(request); err != nil {
		t.Fatal(err)
	}
	if err := helper.writer.Flush(); err != nil {
		t.Fatal(err)
	}
}

func waitForHelperExit(t *testing.T, helper *blockedPipeHelper) {
	t.Helper()
	select {
	case <-helper.waitDone:
		err := helper.waitErr
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
			t.Fatalf("blocked-pipe helper exit = %v stderr=%q, want status 1", err, helper.stderr.String())
		}
	case <-time.After(3 * time.Second):
		_ = helper.cmd.Process.Kill()
		<-helper.waitDone
		t.Fatalf("blocked-pipe helper did not take bounded exit path; stderr=%q", helper.stderr.String())
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
		helper := startBlockedPipeHelper(t, state)
		// Retain the descriptor but deliberately never read it. This is a real OS
		// pipe/window, not a fake Writer that cooperates with Close.
		_ = helper.stdout
		writeHelperRequest(t, helper, &proto.Request{
			ID: "blocked", OperationID: fmt.Sprintf("op_%016x", i+1), ClientID: "client_0123456789abcdef",
			Op: proto.OpExec, Exec: &proto.ExecParams{
				Argv: []string{"head", "-c", strconv.Itoa(512 << 10), "/dev/zero"}, MaxOutputBytes: 512 << 10,
			},
		})
		waitForHelperExit(t, helper)
	}
}

func TestBlockedPipeExitReapsAttachedGroup(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(state, "attached-pgid")
	helper := startBlockedPipeHelper(t, state)
	helper.trackAttachedProcessGroup(pidFile)
	_ = helper.stdout
	writeHelperRequest(t, helper, &proto.Request{
		ID: "attached", OperationID: "op_0000000000007001", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, StreamWindowBytes: 1 << 20,
		Exec: &proto.ExecParams{Argv: []string{
			"sh", "-c", `echo $$ > "$1"; head -c 524288 /dev/zero; sleep 30 & wait`, "sh", pidFile,
		}, MaxOutputBytes: 512 << 10},
	})
	pgid := waitForPIDFile(t, pidFile)
	waitForHelperExit(t, helper)
	if err := syscall.Kill(-pgid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("attached process group %d survived blocked-pipe exit: %v", pgid, err)
	}
}

func TestBlockedPipeExitDoesNotKillDetachedSupervisor(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	helper := startBlockedPipeHelper(t, state)
	writeHelperRequest(t, helper, &proto.Request{
		ID: "detached", OperationID: "op_0000000000007101", ClientID: "client_0123456789abcdef",
		Op: proto.OpJobStart, Job: &proto.JobParams{
			Spec: &proto.ExecParams{Argv: []string{"sleep", "30"}},
		},
	})
	decoder := json.NewDecoder(helper.stdout)
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
	writeHelperRequest(t, helper, &proto.Request{
		ID: "blocked", OperationID: "op_0000000000007102", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, Exec: &proto.ExecParams{
			Argv: []string{"head", "-c", strconv.Itoa(512 << 10), "/dev/zero"}, MaxOutputBytes: 512 << 10,
		},
	})
	waitForHelperExit(t, helper)
	if err := syscall.Kill(-supervisor, 0); err != nil {
		t.Fatalf("detached supervisor group %d was killed by connection teardown: %v", supervisor, err)
	}
}

func waitForPIDFileUntil(t *testing.T, path string, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pid := readPIDFile(path); pid > 0 {
			return pid
		}
		runtimeYield()
	}
	t.Fatalf("process did not reach pid-file barrier %s", path)
	return 0
}

func TestBlockedPipeCleanupCoversEarlyFailures(t *testing.T) {
	for _, mode := range []string{"barrier", "decode", "assertion"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			var helperPID, targetPGID int
			var injected error
			if ok := t.Run("early-return", func(t *testing.T) {
				state := filepath.Join(root, "state")
				if err := os.MkdirAll(state, 0o700); err != nil {
					t.Fatal(err)
				}
				helper := startBlockedPipeHelper(t, state)
				helperPID = helper.cmd.Process.Pid

				switch mode {
				case "barrier":
					if readPIDFile(filepath.Join(root, "never-created")) == 0 {
						injected = errors.New("injected pid-file barrier failure")
						return
					}
					t.Fatal("injected pid-file barrier unexpectedly succeeded")
				case "decode":
					writeHelperRequest(t, helper, &proto.Request{
						ID: "detached", OperationID: "op_0000000000007201", ClientID: "client_0123456789abcdef",
						Op: proto.OpJobStart, Job: &proto.JobParams{
							Spec: &proto.ExecParams{Argv: []string{"sleep", "30"}},
						},
					})
					decoder := json.NewDecoder(helper.stdout)
					for targetPGID == 0 {
						var response proto.Response
						if err := decoder.Decode(&response); err != nil {
							t.Fatalf("read detached setup response: %v", err)
						}
						if response.ID == "detached" && response.Terminal && response.Job != nil && response.Job.Info != nil {
							targetPGID = response.Job.Info.PID
						}
					}
					var response proto.Response
					if err := json.NewDecoder(strings.NewReader("!\n")).Decode(&response); err != nil {
						injected = fmt.Errorf("injected detached response decode failure: %w", err)
						return
					}
					t.Fatal("injected decoder unexpectedly succeeded")
				case "assertion":
					pidFile := filepath.Join(root, "target-pgid")
					helper.trackAttachedProcessGroup(pidFile)
					writeHelperRequest(t, helper, &proto.Request{
						ID: "attached", OperationID: "op_0000000000007202", ClientID: "client_0123456789abcdef",
						Op: proto.OpExec, StreamWindowBytes: 1 << 20,
						Exec: &proto.ExecParams{Argv: []string{
							"sh", "-c", `echo $$ > "$1"; sleep 30 & wait`, "sh", pidFile,
						}},
					})
					targetPGID = waitForPIDFileUntil(t, pidFile, 3*time.Second)
					injected = errors.New("injected assertion failure after attached group startup")
					return
				default:
					t.Fatalf("unknown cleanup probe mode %q", mode)
				}
			}); !ok {
				t.Fatalf("failure injection setup %q failed unexpectedly", mode)
			}
			if injected == nil {
				t.Fatalf("failure injection %q did not trigger", mode)
			}
			waitForExactProcessGone(t, helperPID, false)
			if mode != "barrier" {
				waitForExactProcessGone(t, targetPGID, true)
			}
		})
	}
}

func waitForExactProcessGone(t *testing.T, pid int, group bool) {
	t.Helper()
	if waitForProcessTargetGone(pid, group, 10*time.Second) {
		return
	}
	target := pid
	if group {
		target = -pid
	}
	t.Fatalf("cleanup left exact process target %d alive", target)
}
