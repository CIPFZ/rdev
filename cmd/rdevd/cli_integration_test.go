package main

// This test deliberately starts the real rdev CLI.  The daemon is a small
// helper process running the production serveConn loop with a real broker
// Service; its dispatcher is overridden only to avoid requiring an SSH host.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestCLIUsesSharedBrokerService(t *testing.T) {
	if os.Getenv("RDEV_CLI_DAEMON_HELPER") == "1" {
		runCLIBrokerDaemon(t)
		return
	}

	root := repoRoot(t)
	tmp := t.TempDir()
	// Darwin limits Unix socket paths to a short sockaddr.sun_path. Keep the
	// socket in /tmp even though binaries and event data use the test directory.
	socket := filepath.Join("/tmp", fmt.Sprintf("rdevd-cli-%d.sock", os.Getpid()))
	events := filepath.Join(tmp, "events.jsonl")
	cli := filepath.Join(tmp, "rdev")
	_ = os.Remove(socket)
	defer os.Remove(socket)
	goTool := filepath.Join(runtime.GOROOT(), "bin", "go")
	build := exec.Command(goTool, "build", "-o", cli, "./cmd/rdev")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build rdev: %v\n%s", err, out)
	}

	daemon := exec.Command(os.Args[0], "-test.run=^TestCLIUsesSharedBrokerService$", "-test.v")
	daemon.Env = append(os.Environ(), "RDEV_CLI_DAEMON_HELPER=1", "RDEV_CLI_SOCKET="+socket, "RDEV_CLI_EVENTS="+events)
	daemon.Stdout, daemon.Stderr = os.Stdout, os.Stderr
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = daemon.Process.Signal(syscall.SIGTERM); _, _ = daemon.Process.Wait() }()
	waitForSocket(t, socket)

	run := func(clientID string) *exec.Cmd {
		cmd := exec.Command(cli, "ping", "remote-that-is-not-an-ssh-host")
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+socket, "RDEV_CLIENT_ID="+clientID, "RDEV_PROJECT_ID=phase5")
		return cmd
	}
	allowed := run("cli-allowed")
	denied := run("cli-denied")
	var allowedBuf, deniedBuf bytes.Buffer
	allowed.Stdout, allowed.Stderr = &allowedBuf, &allowedBuf
	denied.Stdout, denied.Stderr = &deniedBuf, &deniedBuf
	allowedErrCh, deniedErrCh := make(chan error, 1), make(chan error, 1)
	if err := allowed.Start(); err != nil {
		t.Fatal(err)
	}
	if err := denied.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { allowedErrCh <- allowed.Wait() }()
	go func() { deniedErrCh <- denied.Wait() }()
	allowedErr := <-allowedErrCh
	allowedOut := allowedBuf.Bytes()
	if allowedErr != nil {
		t.Fatalf("allowed CLI failed: %v\n%s", allowedErr, allowedOut)
	}
	var ping proto.PingResult
	if err := json.Unmarshal(allowedOut, &ping); err != nil {
		t.Fatalf("CLI did not print direct PingResult: %v\n%s", err, allowedOut)
	}
	if ping.Binary != "broker-test-agent" || ping.OS != "test" {
		t.Fatalf("unexpected broker ping: %+v", ping)
	}
	execCmd := exec.Command(cli, "exec", "remote-that-is-not-an-ssh-host", "--", "echo", "broker")
	execCmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+socket, "RDEV_CLIENT_ID=cli-allowed", "RDEV_PROJECT_ID=phase5")
	execOut, execErr := execCmd.CombinedOutput()
	if execErr != nil || string(execOut) != "broker\n" {
		t.Fatalf("broker exec failed: err=%v output=%q", execErr, execOut)
	}
	readCmd := exec.Command(cli, "read", "remote-that-is-not-an-ssh-host", "/tmp/broker.txt")
	readCmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+socket, "RDEV_CLIENT_ID=cli-allowed", "RDEV_PROJECT_ID=phase5")
	readOut, readErr := readCmd.CombinedOutput()
	if readErr != nil || string(readOut) != "broker-read" {
		t.Fatalf("broker read failed: err=%v output=%q", readErr, readOut)
	}
	writeCmd := exec.Command(cli, "write", "remote-that-is-not-an-ssh-host", "/tmp/broker.txt")
	writeCmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+socket, "RDEV_CLIENT_ID=cli-allowed", "RDEV_PROJECT_ID=phase5")
	writeCmd.Stdin = strings.NewReader("broker-write")
	writeOut, writeErr := writeCmd.CombinedOutput()
	if writeErr != nil || !strings.Contains(string(writeOut), "bytes_written") {
		t.Fatalf("broker write failed: err=%v output=%q", writeErr, writeOut)
	}

	deniedErr := <-deniedErrCh
	deniedOut := deniedBuf.Bytes()
	if deniedErr == nil || !strings.Contains(string(deniedOut), "denied by default") {
		t.Fatalf("policy-denied CLI unexpectedly succeeded: err=%v output=%s", deniedErr, deniedOut)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(events)
		if strings.Contains(string(data), `"client":"cli-allowed"`) && !strings.Contains(string(data), `"client":"cli-denied"`) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, _ := os.ReadFile(events)
	t.Fatalf("dispatcher evidence missing or policy-denied request dispatched: %s", data)
}

func runCLIBrokerDaemon(t *testing.T) {
	t.Helper()
	socket, events := os.Getenv("RDEV_CLI_SOCKET"), os.Getenv("RDEV_CLI_EVENTS")
	listener, err := broker.Listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	service := broker.NewService(nil)
	allowed := broker.Owner{ClientID: "cli-allowed", ProjectID: "phase5"}
	if err := service.Grant(allowed, "ping"); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"exec", "read_file", "write_file"} {
		if err := service.Grant(allowed, operation); err != nil {
			t.Fatal(err)
		}
	}
	service.SetDispatcher(func(_ context.Context, host string, req *proto.Request) (*proto.Response, error) {
		f, err := os.OpenFile(events, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		owner := req.ClientID
		line, _ := json.Marshal(map[string]string{"client": owner, "host": host, "op": req.Op})
		_, _ = fmt.Fprintln(f, string(line))
		if req.Op == proto.OpExec {
			return &proto.Response{OK: true, Exec: &proto.ExecResult{Terminal: true, Execution: proto.StateCompleted, ExitCode: 0, Stdout: "broker\n"}}, nil
		}
		if req.Op == proto.OpReadFile {
			return &proto.Response{OK: true, Read: &proto.ReadResult{Terminal: true, Execution: proto.StateCompleted, Content: "broker-read"}}, nil
		}
		if req.Op == proto.OpWriteFile {
			return &proto.Response{OK: true, Cat: &proto.WriteResult{Terminal: true, Execution: proto.StateCompleted, BytesWritten: len(req.Cat.Content)}}, nil
		}
		return &proto.Response{OK: true, Ping: &proto.PingResult{Version: 3, Binary: "broker-test-agent", OS: "test", Arch: "test"}}, nil
	})
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			go serveConn(conn, service)
		}
	}()
	select {}
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("broker socket did not appear: %s", path)
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}
