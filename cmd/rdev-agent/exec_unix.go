//go:build !windows

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// buildCmd turns ExecParams into an *exec.Cmd.
//
// When LoginShell is set the command becomes:
//
//	bash -lc 'exec "$@"' rdev <argv...>
//
// The profile is sourced, then `exec "$@"` replaces the shell with the target
// process. Because argv arrives as positional parameters rather than embedded
// in the script text, the shell never re-parses it: a filename containing
// spaces, quotes, or `$(...)` is passed through byte-for-byte.

func buildCmd(p *proto.ExecParams) (*exec.Cmd, error) {
	if len(p.Argv) == 0 {
		return nil, invalidRequestError("argv must not be empty")
	}

	var cmd *exec.Cmd
	if p.LoginShell {
		shell := os.Getenv("SHELL")
		if shell == "" {
			shell = "/bin/bash"
		}
		args := append([]string{"-lc", `exec "$@"`, "rdev"}, p.Argv...)
		cmd = exec.Command(shell, args...)
	} else {
		cmd = exec.Command(p.Argv[0], p.Argv[1:]...)
	}

	if p.Cwd != "" {
		dir := expandHome(p.Cwd)
		info, err := os.Stat(dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, objectNotFoundError(err)
			}
			return nil, err
		}
		if !info.IsDir() {
			return nil, invalidRequestError("cwd is not a directory")
		}
		cmd.Dir = dir
	}

	cmd.Env = os.Environ()
	for k, v := range p.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd, nil
}

func doExecContextStream(ctx context.Context, p *proto.ExecParams, stdoutHook, stderrHook func([]byte)) (*proto.ExecResult, error) {
	cmd, err := buildCmd(p)
	if err != nil {
		return nil, err
	}

	limit := p.MaxOutputBytes
	if limit < 0 || int64(limit) > proto.AbsoluteOutputBytes {
		return nil, limitExceededError("max_output_bytes is outside the hard limit")
	}
	if limit == 0 {
		limit = defaultMaxOutput
	}
	timeout, err := proto.ResolveTimeout(p.TimeoutSec, proto.DefaultExecTimeoutSeconds)
	if err != nil {
		return nil, err
	}
	if int64(len(p.Stdin)) > proto.AbsoluteRequestFrameBytes {
		return nil, limitExceededError("stdin exceeds the hard limit")
	}
	stdout := &capWriter{cap: limit, hook: stdoutHook}
	stderr := &capWriter{cap: limit, hook: stderrHook}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if p.Stdin != "" {
		cmd.Stdin = strings.NewReader(p.Stdin)
	}

	// Put the child in its own process group so a timeout kill reaches the
	// whole tree, not just the immediate child.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	start := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, processStartError(err)
	}
	pgid := cmd.Process.Pid // Setpgid makes the child's PID the original PGID.
	exited, stopObserving, observeErr := observeProcessExit(cmd.Process.Pid)
	if observeErr != nil {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		_ = cmd.Wait()
		return nil, fmt.Errorf("observe foreground process: %w", observeErr)
	}
	defer stopObserving()

	var timedOut bool
	var canceled bool
	var timer <-chan time.Time
	if timeout > 0 {
		timer = time.After(time.Duration(timeout) * time.Second)
	}
	select {
	case <-exited:
		err = cmd.Wait()
	case <-ctx.Done():
		canceled = true
		terminateProcessGroup(pgid)
		err = cmd.Wait()
		waitProcessGroupGone(pgid)
		err = ctx.Err()
	case <-timer:
		timedOut = true
		terminateProcessGroup(pgid)
		err = cmd.Wait()
		waitProcessGroupGone(pgid)
	}

	stdoutText, stdoutB64, stdoutRetained := stdout.payload()
	stderrText, stderrB64, stderrRetained := stderr.payload()
	stdoutTruncation, _ := proto.NewTruncation(stdout.total, stdoutRetained)
	stderrTruncation, _ := proto.NewTruncation(stderr.total, stderrRetained)
	res := &proto.ExecResult{
		Stdout:           stdoutText,
		Stderr:           stderrText,
		StdoutB64:        stdoutB64,
		StderrB64:        stderrB64,
		StdoutBytes:      stdout.total,
		StderrBytes:      stderr.total,
		Truncated:        stdout.truncated() || stderr.truncated(),
		StdoutTruncation: stdoutTruncation,
		StderrTruncation: stderrTruncation,
		TimedOut:         timedOut,
		DurationMS:       time.Since(start).Milliseconds(),
	}
	if timedOut {
		res.ExitCode = -1
		return res, nil
	}
	if canceled {
		res.ExitCode = -1
		return res, err
	}
	// A non-zero exit is data, not a transport error: report it in ExitCode
	// and let the caller decide. Only failures to *run* the command error out.
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		res.ExitCode = ee.ExitCode()
	} else if err != nil {
		return nil, err
	}
	return res, nil
}

var processGroupGrace = 250 * time.Millisecond

// terminateProcessGroup gives cooperative children a brief TERM window, then
// escalates the original request-owned group independently of leader exit. The
// leader is deliberately left unreaped by observeProcessExit until this helper
// returns, keeping its PID/PGID reserved throughout the reuse-sensitive window.

func terminateProcessGroup(pgid int) {
	if pgid <= 0 {
		return
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	timer := time.NewTimer(processGroupGrace)
	defer timer.Stop()
	<-timer.C
	if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
		return
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return
	}
}

func waitProcessGroupGone(pgid int) {
	deadline := time.Now().Add(processGroupGrace)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pgid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}
