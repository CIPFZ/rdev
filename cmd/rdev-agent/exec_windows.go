package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/winutil"
)

func buildCmd(p *proto.ExecParams) (*exec.Cmd, error) {
	if len(p.Argv) == 0 {
		return nil, invalidRequestError("argv must not be empty")
	}
	if p.LoginShell {
		return nil, invalidRequestError("POSIX login_shell is unsupported on Windows; pass powershell.exe or pwsh.exe explicitly")
	}
	// Batch files are parsed by cmd.exe with different quoting rules. Requiring
	// an explicit shell keeps their metacharacters out of direct-argv execution.
	ext := strings.ToLower(filepath.Ext(p.Argv[0]))
	if ext == ".bat" || ext == ".cmd" {
		return nil, invalidRequestError("batch files require explicit cmd.exe /c execution")
	}
	cmd := exec.Command(p.Argv[0], p.Argv[1:]...)
	resolvedExt := strings.ToLower(filepath.Ext(cmd.Path))
	if resolvedExt == ".bat" || resolvedExt == ".cmd" {
		return nil, invalidRequestError("batch files require explicit cmd.exe /c execution")
	}
	if p.Cwd != "" {
		cmd.Dir = expandHome(p.Cwd)
		if err := winutil.ValidatePath(cmd.Dir); err != nil {
			return nil, invalidRequestError(err.Error())
		}
		st, err := os.Stat(cmd.Dir)
		if err != nil {
			return nil, objectNotFoundError(err)
		}
		if !st.IsDir() {
			return nil, invalidRequestError("cwd is not a directory")
		}
	}
	cmd.Env = os.Environ()
	for k, v := range p.Env {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) {
			return nil, invalidRequestError("invalid environment entry")
		}
		for i := len(cmd.Env) - 1; i >= 0; i-- {
			key, _, _ := strings.Cut(cmd.Env[i], "=")
			if strings.EqualFold(key, k) {
				cmd.Env = append(cmd.Env[:i], cmd.Env[i+1:]...)
			}
		}
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.WaitDelay = 2 * time.Second
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
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	stdout, stderr := &capWriter{cap: limit, hook: stdoutHook}, &capWriter{cap: limit, hook: stderrHook}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if p.Stdin != "" {
		cmd.Stdin = strings.NewReader(p.Stdin)
	}
	started := time.Now()
	tree, err := winutil.StartTree(cmd, "")
	if err != nil {
		return nil, processStartError(err)
	}
	defer tree.Close()
	var timer *time.Timer
	var deadline <-chan time.Time
	if timeout > 0 {
		timer = time.NewTimer(time.Duration(timeout) * time.Second)
		defer timer.Stop()
		deadline = timer.C
	}
	timedOut, canceled := false, false
	select {
	case <-tree.Done:
	case <-ctx.Done():
		canceled = true
	case <-deadline:
		timedOut = true
	}
	_ = tree.Kill()
	err = cmd.Wait()
	so, sob, sor := stdout.payload()
	se, seb, ser := stderr.payload()
	sot, _ := proto.NewTruncation(stdout.total, sor)
	set, _ := proto.NewTruncation(stderr.total, ser)
	result := &proto.ExecResult{Stdout: so, Stderr: se, StdoutB64: sob, StderrB64: seb, StdoutBytes: stdout.total, StderrBytes: stderr.total, StdoutTruncation: sot, StderrTruncation: set, Truncated: stdout.truncated() || stderr.truncated(), TimedOut: timedOut, DurationMS: time.Since(started).Milliseconds()}
	if canceled || timedOut {
		result.ExitCode = -1
		if canceled {
			return result, ctx.Err()
		}
		return result, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		result.ExitCode = ee.ExitCode()
	} else if err != nil {
		return nil, err
	}
	return result, nil
}
