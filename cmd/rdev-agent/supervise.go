// Supervisor mode for rdev-agent.
//
// A detached job cannot rely on the agent to record its exit code: the agent
// dies with the ssh connection, so its reaper goroutine usually never runs.
// Instead jobStart launches the agent binary in supervisor mode as the direct
// parent of the real command:
//
//	rdev-agent -supervise <jobdir> -- <argv...>
//
// The supervisor is part of the detached session, so it outlives the ssh
// connection, waits for the child, and writes status.json itself. Exit codes
// then survive disconnects, agent restarts, and host reboots.
//
// This keeps the no-shell guarantee: argv is passed as real arguments after
// "--" and exec'd directly.
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/storage"
)

// superviseFlag is the argv[1] value that switches the binary into supervisor
// mode instead of serving the JSON protocol.
const superviseFlag = "-supervise"

// killJobChildGroup terminates the child and every descendant in its private
// process group. The supervisor itself remains outside that group so it can
// reap the child and persist a terminal status after an enforced limit.
func killJobChildGroup(pid int) {
	if pid <= 0 {
		return
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// runSupervisor executes args[0:] as a child, waits for it, and records the
// outcome in <jobDir>/status.json. It never returns; it exits with the child's
// code so `ps` and any outer waiter see a faithful status.
func runSupervisor(jobDir string, argv []string) {
	if len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "rdev-agent -supervise: argv required after --")
		os.Exit(2)
	}
	if err := secureJobDir(jobDir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	policy, err := storage.Load(filepath.Join(jobDir, "storage-policy.json"))
	if err != nil {
		policy = storage.Default()
	}

	// The serving agent owns the creation transaction. Until it publishes
	// meta.json, this process must not launch the user command: if the agent is
	// interrupted in that window, a supervisor that went ahead would become an
	// unobservable runnable orphan. Waiting for a metadata record matching our
	// own pid makes publication the supervisor's start barrier. A crashed
	// parent is detected by reparenting; the supervisor then exits without ever
	// starting argv. There is intentionally no wall-clock timeout while the
	// parent is alive, so slow disks cannot turn a valid start into a false
	// rollback.
	parentPID := os.Getppid()
	if raw := os.Getenv(supervisorParentEnv); raw != "" {
		if expected, err := strconv.Atoi(raw); err == nil && expected > 0 {
			parentPID = expected
		}
	}
	for {
		var meta jobMeta
		if err := readJSON(filepath.Join(jobDir, "meta.json"), &meta); err == nil && meta.PID == os.Getpid() {
			break
		}
		if os.Getppid() != parentPID {
			// The serving agent died before publishing the metadata commit
			// point. Do not leave an unobservable directory behind. Re-check
			// under the same lock used by job_start: the parent may have
			// published metadata concurrently with the reparenting notice, in
			// which case this supervisor must continue and own the job.
			_ = withJobLock(jobDir, func() error {
				var committed jobMeta
				if err := readJSON(filepath.Join(jobDir, "meta.json"), &committed); err == nil && committed.PID == os.Getpid() {
					return nil
				}
				if err := os.RemoveAll(jobDir); err != nil {
					return err
				}
				removeJobLock(jobDir)
				return nil
			})
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	var committed jobMeta
	_ = readJSON(filepath.Join(jobDir, "meta.json"), &committed)
	resources := committed.EffectiveResources
	if resources.FDs > 0 {
		lim := &syscall.Rlimit{Cur: uint64(resources.FDs), Max: uint64(resources.FDs)}
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, lim); err != nil {
			_ = writeJSON(filepath.Join(jobDir, "status.json"), map[string]any{"exit_code": -1, "resource_limit": "fd", "ended_at": time.Now().UTC().Format(time.RFC3339)})
			os.Exit(125)
		}
	}

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdin = nil
	cmd.Env = withoutEnvValue(os.Environ(), supervisorParentEnv)
	// Isolate the child in its own process group. This lets timeout and log-limit
	// enforcement kill the complete descendant tree while keeping the
	// supervisor alive long enough to publish a terminal status. job_stop
	// explicitly signals both the supervisor and child groups.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	so := storage.NewSink(policy.PerJob.MaxStdoutBytes, policy.PerJob.OnLogLimit)
	se := storage.NewSink(policy.PerJob.MaxStderrBytes, policy.PerJob.OnLogLimit)
	// Assign bounded writers directly to Cmd. os/exec owns the internal pipe
	// drain goroutines and Wait waits for those goroutines before returning.
	// Using StdoutPipe/Wait manually can close the pipe before io.Copy drains
	// the final bytes, making a completed job occasionally report empty logs.
	cmd.Stdout = so
	cmd.Stderr = se
	if err := cmd.Start(); err != nil {
		_ = withJobLock(jobDir, func() error {
			if !jobExists(jobDir) {
				return nil
			}
			return writeJSON(filepath.Join(jobDir, "status.json"), map[string]any{
				"exit_code": -1,
				"ended_at":  time.Now().UTC().Format(time.RFC3339),
				"error":     fmt.Sprintf("start failed: %v", err),
			})
		})
		os.Exit(127)
	}

	// Record the real child pid alongside the supervisor pid. This publication
	// participates in the same transaction lock as job_start/job_rm: a starter
	// that is still committing metadata, or a remover that is tearing the record
	// down, must not race a child.json write into a half-removed directory.
	childPublishErr := withJobLock(jobDir, func() error {
		if !jobExists(jobDir) {
			return nil
		}
		return writeJSON(filepath.Join(jobDir, "child.json"), map[string]any{"child_pid": cmd.Process.Pid})
	})
	if childPublishErr != nil {
		// Without child.json a later stop cannot address this isolated process
		// group after the supervisor exits. Never continue with an unobservable
		// runnable job: terminate and reap the complete tree, then make a best
		// effort to publish a terminal failure.
		killJobChildGroup(cmd.Process.Pid)
		_ = cmd.Wait()
		_ = writeJSON(filepath.Join(jobDir, "status.json"), map[string]any{
			"exit_code": -1,
			"ended_at":  time.Now().UTC().Format(time.RFC3339),
			"error":     fmt.Sprintf("publish child identity: %v", childPublishErr),
		})
		os.Exit(125)
	}

	stopLedger := make(chan struct{})
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = writeJSON(filepath.Join(jobDir, "ledger.json"), map[string]any{"stdout_ledger": ledgerProto(so.Ledger()), "stderr_ledger": ledgerProto(se.Ledger())})
			case <-stopLedger:
				return
			}
		}
	}()
	stopLimit := make(chan struct{})
	if policy.PerJob.OnLogLimit == "stop_job" {
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if so.LimitReached() || se.LimitReached() {
						killJobChildGroup(cmd.Process.Pid)
						return
					}
				case <-stopLimit:
					return
				}
			}
		}()
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()
	resourceLimit := ""
	if resources.WallTimeoutSec > 0 {
		timer := time.NewTimer(time.Duration(resources.WallTimeoutSec) * time.Second)
		select {
		case err = <-waitDone:
			timer.Stop()
		case <-timer.C:
			resourceLimit = "wall_timeout"
			killJobChildGroup(cmd.Process.Pid)
			err = <-waitDone
		}
	} else {
		err = <-waitDone
	}
	close(stopLimit)
	close(stopLedger)
	_ = so.Flush(filepath.Join(jobDir, "stdout"))
	_ = se.Flush(filepath.Join(jobDir, "stderr"))
	_ = writeJSON(filepath.Join(jobDir, "ledger.json"), map[string]any{"stdout_ledger": ledgerProto(so.Ledger()), "stderr_ledger": ledgerProto(se.Ledger())})
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		code = -1
	}

	// Under the job lock, and only if the record is still there. A job_rm running
	// right now sees a live supervisor and skips the job, but this write happens
	// after the child is reaped -- so without the lock it can land just after a
	// removal that saw the job as finished, leaving a directory holding only
	// status.json. job_list skips such a directory silently, so the job would
	// simply vanish rather than report its exit code.
	writeStatus := func() error {
		if !jobExists(jobDir) {
			return nil
		}
		return writeJSON(filepath.Join(jobDir, "status.json"), map[string]any{
			"exit_code":      code,
			"ended_at":       time.Now().UTC().Format(time.RFC3339),
			"resource_limit": resourceLimit,
			"stdout_ledger":  ledgerProto(so.Ledger()),
			"stderr_ledger":  ledgerProto(se.Ledger()),
		})
	}
	if err := withJobLock(jobDir, writeStatus); err != nil {
		// Locking failed, but an unrecorded exit code is a worse outcome than an
		// unsynchronized one: metaToInfo would have to fall back to a pid probe
		// and report "unknown" forever.
		writeStatus()
	}
	os.Exit(code)
}

func ledgerProto(l storage.Ledger) proto.LogLedger {
	return proto.LogLedger{OriginalBytes: l.OriginalBytes, RetainedBytes: l.RetainedBytes, DroppedBytes: l.DroppedBytes, Truncated: l.Truncated, FirstTruncatedAt: l.FirstTruncatedAt, LimitBytes: l.LimitBytes, Policy: l.Policy}
}
