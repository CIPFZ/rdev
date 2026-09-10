//go:build !windows

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	statepkg "github.com/CIPFZ/rdev/internal/state"
)

func rollbackStartedJob(cmd *exec.Cmd, dir string) error {
	if cmd == nil || cmd.Process == nil {
		return os.RemoveAll(dir)
	}
	pid := cmd.Process.Pid
	var killErr error
	if groupErr := syscall.Kill(-pid, syscall.SIGKILL); groupErr != nil && !errors.Is(groupErr, syscall.ESRCH) {
		// A group probe can fail while the supervisor itself is still
		// signalable. Treat the fallback as successful if it works; report a
		// rollback failure only when both addresses reject the escalation.
		if pidErr := syscall.Kill(pid, syscall.SIGKILL); pidErr != nil && !errors.Is(pidErr, syscall.ESRCH) {
			killErr = errors.Join(groupErr, pidErr)
		}
	}
	waitErr := cmd.Wait()
	// A killed supervisor reports *exec.ExitError; that is the expected result
	// of rollback, not evidence that rollback itself failed.
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		waitErr = nil
	}
	removeErr := os.RemoveAll(dir)
	return errors.Join(killErr, waitErr, removeErr)
}

func jobStop(p *proto.JobParams, state string) (*proto.JobResult, error) {
	lease, err := statepkg.AcquireWriter(state)
	if err != nil {
		return nil, stateWriteError(err)
	}
	defer lease.Close()
	if p.ID == "" {
		return nil, invalidRequestError("job id required")
	}
	if p.GraceSec < 0 || p.GraceSec > hardExecTimeoutSec {
		return nil, limitExceededError("grace_sec is outside the hard limit")
	}
	dir, err := validatedJobDir(state, p.ID)
	if err != nil {
		return nil, err
	}
	meta, err := readMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", p.ID, err)
	}

	sig := syscall.SIGTERM
	switch {
	case p.Signal == "", strings.EqualFold(p.Signal, "TERM"):
	case strings.EqualFold(p.Signal, "KILL"):
		sig = syscall.SIGKILL
	default:
		return nil, invalidRequestError("signal must be TERM or KILL")
	}
	// A PID can be recycled after a job exits. Never signal a new process that
	// happens to occupy the recorded PID; the immutable kernel start token is
	// checked immediately before every stop request.
	if meta.ProcessIdentity != "" {
		current, identityErr := processIdentity(meta.PID)
		if identityErr != nil || current != meta.ProcessIdentity {
			return nil, processStateError("job process identity no longer matches")
		}
	}
	childPID, childIdentity := readChildProcess(dir)
	if childPID > 0 && childIdentity != "" {
		current, identityErr := processIdentity(childPID)
		if identityErr != nil || current != childIdentity {
			childPID = 0
		}
	}

	// A legacy supervisor has no TERM handler and buffers logs until its child
	// exits. Terminate that child's isolated group and leave the supervisor
	// alive to drain output and persist status. Killing both groups here loses
	// all buffered output from jobs started before signal relay was introduced.
	// The grace deadline below still bounds a child that refuses TERM.
	if sig == syscall.SIGTERM && !meta.SignalRelay && childPID > 0 {
		if err := syscall.Kill(-childPID, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
			return nil, processStateError("job child process is unavailable")
		}
	} else {
		// Modern supervisors relay TERM and flush terminal state. KILL must
		// still address both groups, including an orphaned child.
		groupErr := syscall.Kill(-meta.PID, sig)
		childGroupErr := error(nil)
		if childPID > 0 && (sig == syscall.SIGKILL || !meta.SignalRelay || groupErr != nil) {
			childGroupErr = syscall.Kill(-childPID, sig)
		}
		if groupErr != nil {
			// The group is gone. Try the bare supervisor pid, then the recorded
			// child: a SIGKILLed supervisor leaves the child orphaned to init but
			// still running, and that child is exactly what a caller wants to stop.
			pidErr := syscall.Kill(meta.PID, sig)
			if pidErr != nil {
				// A successful child-group signal is sufficient even when the
				// supervisor has already exited; probing the leader again would turn
				// that successful stop into a spurious "unavailable" error.
				if childGroupErr != nil && (childPID <= 0 || syscall.Kill(childPID, sig) != nil) {
					return nil, processStateError("job process is unavailable")
				}
				// Also sweep the orphan's own group, in case it spawned children.
				if childPID > 0 {
					syscall.Kill(-childPID, sig)
				}
			}
		}
		if groupErr != nil && childGroupErr != nil && childPID <= 0 {
			return nil, processStateError("job process is unavailable")
		}
	}

	if sig == syscall.SIGTERM && p.GraceSec > 0 {
		deadline := time.Now().Add(time.Duration(p.GraceSec) * time.Second)
		for time.Now().Before(deadline) {
			if !jobAlive(meta, dir) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if jobAlive(meta, dir) {
			syscall.Kill(-meta.PID, syscall.SIGKILL)
			if child := childPID; child > 0 {
				syscall.Kill(-child, syscall.SIGKILL)
				syscall.Kill(child, syscall.SIGKILL)
			}
		}
	}

	// Record the kill so status reports JobKilled rather than a bare exit.
	//
	// Under the lock, and re-checking the record: a concurrent job_rm may have
	// removed this job while the grace period elapsed above, and recreating
	// status.json in a deleted directory would resurrect a half-job that
	// job_list then reports with no meta.json. Signalling itself stays outside
	// the lock -- it addresses a pgid, not a file, and holding a lock across a
	// multi-second grace wait would block an unrelated job_rm for no reason.
	if !jobAlive(meta, dir) {
		withJobLock(dir, func() error {
			if !jobExists(dir) {
				return nil
			}
			return writeJSON(filepath.Join(dir, "status.json"), map[string]any{
				"exit_code": -1,
				"ended_at":  time.Now().UTC().Format(time.RFC3339),
				"killed":    true,
			})
		})
	}
	return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
}
