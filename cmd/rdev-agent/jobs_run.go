// Starting, stopping, and waiting for jobs.
//
// These are the operations that touch live processes. Two properties matter and
// are easy to break:
//
//   - A job outlives the agent. jobStart detaches with setsid and re-execs this
//     binary as a supervisor, so an exit code survives an ssh drop.
//   - A job is addressed by its recorded pgid, never by grepping ps output, which
//     is what makes jobStop reach the whole tree reliably.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// Wait polling bounds. The interval backs off so a job that runs for an hour
// costs a handful of stat calls per minute instead of ten per second, while a
// short job is still noticed almost immediately.
const (
	waitPollMin = 200 * time.Millisecond
	waitPollMax = 3 * time.Second
	// maxWaitSec caps a single wait. Beyond this the agent returns TimedOut so
	// the request cannot be stranded by a job that never finishes.
	maxWaitSec     = 3600
	defaultWaitSec = 300
)

func jobStart(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.Spec == nil || len(p.Spec.Argv) == 0 {
		return nil, invalidRequestError("job spec with argv required")
	}

	// The jobs directory is shared by multiple agent processes. The directory
	// creation itself is the uniqueness commit point; Mkdir (rather than
	// MkdirAll) lets a deliberately repeated ID fail closed instead of opening
	// an existing record and mixing its logs with a new process.
	if err := os.MkdirAll(filepath.Join(state, "jobs"), 0o755); err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 32; attempt++ {
		id := jobIDGenerator()
		dir := jobDir(state, id)
		var result *proto.JobResult
		err := withJobLock(dir, func() error {
			if err := os.Mkdir(dir, 0o755); err != nil {
				if errors.Is(err, os.ErrExist) {
					return errJobIDCollision
				}
				return err
			}
			var err error
			result, err = startJobTransaction(p, id, dir)
			return err
		})
		if errors.Is(err, errJobIDCollision) {
			continue
		}
		if err != nil {
			removeJobLock(dir)
			return nil, err
		}
		return result, nil
	}
	return nil, processStateError("could not allocate a unique job id")
}

var errJobIDCollision = errors.New("job id already exists")

// writeJobMeta is replaceable only by package tests. Keeping the fault seam at
// the publication boundary lets tests prove that a failed metadata rename
// rolls back the already-started supervisor, without weakening normal file
// permissions or relying on a root/non-root distinction.
var writeJobMeta = func(path string, v any) error { return writeJSON(path, v) }

// startJobTransaction is called while holding the job lock. No externally
// visible job record is committed until meta.json is atomically published. If
// any post-Start step fails, the whole process group is killed and reaped before
// the directory is removed, so metadata failures cannot strand a runnable job.
func startJobTransaction(p *proto.JobParams, id, dir string) (*proto.JobResult, error) {
	cmd, err := buildCmd(p.Spec)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	self, err := os.Executable()
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("locate agent binary: %w", err)
	}
	inner := append([]string(nil), cmd.Args...)
	cmd.Path = self
	cmd.Args = append([]string{self, superviseFlag, dir, "--"}, inner...)

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		stdout.Close()
		os.RemoveAll(dir)
		return nil, err
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		stdout.Close()
		stderr.Close()
		os.RemoveAll(dir)
		return nil, processStartError(err)
	}

	meta := &jobMeta{
		ID:        id,
		Label:     p.Label,
		Argv:      p.Spec.Argv,
		Cwd:       p.Spec.Cwd,
		PID:       cmd.Process.Pid,
		StartedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := writeJobMeta(filepath.Join(dir, "meta.json"), meta); err != nil {
		rbErr := rollbackStartedJob(cmd, dir)
		stdout.Close()
		stderr.Close()
		return nil, errors.Join(err, rbErr)
	}

	stdout.Close()
	stderr.Close()
	// The serving agent reaps the supervisor when it remains alive. The
	// supervisor independently writes status.json, so this is only a fast path.
	go func() { _ = cmd.Wait() }()
	return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
}

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
	if p.ID == "" {
		return nil, invalidRequestError("job id required")
	}
	if p.GraceSec < 0 || p.GraceSec > hardExecTimeoutSec {
		return nil, limitExceededError("grace_sec is outside the hard limit")
	}
	dir := jobDir(state, p.ID)
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

	// Signal the whole process group (negative pid). Because jobStart used
	// Setsid, the supervisor pid is also the pgid, so this reaches every
	// descendant -- the reason a recorded pgid beats grepping for a command
	// string.
	groupErr := syscall.Kill(-meta.PID, sig)
	if groupErr != nil {
		// The group is gone. Try the bare supervisor pid, then the recorded
		// child: a SIGKILLed supervisor leaves the child orphaned to init but
		// still running, and that child is exactly what a caller wants to stop.
		pidErr := syscall.Kill(meta.PID, sig)
		if pidErr != nil {
			child := readChildPID(dir)
			if child <= 0 || syscall.Kill(child, sig) != nil {
				return nil, processStateError("job process is unavailable")
			}
			// Also sweep the orphan's own group, in case it spawned children.
			syscall.Kill(-child, sig)
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
			if child := readChildPID(dir); child > 0 {
				syscall.Kill(child, syscall.SIGKILL)
				syscall.Kill(-child, syscall.SIGKILL)
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

// jobWait blocks until the job leaves the running state, or the wait budget
// expires.
//
// This replaces caller-side polling: one call covers a long batch instead of a
// status check every few seconds. The job is never affected by the wait, so a
// TimedOut reply just means "ask again".
func jobWait(p *proto.JobParams, state string) (*proto.JobResult, error) {
	return jobWaitContext(context.Background(), defaultWaitHub, p, state)
}

// jobWaitMany waits on several jobs in one call.
//
// Waiting on N parallel jobs used to mean N serial round trips, each re-sending
// the same context and each blocking its own budget. One call now covers the
// batch: the shared deadline is what makes it cheaper, not just tidier.
//
// A job that cannot be read (unknown id) is reported per-job rather than failing
// the call, since the other jobs still have useful answers.
func jobWaitMany(p *proto.JobParams, state string) (*proto.JobResult, error) {
	return jobWaitContext(context.Background(), defaultWaitHub, p, state)
}

// finishWait assembles the wait reply, attaching trailing output when asked.
func finishWait(p *proto.JobParams, dir string, info *proto.JobInfo, start time.Time, timedOut bool) *proto.JobResult {
	res := &proto.JobResult{
		Info:     info,
		TimedOut: timedOut,
		WaitedMS: time.Since(start).Milliseconds(),
	}
	if p.TailOnExit > 0 {
		// Best effort: a missing log file should not fail the wait, since the
		// status is the answer the caller actually needs.
		if logs, err := readTail(filepath.Join(dir, "stdout"), p.TailOnExit); err == nil {
			res.Logs = logs
		}
	}
	return res
}
