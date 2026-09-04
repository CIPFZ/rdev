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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/storage"
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

// supervisorParentEnv carries the serving agent's PID across the short
// startup barrier. Reading os.Getppid only after a very fast parent crash can
// yield init's PID (1), making the supervisor believe it has a live parent and
// wait forever for metadata that can never be published.
const supervisorParentEnv = "RDEV_SUPERVISOR_PARENT_PID"

func setEnvValue(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return append(out, prefix+value)
}

func withoutEnvValue(env []string, key string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env))
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return out
}

func jobStart(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.Spec == nil || len(p.Spec.Argv) == 0 {
		return nil, invalidRequestError("job spec with argv required")
	}

	id := newJobID()
	if _, err := secureJobRoot(state); err != nil {
		return nil, err
	}
	dir, err := validatedJobDir(state, id)
	if err != nil {
		return nil, err
	}
	if err := secureJobDir(dir); err != nil {
		return nil, err
	}
	policy, err := storage.Load(filepath.Join(state, "storage-policy.json"))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if err := storage.Save(filepath.Join(dir, "storage-policy.json"), policy); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}

	cmd, err := buildCmd(p.Spec)
	if err != nil {
		return nil, err
	}

	// Re-target the command at this same binary in supervisor mode, so the
	// direct parent of the real process is something that outlives ssh and can
	// record the exit code. buildCmd already validated cwd/env/argv and
	// resolved the login-shell wrapper, so only Path and Args change here.
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate agent binary: %w", err)
	}
	inner := cmd.Args // includes the login-shell wrapper when requested
	cmd.Path = self
	cmd.Args = append([]string{self, superviseFlag, dir, "--"}, inner...)
	cmd.Env = setEnvValue(cmd.Env, supervisorParentEnv, strconv.Itoa(os.Getpid()))

	stdout, err := os.OpenFile(filepath.Join(dir, "stdout"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	_ = stdout.Chmod(0o600)
	stderr, err := os.OpenFile(filepath.Join(dir, "stderr"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	defer stderr.Close()
	_ = stderr.Chmod(0o600)

	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = nil // a detached job has no console to read from

	// Setsid detaches the child into a new session and process group. Without
	// it the child would share the agent's group and die when ssh hangs up.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return nil, processStartError(err)
	}
	identity, err := processIdentity(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, processStartError(fmt.Errorf("record process identity: %w", err))
	}

	// Preserve sub-second start order. keep_last cannot infer which job is newer
	// when several starts are truncated to the same whole second.
	startedAt := time.Now().UTC().Format(time.RFC3339Nano)
	meta := &jobMeta{
		ID:              id,
		Label:           p.Label,
		Argv:            p.Spec.Argv,
		Cwd:             p.Spec.Cwd,
		PID:             cmd.Process.Pid,
		ProcessIdentity: identity,
		StartedAt:       startedAt,
		StoragePolicy:   policy.PerJob,
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return nil, err
	}

	// Reap in the background as a best-effort fast path: when the agent does
	// stay alive, status.json lands immediately. The supervisor writes the same
	// file independently, so correctness never depends on this goroutine.
	go func() {
		cmd.Wait()
	}()

	return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
}

func jobStop(p *proto.JobParams, state string) (*proto.JobResult, error) {
	dir, err := validatedJobDir(state, p.ID)
	if err != nil {
		return nil, err
	}
	if p.GraceSec < 0 || p.GraceSec > hardExecTimeoutSec {
		return nil, limitExceededError("grace_sec is outside the hard limit")
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

	// Verify identities immediately before signalling. A recycled PID must
	// never receive a signal intended for an older job.
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
			child := childPID
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
			if child := childPID; child > 0 {
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
