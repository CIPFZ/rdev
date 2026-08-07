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
	"errors"
	"fmt"
	"os"
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
		return nil, errors.New("job spec with argv required")
	}

	id := newJobID()
	dir := jobDir(state, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
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

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		return nil, err
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		return nil, err
	}
	defer stderr.Close()

	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = nil // a detached job has no console to read from

	// Setsid detaches the child into a new session and process group. Without
	// it the child would share the agent's group and die when ssh hangs up.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start job: %w", err)
	}

	meta := &jobMeta{
		ID:        id,
		Label:     p.Label,
		Argv:      p.Spec.Argv,
		Cwd:       p.Spec.Cwd,
		PID:       cmd.Process.Pid,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
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
	if p.ID == "" {
		return nil, errors.New("job id required")
	}
	dir := jobDir(state, p.ID)
	meta, err := readMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", p.ID, err)
	}

	sig := syscall.SIGTERM
	if strings.EqualFold(p.Signal, "KILL") {
		sig = syscall.SIGKILL
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
				return nil, fmt.Errorf("signal job %s (pid %d): %w", p.ID, meta.PID, groupErr)
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
	if !jobAlive(meta, dir) {
		writeJSON(filepath.Join(dir, "status.json"), map[string]any{
			"exit_code": -1,
			"ended_at":  time.Now().UTC().Format(time.RFC3339),
			"killed":    true,
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
	if len(p.IDs) > 0 {
		return jobWaitMany(p, state)
	}
	if p.ID == "" {
		return nil, errors.New("job id required")
	}
	dir := jobDir(state, p.ID)
	meta, err := readMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", p.ID, err)
	}

	budget := p.WaitTimeoutSec
	if budget <= 0 {
		budget = defaultWaitSec
	}
	if budget > maxWaitSec {
		budget = maxWaitSec
	}

	start := time.Now()
	deadline := start.Add(time.Duration(budget) * time.Second)
	interval := waitPollMin

	for {
		info := metaToInfo(meta, dir)
		if info.State != proto.JobRunning {
			return finishWait(p, dir, info, start, false), nil
		}
		if !time.Now().Before(deadline) {
			return finishWait(p, dir, info, start, true), nil
		}

		// Do not overshoot the deadline: sleeping past it would report a longer
		// wait than the caller asked for.
		sleep := interval
		if remaining := time.Until(deadline); remaining < sleep {
			sleep = remaining
		}
		time.Sleep(sleep)

		if interval < waitPollMax {
			interval *= 2
			if interval > waitPollMax {
				interval = waitPollMax
			}
		}
	}
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
	budget := p.WaitTimeoutSec
	if budget <= 0 {
		budget = defaultWaitSec
	}
	if budget > maxWaitSec {
		budget = maxWaitSec
	}

	start := time.Now()
	deadline := start.Add(time.Duration(budget) * time.Second)

	// Deduplicate: a caller assembling ids from several places can repeat one, and
	// polling it twice per round is pure waste.
	type target struct {
		id   string
		dir  string
		meta *jobMeta
		done *proto.JobInfo
		err  string
	}
	seen := make(map[string]bool, len(p.IDs))
	targets := make([]*target, 0, len(p.IDs))
	for _, id := range p.IDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		t := &target{id: id, dir: jobDir(state, id)}
		meta, err := readMeta(t.dir)
		if err != nil {
			t.err = fmt.Sprintf("job %s: %v", id, err)
		} else {
			t.meta = meta
		}
		targets = append(targets, t)
	}
	if len(targets) == 0 {
		return nil, errors.New("job_wait: no usable ids")
	}

	interval := waitPollMin
	timedOut := false
	for {
		pending := 0
		anyDone := false
		for _, t := range targets {
			if t.done != nil || t.err != "" {
				if t.done != nil {
					anyDone = true
				}
				continue
			}
			info := metaToInfo(t.meta, t.dir)
			if info.State != proto.JobRunning {
				t.done = info
				anyDone = true
				continue
			}
			pending++
		}

		// WaitAny lets a caller react to the first finisher -- usually the first
		// failure in a batch -- without waiting out the slowest job.
		if pending == 0 || (p.WaitAny && anyDone) {
			break
		}
		if !time.Now().Before(deadline) {
			timedOut = true
			break
		}

		sleep := interval
		if remaining := time.Until(deadline); remaining < sleep {
			sleep = remaining
		}
		time.Sleep(sleep)
		if interval < waitPollMax {
			interval *= 2
			if interval > waitPollMax {
				interval = waitPollMax
			}
		}
	}

	res := &proto.JobResult{
		TimedOut: timedOut,
		WaitedMS: time.Since(start).Milliseconds(),
	}
	for _, t := range targets {
		w := &proto.WaitedJob{ID: t.id, Err: t.err}
		switch {
		case t.err != "":
		case t.done != nil:
			w.Info = t.done
		default:
			// Still running when the budget expired, or when WaitAny returned early.
			w.Info = metaToInfo(t.meta, t.dir)
		}
		if p.TailOnExit > 0 && w.Info != nil {
			if logs, err := readTail(filepath.Join(t.dir, "stdout"), p.TailOnExit); err == nil {
				w.Logs = logs
			}
		}
		res.Waited = append(res.Waited, w)
	}
	return res, nil
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
