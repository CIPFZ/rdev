// Job handling for rdev-agent.
//
// A job is a process the agent starts and then forgets: it is detached with
// setsid, its output goes to files, and its metadata is written to disk before
// the agent replies. Nothing about a running job depends on the agent, the ssh
// connection, or the host process staying alive.
//
// This is the piece that fixes the "long batch died when the tool call timed
// out" problem, and the "pkill pattern did not match the real process" problem
// (jobs are addressed by recorded pgid, never by grepping ps output).
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/tonynyyan/rdev/internal/proto"
)

const defaultLogTail = 200

// jobMeta is the on-disk record. It is the single source of truth: a fresh
// agent process reconstructs everything by reading these files.
type jobMeta struct {
	ID        string   `json:"id"`
	Label     string   `json:"label,omitempty"`
	Argv      []string `json:"argv"`
	Cwd       string   `json:"cwd,omitempty"`
	PID       int      `json:"pid"`
	StartedAt string   `json:"started_at"`
}

// jobOps is the set of ops doJob handles. main's dispatcher consults this rather
// than repeating the list, so a new job op cannot be added to one place and
// forgotten in the other.
var jobOps = map[string]bool{
	proto.OpJobStart:  true,
	proto.OpJobList:   true,
	proto.OpJobStatus: true,
	proto.OpJobLogs:   true,
	proto.OpJobStop:   true,
	proto.OpJobWait:   true,
}

// isJobOp reports whether op is dispatched by doJob.
func isJobOp(op string) bool { return jobOps[op] }

func doJob(op string, p *proto.JobParams, state string) (*proto.JobResult, error) {
	switch op {
	case proto.OpJobStart:
		return jobStart(p, state)
	case proto.OpJobList:
		return jobList(state)
	case proto.OpJobStatus:
		info, err := jobStatus(p.ID, state)
		if err != nil {
			return nil, err
		}
		return &proto.JobResult{Info: info}, nil
	case proto.OpJobLogs:
		return jobLogs(p, state)
	case proto.OpJobStop:
		return jobStop(p, state)
	case proto.OpJobWait:
		return jobWait(p, state)
	}
	return nil, fmt.Errorf("unknown job op %q", op)
}

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

// jobWait blocks until the job leaves the running state, or the wait budget
// expires.
//
// This replaces caller-side polling: one call covers a long batch instead of a
// status check every few seconds. The job is never affected by the wait, so a
// TimedOut reply just means "ask again".
func jobWait(p *proto.JobParams, state string) (*proto.JobResult, error) {
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

// readTail returns the last n lines of a file.
func readTail(path string, n int) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n"), nil
}

func jobDir(state, id string) string { return filepath.Join(state, "jobs", id) }

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

func jobList(state string) (*proto.JobResult, error) {
	root := filepath.Join(state, "jobs")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return &proto.JobResult{List: nil}, nil
		}
		return nil, err
	}

	var list []*proto.JobInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		meta, err := readMeta(dir)
		if err != nil {
			continue // skip half-written or foreign directories
		}
		list = append(list, metaToInfo(meta, dir))
	}
	// Newest first: the job you just started is the one you want to see.
	sort.Slice(list, func(i, j int) bool { return list[i].StartedAt > list[j].StartedAt })
	return &proto.JobResult{List: list}, nil
}

func jobStatus(id, state string) (*proto.JobInfo, error) {
	if id == "" {
		return nil, errors.New("job id required")
	}
	dir := jobDir(state, id)
	meta, err := readMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", id, err)
	}
	return metaToInfo(meta, dir), nil
}

// metaToInfo merges the immutable job record with its current state.
//
// State is resolved in three tiers, because each source can be missing:
//  1. status.json, written by the supervisor when it reaped the child. Exact.
//  2. a signal-0 probe on the supervisor pid. Covers a running job whose
//     supervisor has not exited yet.
//  3. a signal-0 probe on the recorded child pid. This is what catches a
//     SIGKILLed supervisor: the child is reparented to init and keeps working,
//     so reporting "unknown" would hide live work. The exit code is genuinely
//     lost in that case, but the job is still observable and stoppable.
func metaToInfo(m *jobMeta, dir string) *proto.JobInfo {
	info := &proto.JobInfo{
		ID:        m.ID,
		Label:     m.Label,
		Argv:      m.Argv,
		Cwd:       m.Cwd,
		PID:       m.PID,
		StartedAt: m.StartedAt,
	}

	var st struct {
		ExitCode *int   `json:"exit_code"`
		EndedAt  string `json:"ended_at"`
		Killed   bool   `json:"killed"`
	}
	if err := readJSON(filepath.Join(dir, "status.json"), &st); err == nil && st.ExitCode != nil {
		info.ExitCode = *st.ExitCode
		info.EndedAt = st.EndedAt
		info.State = proto.JobExited
		if st.Killed {
			info.State = proto.JobKilled
		}
		return info
	}

	if processAlive(m.PID) {
		info.State = proto.JobRunning
		return info
	}

	// The supervisor is gone without recording a status. If the child survived
	// it (orphaned to init), the job is still doing work and must not be
	// reported as finished.
	if child := readChildPID(dir); child > 0 && processAlive(child) {
		info.State = proto.JobRunning
		info.ChildPID = child
		info.Orphaned = true
		return info
	}

	// Nothing alive and no status file: the process is gone but its exit code
	// was never recorded.
	info.State = proto.JobUnknown
	return info
}

// readChildPID returns the supervised child's pid, or 0 when unrecorded.
func readChildPID(dir string) int {
	var c struct {
		ChildPID int `json:"child_pid"`
	}
	if err := readJSON(filepath.Join(dir, "child.json"), &c); err != nil {
		return 0
	}
	return c.ChildPID
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// Signal 0 performs permission and existence checks without delivering.
	return syscall.Kill(pid, 0) == nil
}

func jobLogs(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.ID == "" {
		return nil, errors.New("job id required")
	}
	stream := p.Stream
	if stream == "" {
		stream = "stdout"
	}
	if stream != "stdout" && stream != "stderr" {
		return nil, fmt.Errorf("stream must be stdout or stderr, got %q", stream)
	}

	path := filepath.Join(jobDir(state, p.ID), stream)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if p.SinceOffset > 0 {
		if _, err := f.Seek(p.SinceOffset, 0); err != nil {
			return nil, err
		}
	}

	// Read the region of interest, then filter here on the remote side. A
	// multi-megabyte log never crosses the wire just to be grepped locally.
	buf := make([]byte, info.Size()-p.SinceOffset)
	n, _ := readFull(f, buf)
	text := string(buf[:n])

	res := &proto.JobResult{LogSize: info.Size(), NextOffset: p.SinceOffset + int64(n)}

	lines := strings.Split(text, "\n")
	if p.Grep != "" {
		kept := make([]string, 0, len(lines))
		for _, l := range lines {
			if strings.Contains(l, p.Grep) {
				kept = append(kept, l)
			}
		}
		lines = kept
		res.Matched = len(kept)
	}

	tail := p.TailLines
	if tail <= 0 {
		tail = defaultLogTail
	}
	if len(lines) > tail {
		lines = lines[len(lines)-tail:]
	}
	res.Logs = strings.Join(lines, "\n")
	return res, nil
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

// jobAlive reports whether any part of the job is still running: the supervisor
// or, if it died, the orphaned child.
func jobAlive(m *jobMeta, dir string) bool {
	if processAlive(m.PID) {
		return true
	}
	child := readChildPID(dir)
	return child > 0 && processAlive(child)
}

func readMeta(dir string) (*jobMeta, error) {
	var m jobMeta
	if err := readJSON(filepath.Join(dir, "meta.json"), &m); err != nil {
		return nil, err
	}
	if m.ID == "" {
		return nil, errors.New("invalid job metadata")
	}
	return &m, nil
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	// Write-then-rename so a reader never observes a partial record.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// newJobID returns a sortable, collision-resistant id.
//
// The timestamp prefix keeps `ls` output chronological, which helps when
// debugging by hand. A random suffix rather than sub-second time is what makes
// it unique: two jobs started in the same millisecond would otherwise collide,
// and a collision means one job silently overwrites another's logs.
func newJobID() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fall back to nanoseconds; still better than a fixed suffix.
		return fmt.Sprintf("%s-%08x", time.Now().UTC().Format("20060102-150405"), time.Now().Nanosecond())
	}
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102-150405"), hex.EncodeToString(b[:]))
}
