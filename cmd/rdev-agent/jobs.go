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
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	proto.OpJobRm:     true,
}

// isJobOp reports whether op is dispatched by doJob.
func isJobOp(op string) bool { return jobOps[op] }

func doJob(op string, p *proto.JobParams, state string) (*proto.JobResult, error) {
	switch op {
	case proto.OpJobStart:
		return jobStart(p, state)
	case proto.OpJobList:
		return jobList(p, state)
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
	case proto.OpJobRm:
		return jobRm(p, state)
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

// readTail returns the last n lines of a file.
//
// Reads backward in chunks from the end rather than loading the whole file:
// tail_on_exit is commonly used on batch logs that can reach hundreds of
// megabytes, and os.ReadFile on one of those would allocate the entire thing to
// return a handful of lines.
func readTail(path string, n int) (string, error) {
	if n < 1 {
		n = 1
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}

	size := info.Size()
	// Cap the scan: a file whose last n lines are enormous should still not pull
	// an unbounded amount into memory.
	const chunk = 64 << 10
	maxScan := int64(chunk) * 16

	var tail []byte
	var pos = size
	for pos > 0 && int64(len(tail)) < maxScan {
		step := int64(chunk)
		if pos < step {
			step = pos
		}
		pos -= step

		buf := make([]byte, step)
		if _, err := f.ReadAt(buf, pos); err != nil && err != io.EOF {
			return "", err
		}
		tail = append(buf, tail...)

		// Stop once the window holds enough newlines for n lines. One extra
		// accounts for a partial line at the front of the window.
		if bytes.Count(tail, []byte("\n")) > n {
			break
		}
	}

	lines := strings.Split(strings.TrimRight(string(tail), "\n"), "\n")
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

// jobList reports jobs, newest first.
//
// Limit is applied before any metadata is read. Job IDs are timestamp-prefixed,
// so sorting directory names already puts them in chronological order, and
// listing the newest 20 on a host with 5000 jobs costs 20 file reads instead of
// 5000 -- each of which is a stat plus a JSON parse plus liveness probes.
func jobList(p *proto.JobParams, state string) (*proto.JobResult, error) {
	root := filepath.Join(state, "jobs")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return &proto.JobResult{List: nil}, nil
		}
		return nil, err
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	// Descending, so the newest directories come first.
	sort.Sort(sort.Reverse(sort.StringSlice(names)))

	limit := p.Limit
	if limit <= 0 {
		limit = defaultJobListLimit
	}

	list := make([]*proto.JobInfo, 0, min(limit, len(names)))
	for _, name := range names {
		if len(list) >= limit {
			break
		}
		dir := filepath.Join(root, name)
		meta, err := readMeta(dir)
		if err != nil {
			continue // skip half-written or foreign directories
		}
		list = append(list, metaToInfo(meta, dir))
	}

	// StartedAt is authoritative for ordering: a hand-created or clock-skewed
	// directory name could otherwise misplace an entry. Cheap, since this only
	// sorts what is being returned.
	sort.Slice(list, func(i, j int) bool { return list[i].StartedAt > list[j].StartedAt })

	res := &proto.JobResult{List: list}
	res.Total = len(names)
	res.Truncated = len(names) > len(list)
	return res, nil
}

// defaultJobListLimit bounds an unspecified listing. High enough to cover normal
// use, low enough that a host with thousands of old jobs stays responsive.
const defaultJobListLimit = 100

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

	// Clamp the offset to the file: a caller polling incrementally can hold a
	// next_offset from before the log was rotated or truncated, and the
	// resulting negative length would panic in make. Treat a stale offset as
	// "nothing new to read" rather than an error, since the caller's next poll
	// with the returned offset then recovers on its own.
	since := p.SinceOffset
	if since < 0 {
		since = 0
	}
	if since > info.Size() {
		since = info.Size()
	}
	if since > 0 {
		if _, err := f.Seek(since, 0); err != nil {
			return nil, err
		}
	}

	res := &proto.JobResult{LogSize: info.Size()}

	tail := p.TailLines
	if tail <= 0 {
		tail = defaultLogTail
	}

	// Fast path: plain "tail the last N lines". Seek backward from the end instead
	// of walking the file, so cost depends on the output size rather than the log
	// size. This is the common shape -- checking on a running batch -- and on a
	// 50 MB log it is ~40x faster than scanning.
	if p.Grep == "" && since == 0 {
		logs, err := readTail(path, tail)
		if err != nil {
			return nil, err
		}
		res.Logs = logs
		res.NextOffset = info.Size()
		return res, nil
	}

	// Otherwise stream the region, keeping only the lines that will be returned.
	//
	// Reading it whole would allocate the entire span: measured at 412 MB to
	// return 1900 bytes from a 190 MB log, which is enough to OOM a shared dev box
	// during a long batch. Grep and tail both reduce, so neither needs the full
	// text in memory at once.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64<<10), maxLogLineLen)

	ring := newLineRing(tail)
	grep := []byte(p.Grep)
	var consumed int64
	matched := 0
	for scanner.Scan() {
		// Bytes() reuses the scanner's buffer, so a filtered-out line costs no
		// allocation at all. Converting every line to a string instead cost ~200 MB
		// of garbage on a 190 MB log.
		line := scanner.Bytes()
		// +1 for the newline the scanner stripped. The last line may not have one,
		// so the offset is clamped to the file size below.
		consumed += int64(len(line)) + 1
		if len(grep) > 0 {
			if !bytes.Contains(line, grep) {
				continue
			}
			matched++
		}
		ring.add(line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s log: %w", stream, err)
	}

	// A line longer than the scanner buffer, or a final line without a newline,
	// can leave the count slightly over; never report an offset past the end.
	next := since + consumed
	if next > info.Size() {
		next = info.Size()
	}
	res.NextOffset = next
	if p.Grep != "" {
		res.Matched = matched
	}
	res.Logs = strings.Join(ring.lines(), "\n")
	return res, nil
}

// maxLogLineLen bounds a single log line. A process emitting one enormous line
// (a minified bundle, a base64 blob) should not be able to make the agent
// allocate without limit.
const maxLogLineLen = 1 << 20

// lineRing keeps the last n lines seen, discarding earlier ones.
//
// This is what makes tailing independent of log size: memory is bounded by the
// number of lines actually returned rather than by the file.
type lineRing struct {
	buf   []string
	next  int
	full  bool
	limit int
}

func newLineRing(limit int) *lineRing {
	if limit < 1 {
		limit = 1
	}
	return &lineRing{buf: make([]string, 0, limit), limit: limit}
}

// add copies line into the ring. The copy is required: callers pass the
// scanner's reusable buffer, which the next Scan overwrites.
func (r *lineRing) add(line []byte) {
	s := string(line)
	if len(r.buf) < r.limit {
		r.buf = append(r.buf, s)
		return
	}
	r.buf[r.next] = s
	r.next = (r.next + 1) % r.limit
	r.full = true
}

// lines returns the retained lines in arrival order.
func (r *lineRing) lines() []string {
	if !r.full {
		return r.buf
	}
	out := make([]string, 0, len(r.buf))
	out = append(out, r.buf[r.next:]...)
	return append(out, r.buf[:r.next]...)
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

// jobRm deletes job records, either one by ID or a filtered sweep.
//
// A job's stdout and stderr are unbounded files, so a machine running batches
// accumulates them until the disk fills; job_list also slows down because it
// reads every directory. This is the reclaim path.
//
// A running job is never removed, whichever mode is used: deleting its records
// would leave the process alive with no way to observe or stop it, which is worse
// than the disk usage. Such jobs come back in Skipped so the caller knows why
// nothing happened.
func jobRm(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.ID != "" {
		return jobRmOne(p.ID, state)
	}
	if p.OlderThanSec <= 0 && p.KeepLast <= 0 {
		return nil, errors.New("job_rm needs an id, older_than_sec, or keep_last")
	}
	return jobRmSweep(p, state)
}

func jobRmOne(id, state string) (*proto.JobResult, error) {
	dir := jobDir(state, id)
	meta, err := readMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", id, err)
	}
	info := metaToInfo(meta, dir)
	if info.State == proto.JobRunning {
		return &proto.JobResult{Skipped: []string{id}, Info: info}, nil
	}

	size := dirSize(dir)
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("remove job %s: %w", id, err)
	}
	return &proto.JobResult{Removed: []string{id}, FreedBytes: size}, nil
}

func jobRmSweep(p *proto.JobParams, state string) (*proto.JobResult, error) {
	root := filepath.Join(state, "jobs")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return &proto.JobResult{}, nil
		}
		return nil, err
	}

	type candidate struct {
		id   string
		dir  string
		info *proto.JobInfo
	}
	var finished []candidate
	res := &proto.JobResult{}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		meta, err := readMeta(dir)
		if err != nil {
			continue // half-written or foreign directory
		}
		info := metaToInfo(meta, dir)
		if info.State == proto.JobRunning {
			res.Skipped = append(res.Skipped, info.ID)
			continue
		}
		finished = append(finished, candidate{id: info.ID, dir: dir, info: info})
	}

	// Newest first, so KeepLast retains the most recent jobs.
	sort.Slice(finished, func(i, j int) bool {
		return finished[i].info.StartedAt > finished[j].info.StartedAt
	})

	now := time.Now()
	for i, c := range finished {
		// Both filters must agree: with keep_last=5 and older_than_sec=3600, a
		// recent job inside the keep window stays even if it is old, and a job
		// beyond the window stays if it has not aged out yet. Requiring both makes
		// the combination conservative rather than surprising.
		if p.KeepLast > 0 && i < p.KeepLast {
			continue
		}
		if p.OlderThanSec > 0 && !endedBefore(c.info, now, time.Duration(p.OlderThanSec)*time.Second) {
			continue
		}
		size := dirSize(c.dir)
		if err := os.RemoveAll(c.dir); err != nil {
			continue // a failed removal is not worth failing the whole sweep
		}
		res.Removed = append(res.Removed, c.id)
		res.FreedBytes += size
	}
	return res, nil
}

// endedBefore reports whether a finished job aged past d.
//
// EndedAt is missing for a job whose supervisor died without recording a status,
// so StartedAt is the fallback: it is always present and, for a job that is no
// longer running, is a safe lower bound on when the work stopped.
func endedBefore(info *proto.JobInfo, now time.Time, d time.Duration) bool {
	stamp := info.EndedAt
	if stamp == "" {
		stamp = info.StartedAt
	}
	t, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		return false // an unparseable timestamp should not cause deletion
	}
	return now.Sub(t) > d
}

// dirSize sums the job directory's files so the caller learns what was freed.
func dirSize(dir string) int64 {
	var total int64
	filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
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
