// Job records and status for rdev-agent.
//
// A job is a process the agent starts and then forgets: it is detached with
// setsid, its output goes to files, and its metadata is written to disk before
// the agent replies. Nothing about a running job depends on the agent, the ssh
// connection, or the host process staying alive.
//
// This file owns the on-disk record and the op dispatcher. Starting and stopping
// live in jobs_run.go, output in jobs_logs.go, and reclamation in
// jobs_reclaim.go.
package main

import (
	"container/heap"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// isJobOp reports whether op is dispatched by doJob.
func isJobOp(op string) bool {
	descriptor, ok := proto.LookupOperation(op)
	return ok && descriptor.UsesJobParams
}

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
	return nil, invalidRequestError("unknown job operation")
}

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

func jobDir(state, id string) string { return filepath.Join(state, "jobs", id) }

func jobStatus(id, state string) (*proto.JobInfo, error) {
	if id == "" {
		return nil, invalidRequestError("job id required")
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
		return nil, processStateError("invalid job metadata")
	}
	return &m, nil
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	// Write-then-rename so a reader never observes a partial record.
	//
	// The temp name carries the pid: a fixed "<path>.tmp" is shared state between
	// every writer of that path, and two of them interleaving would let one
	// rename the other's half-written bytes into place. Writers of one job's
	// status now hold the job lock, but the supervisor writes child.json outside
	// it, and a unique name costs nothing.
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp) // do not leave the temp file behind on a failed rename
		return err
	}
	return nil
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

// jobIDGenerator is a narrow test seam for exercising duplicate-ID handling.
// Production always uses newJobID, whose random suffix makes collisions
// vanishingly unlikely; the retry in jobStart is still required because the
// directory is the durable uniqueness boundary.
var jobIDGenerator = newJobID

// jobList reports jobs, newest first.
//
// StartedAt, not the directory name, defines recency, so every readable metadata
// record must participate before Limit is applied. The bounded heap keeps memory
// proportional to Limit, and liveness probes still run only for the selected
// records; the full scan pays one metadata read per directory without retaining
// every record in memory.
func jobList(p *proto.JobParams, state string) (*proto.JobResult, error) {
	limit := p.Limit
	if limit < 0 || limit > 1000 {
		return nil, limitExceededError("job list limit is outside the hard limit")
	}
	if limit == 0 {
		limit = defaultJobListLimit
	}

	root := filepath.Join(state, "jobs")
	dir, err := os.Open(root)
	if err != nil {
		if os.IsNotExist(err) {
			return &proto.JobResult{List: nil}, nil
		}
		return nil, err
	}
	defer dir.Close()
	selected := &oldestJobHeap{}
	heap.Init(selected)
	total := 0
	for {
		entries, readErr := dir.ReadDir(256)
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			total++
			jobDir := filepath.Join(root, entry.Name())
			meta, err := readMeta(jobDir)
			if err != nil {
				continue // skip half-written or foreign directories
			}
			candidate := jobListCandidate{dir: jobDir, meta: meta}
			if selected.Len() < limit {
				heap.Push(selected, candidate)
			} else if jobMetaNewer(candidate.meta, (*selected)[0].meta) {
				(*selected)[0] = candidate
				heap.Fix(selected, 0)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	candidates := append([]jobListCandidate(nil), (*selected)...)
	sort.Slice(candidates, func(i, j int) bool {
		return jobMetaNewer(candidates[i].meta, candidates[j].meta)
	})

	list := make([]*proto.JobInfo, 0, len(candidates))
	for _, candidate := range candidates {
		list = append(list, metaToInfo(candidate.meta, candidate.dir))
	}

	res := &proto.JobResult{List: list}
	res.Total = total
	// Total has always counted directories, including an unreadable record, and
	// Truncated has always meant that the directory count exceeded Limit. Keep
	// those observability semantics while choosing valid records by metadata.
	res.Truncated = total > limit
	return res, nil
}

type jobListCandidate struct {
	dir  string
	meta *jobMeta
}

// oldestJobHeap keeps the least-recent selected record at its root, so a newer
// record found later in the directory scan can replace it in O(log Limit).
type oldestJobHeap []jobListCandidate

func (h oldestJobHeap) Len() int      { return len(h) }
func (h oldestJobHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h oldestJobHeap) Less(i, j int) bool {
	return jobMetaNewer(h[j].meta, h[i].meta)
}
func (h *oldestJobHeap) Push(value any) { *h = append(*h, value.(jobListCandidate)) }
func (h *oldestJobHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

// defaultJobListLimit bounds an unspecified listing. High enough to cover normal
// use, low enough that a host with thousands of old jobs stays responsive.
const defaultJobListLimit = 100

// jobInfoNewer defines the total recency order used by listing and reclamation.
//
// New records use RFC3339Nano, but older records only have whole-second
// timestamps. Parsing rather than comparing strings keeps the two formats in
// chronological order (a fractional timestamp sorts before "Z" as raw text).
// Equal timestamps fall back to ID so every agent protects the same keep_last
// set. An invalid timestamp sorts first, conservatively keeping an uncertain
// record ahead of a valid deletion candidate; two invalid values still get a
// deterministic textual order.
func jobInfoNewer(a, b *proto.JobInfo) bool {
	return jobRecencyNewer(a.StartedAt, a.ID, b.StartedAt, b.ID)
}

func jobMetaNewer(a, b *jobMeta) bool {
	return jobRecencyNewer(a.StartedAt, a.ID, b.StartedAt, b.ID)
}

func jobRecencyNewer(aStartedAt, aID, bStartedAt, bID string) bool {
	at, aErr := time.Parse(time.RFC3339Nano, aStartedAt)
	bt, bErr := time.Parse(time.RFC3339Nano, bStartedAt)
	switch {
	case aErr == nil && bErr == nil && !at.Equal(bt):
		return at.After(bt)
	case aErr != nil && bErr == nil:
		return true
	case aErr == nil && bErr != nil:
		return false
	case aErr != nil && bErr != nil && aStartedAt != bStartedAt:
		return aStartedAt > bStartedAt
	default:
		return aID > bID
	}
}
