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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

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
