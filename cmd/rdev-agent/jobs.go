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
	}
	return nil, fmt.Errorf("unknown job op %q", op)
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

// metaToInfo merges the immutable job record with its current state. State is
// derived from status.json when the reaper recorded an exit, and otherwise from
// a signal-0 liveness probe, which is what makes jobs observable across agent
// restarts.
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
	} else {
		// Alive-check failed and no status file: the agent that owned this job
		// died before reaping. The process is gone but the exit code is lost.
		info.State = proto.JobUnknown
	}
	return info
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
	// Setsid, the pid is also the pgid, so this reaches every descendant --
	// the reason a recorded pgid beats grepping for a command string.
	if err := syscall.Kill(-meta.PID, sig); err != nil {
		// Fall back to the bare pid in case the group is already gone.
		if err2 := syscall.Kill(meta.PID, sig); err2 != nil {
			return nil, fmt.Errorf("signal job %s (pid %d): %w", p.ID, meta.PID, err)
		}
	}

	if sig == syscall.SIGTERM && p.GraceSec > 0 {
		deadline := time.Now().Add(time.Duration(p.GraceSec) * time.Second)
		for time.Now().Before(deadline) {
			if !processAlive(meta.PID) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if processAlive(meta.PID) {
			syscall.Kill(-meta.PID, syscall.SIGKILL)
		}
	}

	// Record the kill so status reports JobKilled rather than a bare exit.
	if !processAlive(meta.PID) {
		writeJSON(filepath.Join(dir, "status.json"), map[string]any{
			"exit_code": -1,
			"ended_at":  time.Now().UTC().Format(time.RFC3339),
			"killed":    true,
		})
	}
	return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
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
