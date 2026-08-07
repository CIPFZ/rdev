// Package proto defines the wire contract between the rdev host and the
// rdev-agent running on a remote machine.
//
// Every request carries an explicit argv slice. Nothing in this protocol is a
// shell string: the agent execs argv directly, so quoting, word splitting, and
// glob expansion never happen. That property is the whole reason this protocol
// exists, so keep it when adding ops.
package proto

// Version is the wire format this build speaks. It is bumped whenever the format
// changes in a way an older peer cannot handle.
//
// 2 added job_rm, list, the -state flag, multi-id job_wait, and a job_list limit.
const Version = 2

// MinVersion is the oldest wire format this build can still serve.
//
// Compatibility is a range, not an exact match. An agent one version ahead of its
// host is normally fine -- new ops are additive, and the host simply does not use
// them -- so rejecting it outright would break the case where two machines share a
// dev box and the newer rdev uploaded the binary last. The host checks that its own
// Version falls within the agent's [MinVersion, Version] range and proceeds if it
// does.
//
// Raise this only when support for an older format is genuinely dropped, which
// forces the peer to upgrade instead of failing at an arbitrary later op.
const MinVersion = 1

// Op names carried in Request.Op.
const (
	OpPing      = "ping"
	OpExec      = "exec"
	OpReadFile  = "read_file"
	OpWriteFile = "write_file"
	OpJobStart  = "job_start"
	OpJobList   = "job_list"
	OpJobStatus = "job_status"
	OpJobLogs   = "job_logs"
	OpJobStop   = "job_stop"
	OpJobWait   = "job_wait"
	OpJobRm     = "job_rm"
	OpList      = "list"
)

// Job states reported by the agent.
const (
	JobRunning = "running"
	JobExited  = "exited"
	JobKilled  = "killed"
	JobUnknown = "unknown"
)

// Request is one JSON-encoded line sent to the agent's stdin.
type Request struct {
	ID   string       `json:"id"`
	Op   string       `json:"op"`
	Exec *ExecParams  `json:"exec,omitempty"`
	Read *ReadParams  `json:"read,omitempty"`
	Cat  *WriteParams `json:"write,omitempty"`
	Job  *JobParams   `json:"job,omitempty"`
	List *ListParams  `json:"list,omitempty"`
}

// ExecParams describes a foreground command.
type ExecParams struct {
	// Argv is the command and its arguments. Argv[0] is resolved against PATH.
	// Never a shell string.
	Argv []string `json:"argv"`
	// Cwd is the working directory. "~" and "~/" are expanded by the agent.
	Cwd string `json:"cwd,omitempty"`
	// Env entries are added to the inherited environment.
	Env map[string]string `json:"env,omitempty"`
	// LoginShell wraps the command so the user's profile is sourced first.
	//
	// This exists because a non-login ssh command misses ~/.bashrc, which is
	// where tools installed to ~/.local/bin (uv, pipx, cargo) land. Rather
	// than reintroduce shell parsing, the agent sources the profile and then
	// execs argv via a positional-argument trampoline, so argv stays a list.
	LoginShell bool `json:"login_shell,omitempty"`
	// Stdin is written to the child's standard input, then closed.
	Stdin string `json:"stdin,omitempty"`
	// TimeoutSec kills the child after this many seconds. 0 means no timeout.
	TimeoutSec int `json:"timeout_sec,omitempty"`
	// MaxOutputBytes caps stdout and stderr independently. Output beyond the
	// cap is dropped from the reply but the full stream is still counted, so
	// callers can tell how much they did not see.
	MaxOutputBytes int `json:"max_output_bytes,omitempty"`
}

// ReadParams reads a slice of a remote file.
type ReadParams struct {
	Path string `json:"path"`
	// Offset is a byte offset from the start of the file.
	Offset int64 `json:"offset,omitempty"`
	// Limit caps the number of bytes returned. 0 means the agent default.
	Limit int64 `json:"limit,omitempty"`
}

// WriteParams writes a whole remote file, creating parent directories.
type WriteParams struct {
	Path string `json:"path"`
	// Content is the literal file body. Base64 when ContentB64 is set.
	Content string `json:"content"`
	// ContentB64 marks Content as base64-encoded, for binary payloads.
	ContentB64 bool `json:"content_b64,omitempty"`
	// Mode is the octal file mode, e.g. 0o755. 0 means 0o644.
	Mode uint32 `json:"mode,omitempty"`
	// Append adds to an existing file instead of truncating it.
	Append bool `json:"append,omitempty"`
}

// JobParams covers the job lifecycle ops.
type JobParams struct {
	// ID identifies an existing job for status, logs, and stop.
	ID string `json:"id,omitempty"`
	// IDs waits on several jobs at once in job_wait. Without it, waiting on N
	// parallel jobs costs N serial calls, each re-sending the same context.
	IDs []string `json:"ids,omitempty"`
	// Spec starts a new job. Its TimeoutSec and MaxOutputBytes are ignored:
	// jobs outlive the request, and their output goes to files on disk.
	Spec *ExecParams `json:"spec,omitempty"`
	// Label is a human-readable tag recorded with the job.
	Label string `json:"label,omitempty"`
	// WaitAny returns as soon as one of IDs finishes, instead of waiting for all
	// of them. Useful for reacting to the first failure in a batch.
	WaitAny bool `json:"wait_any,omitempty"`

	// Stream selects "stdout" or "stderr" for job_logs.
	Stream string `json:"stream,omitempty"`
	// TailLines returns only the last N lines. Applied after Grep.
	TailLines int `json:"tail_lines,omitempty"`
	// Grep keeps only lines containing this substring. Filtering happens on
	// the remote side so a multi-megabyte log never crosses the wire.
	Grep string `json:"grep,omitempty"`
	// SinceOffset resumes reading from a byte offset, for incremental polling.
	SinceOffset int64 `json:"since_offset,omitempty"`

	// Signal is "TERM" or "KILL" for job_stop. Default "TERM".
	Signal string `json:"signal,omitempty"`
	// GraceSec waits this long after TERM before sending KILL. 0 skips KILL.
	GraceSec int `json:"grace_sec,omitempty"`

	// WaitTimeoutSec bounds a job_wait call. The agent returns with TimedOut set
	// rather than blocking forever, so the caller can decide whether to keep
	// waiting; an unbounded wait would strand the request if the job never ends.
	WaitTimeoutSec int `json:"wait_timeout_sec,omitempty"`
	// TailOnExit returns this many trailing stdout lines with the final status,
	// saving a follow-up job_logs round trip.
	TailOnExit int `json:"tail_on_exit,omitempty"`

	// OlderThanSec removes finished jobs that ended more than this long ago,
	// instead of removing one job by ID. Job logs are unbounded, so without a
	// reclaim path the state directory grows for the life of the machine.
	OlderThanSec int `json:"older_than_sec,omitempty"`
	// KeepLast retains this many of the most recent finished jobs, removing the
	// rest. Combined with OlderThanSec, a job must satisfy both to be removed.
	KeepLast int `json:"keep_last,omitempty"`

	// Limit caps how many jobs job_list returns, newest first. Applied before
	// metadata is read, so a small limit is cheap on a host with many jobs.
	Limit int `json:"limit,omitempty"`
}

// Response is one JSON-encoded line read from the agent's stdout.
type Response struct {
	ID  string `json:"id"`
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`

	Ping *PingResult  `json:"ping,omitempty"`
	Exec *ExecResult  `json:"exec,omitempty"`
	Read *ReadResult  `json:"read,omitempty"`
	Cat  *WriteResult `json:"write,omitempty"`
	Job  *JobResult   `json:"job,omitempty"`
	List *ListResult  `json:"list,omitempty"`
}

// PingResult reports agent identity for the handshake.
type PingResult struct {
	// Version is the wire format the agent speaks.
	Version int `json:"version"`
	// MinVersion is the oldest format the agent still serves. Zero from a build
	// that predates the field, which the host reads as "exactly Version".
	MinVersion int    `json:"min_version,omitempty"`
	Binary     string `json:"binary"`
	Home       string `json:"home"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	PID        int    `json:"pid"`
	// Build is the agent's build stamp (see internal/buildinfo). Empty from a
	// build that predates it, which callers must read as "unknown" rather than
	// as a mismatch. Protocol version answers "can we talk"; this answers "which
	// binary am I actually talking to", which is what a bootstrap that keeps
	// flipping between two builds needs.
	Build string `json:"build,omitempty"`
}

// Compatible reports whether an agent advertising this ping can serve a host
// speaking hostVersion.
//
// An agent newer than the host is accepted as long as it still serves the host's
// format: new ops are additive, so the host just does not use them. An agent older
// than the host is rejected, because the host may issue an op it does not have --
// which would otherwise surface as a confusing "unknown op" partway through a
// session instead of at connect time.
func (p *PingResult) Compatible(hostVersion int) bool {
	if p == nil {
		return false
	}
	min := p.MinVersion
	if min == 0 {
		// A build predating MinVersion serves exactly one format.
		min = p.Version
	}
	return hostVersion >= min && hostVersion <= p.Version
}

// ExecResult reports the outcome of a foreground command.
type ExecResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	// StdoutBytes and StderrBytes are the true stream sizes before capping,
	// so a caller seeing truncation knows the real volume.
	StdoutBytes int64 `json:"stdout_bytes"`
	StderrBytes int64 `json:"stderr_bytes"`
	Truncated   bool  `json:"truncated,omitempty"`
	// TimedOut reports that the child was killed by TimeoutSec rather than
	// exiting on its own. ExitCode is meaningless in that case.
	//
	// Stdout and Stderr still carry whatever was produced before the kill, so a
	// bounded exec doubles as a way to peek at a slow command. Discarding it would
	// make a timeout indistinguishable from a command that printed nothing.
	TimedOut   bool  `json:"timed_out,omitempty"`
	DurationMS int64 `json:"duration_ms"`
}

// ReadResult carries a slice of file content.
type ReadResult struct {
	Content    string `json:"content"`
	ContentB64 bool   `json:"content_b64,omitempty"`
	Size       int64  `json:"size"`
	// EOF reports that the returned slice reaches the end of the file.
	EOF bool `json:"eof"`
}

// WriteResult confirms a write.
type WriteResult struct {
	Path         string `json:"path"`
	BytesWritten int    `json:"bytes_written"`
}

// JobInfo is the persisted record of one job.
type JobInfo struct {
	ID    string   `json:"id"`
	Label string   `json:"label,omitempty"`
	Argv  []string `json:"argv"`
	Cwd   string   `json:"cwd,omitempty"`
	// PID is the supervisor process, which is also the job's process group id.
	PID   int    `json:"pid"`
	State string `json:"state"`
	// ChildPID is the supervised command, reported only when the supervisor died
	// and the child was orphaned.
	ChildPID int `json:"child_pid,omitempty"`
	// Orphaned marks a job still running without its supervisor. The work
	// continues but no exit code will be recorded.
	Orphaned bool `json:"orphaned,omitempty"`
	// ExitCode is valid only when State is JobExited.
	ExitCode  int    `json:"exit_code,omitempty"`
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at,omitempty"`
}

// JobResult is the union of replies for the job ops.
type JobResult struct {
	Info *JobInfo   `json:"info,omitempty"`
	List []*JobInfo `json:"list,omitempty"`

	// Waited holds one entry per requested job when job_wait was given IDs. Each
	// carries the same Logs treatment as a single wait.
	Waited []*WaitedJob `json:"waited,omitempty"`

	// Logs fields, set for job_logs.
	Logs string `json:"logs,omitempty"`
	// NextOffset is the byte offset to pass as SinceOffset on the next poll.
	NextOffset int64 `json:"next_offset,omitempty"`
	// LogSize is the current total size of the selected stream.
	LogSize int64 `json:"log_size,omitempty"`
	// Matched counts lines kept by Grep, before TailLines was applied.
	Matched int `json:"matched,omitempty"`

	// TimedOut is set by job_wait when the wait budget expired while the job was
	// still running. The job is unaffected; the caller may wait again.
	TimedOut bool `json:"timed_out,omitempty"`
	// WaitedMS is how long job_wait actually blocked.
	WaitedMS int64 `json:"waited_ms,omitempty"`

	// Removed lists job IDs deleted by job_rm.
	Removed []string `json:"removed,omitempty"`
	// FreedBytes is the disk space reclaimed by job_rm.
	FreedBytes int64 `json:"freed_bytes,omitempty"`
	// Skipped lists jobs that matched the filter but were left alone because they
	// are still running. Removing a live job's records would orphan the process
	// with no way to observe or stop it.
	Skipped []string `json:"skipped,omitempty"`
	// Missing lists jobs whose records were already gone. Distinct from Skipped:
	// that means "alive, deliberately kept", this means "nothing left to delete".
	// Reported rather than raised as an error so a repeated or concurrent job_rm
	// is idempotent -- the caller asked for the job to be absent, and it is.
	Missing []string `json:"missing,omitempty"`

	// Total is the number of jobs on the host before Limit was applied, so a
	// caller can tell a listing was cut short.
	Total int `json:"total,omitempty"`
	// Truncated reports that Limit hid some jobs.
	Truncated bool `json:"truncated,omitempty"`
}

// WaitedJob is one job's outcome in a multi-job wait.
type WaitedJob struct {
	ID   string   `json:"id"`
	Info *JobInfo `json:"info,omitempty"`
	// Err explains why this particular job could not be waited on (typically an
	// unknown id). One bad id does not fail the whole call, since the other jobs
	// still have useful answers.
	Err string `json:"err,omitempty"`
	// Logs is the trailing output when TailOnExit was requested.
	Logs string `json:"logs,omitempty"`
}

// DirEntry is one item in a directory listing.
type DirEntry struct {
	Name string `json:"name"`
	// Size is the file size in bytes; meaningless for directories.
	Size  int64  `json:"size"`
	Mode  string `json:"mode"`
	IsDir bool   `json:"is_dir,omitempty"`
	// Symlink marks an entry that is a symbolic link. Size and IsDir describe the
	// link itself, not its target, since resolving it may cross a mount or dangle.
	Symlink bool `json:"symlink,omitempty"`
	// ModTime is RFC3339 in UTC.
	ModTime string `json:"mod_time"`
}

// ListParams reads a remote directory.
type ListParams struct {
	Path string `json:"path"`
	// Limit caps the number of entries returned. 0 means the agent default.
	Limit int `json:"limit,omitempty"`
}

// ListResult carries a directory listing.
type ListResult struct {
	Path    string     `json:"path"`
	Entries []DirEntry `json:"entries"`
	// Total is the full entry count before Limit was applied, so a caller can
	// tell a listing was cut short.
	Total     int  `json:"total"`
	Truncated bool `json:"truncated,omitempty"`
}
