// Package proto defines the wire contract between the rdev host and the
// rdev-agent running on a remote machine.
//
// Every request carries an explicit argv slice. Nothing in this protocol is a
// shell string: the agent execs argv directly, so quoting, word splitting, and
// glob expansion never happen. That property is the whole reason this protocol
// exists, so keep it when adding ops.
package proto

// Version is bumped whenever the wire format changes incompatibly. The host
// compares it against the agent's reported version during handshake and
// re-uploads the agent binary on mismatch.
const Version = 1

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
	// Spec starts a new job. Its TimeoutSec and MaxOutputBytes are ignored:
	// jobs outlive the request, and their output goes to files on disk.
	Spec *ExecParams `json:"spec,omitempty"`
	// Label is a human-readable tag recorded with the job.
	Label string `json:"label,omitempty"`

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
}

// PingResult reports agent identity for the handshake.
type PingResult struct {
	Version int    `json:"version"`
	Binary  string `json:"binary"`
	Home    string `json:"home"`
	OS      string `json:"os"`
	Arch    string `json:"arch"`
	PID     int    `json:"pid"`
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
	PID   int      `json:"pid"`
	State string   `json:"state"`
	// ExitCode is valid only when State is JobExited.
	ExitCode  int    `json:"exit_code,omitempty"`
	StartedAt string `json:"started_at"`
	EndedAt   string `json:"ended_at,omitempty"`
}

// JobResult is the union of replies for the job ops.
type JobResult struct {
	Info *JobInfo   `json:"info,omitempty"`
	List []*JobInfo `json:"list,omitempty"`

	// Logs fields, set for job_logs.
	Logs string `json:"logs,omitempty"`
	// NextOffset is the byte offset to pass as SinceOffset on the next poll.
	NextOffset int64 `json:"next_offset,omitempty"`
	// LogSize is the current total size of the selected stream.
	LogSize int64 `json:"log_size,omitempty"`
	// Matched counts lines kept by Grep, before TailLines was applied.
	Matched int `json:"matched,omitempty"`
}
