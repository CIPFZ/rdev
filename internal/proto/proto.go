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
// 3 adds stable operation identities, structured errors, protocol-level
// cancellation/deadlines, feature negotiation, stream events, and explicit
// truncation accounting.
const Version = 3

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
const MinVersion = 2

// Op names carried in Request.Op.
const (
	OpPing            = "ping"
	OpExec            = "exec"
	OpReadFile        = "read_file"
	OpWriteFile       = "write_file"
	OpJobStart        = "job_start"
	OpJobList         = "job_list"
	OpJobStatus       = "job_status"
	OpJobLogs         = "job_logs"
	OpJobStop         = "job_stop"
	OpJobWait         = "job_wait"
	OpJobRm           = "job_rm"
	OpStorageStatus   = "storage_status"
	OpStorageGC       = "storage_gc"
	OpStorageDoctor   = "storage_doctor"
	OpStateInspect    = "state_inspect"
	OpStateMigrate    = "state_migrate"
	OpStateRepair     = "state_repair"
	OpCapabilityProbe = "capability_probe"
	OpList            = "list"
	OpCancel          = "cancel"
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
	// ID is a connection-local request identity. It may change on a transport
	// retry and must never be used as the exactly-once identity of an operation.
	ID string `json:"id"`
	// OperationID is stable across reconnects and transport retries.
	OperationID string `json:"operation_id,omitempty"`
	// ClientID identifies the calling client session. Agent deduplication binds
	// OperationID to this identity so unrelated clients cannot claim each
	// other's cached result.
	ClientID string `json:"client_id,omitempty"`
	Op       string `json:"op"`
	// Replay distinguishes a retry from a first attempt. A mutating replay whose
	// deduplication record is missing must return ambiguous_outcome rather than
	// silently execute again.
	Replay bool `json:"replay,omitempty"`
	// DeadlineUnixMilli is an absolute deadline. It is deliberately part of the
	// canonical request digest and therefore cannot change across a retry.
	DeadlineUnixMilli int64 `json:"deadline_unix_milli,omitempty"`
	// StreamWindowBytes is the maximum total data-frame payload the agent may
	// emit before the final frame. Zero disables data frames (accepted/progress/
	// final still apply). It may only lower the shared hard window.
	StreamWindowBytes int64             `json:"stream_window_bytes,omitempty"`
	Hello             *HelloParams      `json:"hello,omitempty"`
	Cancel            *CancelParams     `json:"cancel,omitempty"`
	Exec              *ExecParams       `json:"exec,omitempty"`
	Read              *ReadParams       `json:"read,omitempty"`
	Cat               *WriteParams      `json:"write,omitempty"`
	Job               *JobParams        `json:"job,omitempty"`
	List              *ListParams       `json:"list,omitempty"`
	Storage           *StorageParams    `json:"storage,omitempty"`
	State             *StateParams      `json:"state,omitempty"`
	Capability        *CapabilityParams `json:"capability,omitempty"`
}

// CancelParams targets one foreground operation. Detached jobs are controlled
// by job_stop and are never implicitly converted into foreground cancellation.
type CancelParams struct {
	OperationID string `json:"operation_id"`
	// TargetOp binds cancel-before-request tombstones to an operation whose
	// registry policy explicitly permits foreground cancellation.
	TargetOp string `json:"target_op,omitempty"`
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
	// Mode is the octal file mode, e.g. 0o755. 0 preserves an existing mode;
	// newly created files use a restrictive 0o600 mode.
	Mode uint32 `json:"mode,omitempty"`
	// Append adds to an existing file instead of truncating it.
	Append bool `json:"append,omitempty"`
	// TransferID enables resumable chunked transfer. Chunks are written to a
	// managed staging file and become visible only when Final verifies Size and
	// Digest. Offset must equal the currently staged length.
	TransferID string `json:"transfer_id,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	TotalSize  int64  `json:"total_size,omitempty"`
	Digest     string `json:"digest,omitempty"`
	Final      bool   `json:"final,omitempty"`
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
	// Resources requests a bounded process envelope. Zero fields are
	// unspecified; the agent returns the effective values in JobInfo.
	Resources *ResourceEnvelope `json:"resources,omitempty"`
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

	// Limit caps how many jobs job_list returns, newest first by StartedAt and ID.
	// Metadata is scanned before the limit so the result is globally newest.
	Limit int `json:"limit,omitempty"`
}

// StorageParams controls bounded storage inspection and reclamation. Scope is
// "remote_state" (the agent's managed state root) or "local" (an alias kept
// for callers that use local/remote policy terminology). Unknown scopes fail
// closed; no arbitrary filesystem path is accepted.
type StorageParams struct {
	Scope          string `json:"scope,omitempty"`
	DryRun         bool   `json:"dry_run,omitempty"`
	MaxScanJobs    int    `json:"max_scan_jobs,omitempty"`
	MaxDeleteJobs  int    `json:"max_delete_jobs,omitempty"`
	MaxDeleteBytes int64  `json:"max_delete_bytes,omitempty"`
}

// Response is one JSON-encoded line read from the agent's stdout.
type Response struct {
	ID          string         `json:"id"`
	OperationID string         `json:"operation_id,omitempty"`
	Type        EventKind      `json:"type,omitempty"`
	StreamID    string         `json:"stream_id,omitempty"`
	Seq         uint64         `json:"seq,omitempty"`
	Terminal    bool           `json:"terminal,omitempty"`
	Execution   ExecutionState `json:"execution_state,omitempty"`
	OK          bool           `json:"ok"`
	// Err is retained for protocol-2 compatibility. Protocol-3 peers use Error.
	Err      string         `json:"err,omitempty"`
	Error    *ErrorEnvelope `json:"error,omitempty"`
	Data     *DataFrame     `json:"data,omitempty"`
	Progress *ProgressFrame `json:"progress,omitempty"`

	Ping       *PingResult       `json:"ping,omitempty"`
	Exec       *ExecResult       `json:"exec,omitempty"`
	Read       *ReadResult       `json:"read,omitempty"`
	Cat        *WriteResult      `json:"write,omitempty"`
	Job        *JobResult        `json:"job,omitempty"`
	Storage    *StorageResult    `json:"storage,omitempty"`
	State      *StateResult      `json:"state,omitempty"`
	Capability *CapabilityResult `json:"capability,omitempty"`
	List       *ListResult       `json:"list,omitempty"`
}

// ResourceEnvelope carries requested and effective process budgets.
type ResourceEnvelope struct {
	CPUQuotaMillis int64 `json:"cpu_quota_millis,omitempty"`
	MemoryBytes    int64 `json:"memory_bytes,omitempty"`
	PIDs           int   `json:"pids,omitempty"`
	FDs            int   `json:"fds,omitempty"`
	WallTimeoutSec int   `json:"wall_timeout_sec,omitempty"`
	JobCount       int   `json:"job_count,omitempty"`
}

type CapabilityParams struct {
	Refresh bool `json:"refresh,omitempty"`
}

type ExecutionProfile struct {
	Shell      string `json:"shell,omitempty"`
	Path       string `json:"path,omitempty"`
	Home       string `json:"home,omitempty"`
	Locale     string `json:"locale,omitempty"`
	Cwd        string `json:"cwd,omitempty"`
	Umask      string `json:"umask,omitempty"`
	LoginShell bool   `json:"login_shell"`
	Digest     string `json:"digest,omitempty"`
}

type CapabilityResult struct {
	ProbeVersion string            `json:"probe_version"`
	ProbedAt     string            `json:"probed_at"`
	OS           string            `json:"os"`
	Arch         string            `json:"arch"`
	Cgroup       bool              `json:"cgroup"`
	Rlimit       bool              `json:"rlimit"`
	Resources    ResourceEnvelope  `json:"resources"`
	Effective    ResourceEnvelope  `json:"effective"`
	Profile      *ExecutionProfile `json:"profile,omitempty"`
}

// StateParams controls inspection and maintenance of the agent state root.
// Mutating operations require DryRun=false explicitly; callers should preview
// the returned Changed/Quarantined paths before applying them.
type StateParams struct {
	DryRun bool `json:"dry_run,omitempty"`
}

type StateResult struct {
	Root          string         `json:"root"`
	DryRun        bool           `json:"dry_run"`
	SchemaVersion int            `json:"schema_version"`
	Manifest      *StateManifest `json:"manifest,omitempty"`
	Records       []StateRecord  `json:"records,omitempty"`
	Findings      []StateFinding `json:"findings,omitempty"`
	Changed       []string       `json:"changed,omitempty"`
	Quarantined   []string       `json:"quarantined,omitempty"`
}

type StateManifest struct {
	SchemaVersion int    `json:"schema_version"`
	WriterVersion string `json:"writer_version,omitempty"`
	AgentIdentity string `json:"agent_identity,omitempty"`
	Namespace     string `json:"namespace,omitempty"`
	LastMigration string `json:"last_migration,omitempty"`
}
type StateRecord struct {
	Path          string `json:"path"`
	SchemaVersion int    `json:"schema_version"`
	Valid         bool   `json:"valid"`
	Bytes         int64  `json:"bytes"`
}
type StateFinding struct {
	Path    string `json:"path"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Action  string `json:"action,omitempty"`
}

type DataFrame struct {
	Stream     string     `json:"stream"`
	Content    string     `json:"content"`
	ContentB64 bool       `json:"content_b64,omitempty"`
	Truncation Truncation `json:"truncation"`
}

type ProgressFrame struct {
	Phase string `json:"phase"`
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
	// NegotiatedVersion is the highest version in the peer range intersection.
	// Zero means this field came from a protocol-2 peer.
	NegotiatedVersion int       `json:"negotiated_version,omitempty"`
	Features          []Feature `json:"features,omitempty"`
}

// Compatible reports whether an agent advertising this ping has a protocol
// version in common with this build through hostVersion.
//
// Compatibility is only the first gate. Callers must also negotiate features and
// refuse any operation whose required feature is absent; protocol overlap must
// never be treated as permission for a dangerous semantic downgrade.
func (p *PingResult) Compatible(hostVersion int) bool {
	if p == nil {
		return false
	}
	min := p.MinVersion
	if min == 0 {
		// A build predating MinVersion serves exactly one format.
		min = p.Version
	}
	_, ok := NegotiateVersion(
		ProtocolRange{Min: MinVersion, Max: hostVersion},
		ProtocolRange{Min: min, Max: p.Version},
	)
	return ok
}

// ExecResult reports the outcome of a foreground command.
type ExecResult struct {
	OperationID string         `json:"operation_id,omitempty"`
	Terminal    bool           `json:"terminal"`
	Execution   ExecutionState `json:"execution_state"`
	ExitCode    int            `json:"exit_code"`
	Stdout      string         `json:"stdout"`
	Stderr      string         `json:"stderr"`
	StdoutB64   bool           `json:"stdout_b64,omitempty"`
	StderrB64   bool           `json:"stderr_b64,omitempty"`
	// StdoutBytes and StderrBytes are the true stream sizes before capping,
	// so a caller seeing truncation knows the real volume.
	StdoutBytes int64 `json:"stdout_bytes"`
	StderrBytes int64 `json:"stderr_bytes"`
	Truncated   bool  `json:"truncated,omitempty"`
	// Per-stream metadata removes the ambiguity in the legacy combined flag and
	// preserves exact retained/original/dropped byte counts.
	StdoutTruncation Truncation `json:"stdout_truncation"`
	StderrTruncation Truncation `json:"stderr_truncation"`
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
	OperationID string         `json:"operation_id,omitempty"`
	Terminal    bool           `json:"terminal"`
	Execution   ExecutionState `json:"execution_state"`
	Content     string         `json:"content"`
	ContentB64  bool           `json:"content_b64,omitempty"`
	Size        int64          `json:"size"`
	// EOF reports that the returned slice reaches the end of the file.
	EOF        bool       `json:"eof"`
	Truncation Truncation `json:"truncation"`
}

// WriteResult confirms a write.
type WriteResult struct {
	OperationID  string         `json:"operation_id,omitempty"`
	Terminal     bool           `json:"terminal"`
	Execution    ExecutionState `json:"execution_state"`
	Path         string         `json:"path"`
	BytesWritten int            `json:"bytes_written"`
	Offset       int64          `json:"offset,omitempty"`
	Committed    bool           `json:"committed,omitempty"`
	Resumed      bool           `json:"resumed,omitempty"`
}

// JobInfo is the persisted record of one job.
type JobInfo struct {
	OperationID string         `json:"operation_id,omitempty"`
	Terminal    bool           `json:"terminal"`
	Execution   ExecutionState `json:"execution_state"`
	ID          string         `json:"id"`
	Label       string         `json:"label,omitempty"`
	Argv        []string       `json:"argv"`
	Cwd         string         `json:"cwd,omitempty"`
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
	ExitCode     int       `json:"exit_code,omitempty"`
	StartedAt    string    `json:"started_at"`
	EndedAt      string    `json:"ended_at,omitempty"`
	StdoutLedger LogLedger `json:"stdout_ledger"`
	StderrLedger LogLedger `json:"stderr_ledger"`
	// Requested and Effective preserve the admission decision for retries and
	// inspection. Effective is never broader than the remote hard policy.
	Requested     ResourceEnvelope `json:"requested_resources"`
	Effective     ResourceEnvelope `json:"effective_resources"`
	ResourceLimit string           `json:"resource_limit,omitempty"`
}

// LogLedger is the durable accounting for a bounded job log stream.
type LogLedger struct {
	OriginalBytes    int64  `json:"original_bytes"`
	RetainedBytes    int64  `json:"retained_bytes"`
	DroppedBytes     int64  `json:"dropped_bytes"`
	Truncated        bool   `json:"truncated"`
	FirstTruncatedAt string `json:"first_truncated_at,omitempty"`
	LimitBytes       int64  `json:"limit_bytes"`
	Policy           string `json:"policy"`
}

// JobResult is the union of replies for the job ops.
type JobResult struct {
	OperationID string         `json:"operation_id,omitempty"`
	Terminal    bool           `json:"terminal"`
	Execution   ExecutionState `json:"execution_state"`
	Info        *JobInfo       `json:"info,omitempty"`
	List        []*JobInfo     `json:"list,omitempty"`

	// Waited holds one entry per requested job when job_wait was given IDs. Each
	// carries the same Logs treatment as a single wait.
	Waited []*WaitedJob `json:"waited,omitempty"`

	// Logs fields, set for job_logs.
	Logs           string     `json:"logs,omitempty"`
	LogsTruncation Truncation `json:"logs_truncation"`
	LogLedger      LogLedger  `json:"log_ledger"`
	// NextOffset is the byte offset to pass as SinceOffset on the next poll.
	NextOffset int64 `json:"next_offset,omitempty"`
	// LogSize is the current total size of the selected stream.
	LogSize int64 `json:"log_size,omitempty"`
	// Matched counts lines kept by Grep, before TailLines was applied.
	Matched int `json:"matched,omitempty"`
	// TailTruncated indicates the bounded backward scan could not reach the
	// requested number of lines. TailScanBytes records bytes inspected.
	TailTruncated bool  `json:"tail_truncated,omitempty"`
	TailScanBytes int64 `json:"tail_scan_bytes,omitempty"`

	// TimedOut is set by job_wait when the wait budget expired while the job was
	// still running. The job is unaffected; the caller may wait again.
	TimedOut bool `json:"timed_out,omitempty"`
	// WaitedMS is how long job_wait actually blocked.
	WaitedMS int64 `json:"waited_ms,omitempty"`

	// Removed lists job IDs deleted by job_rm.
	Removed []string `json:"removed,omitempty"`
	// FreedBytes is the logical size of non-directory entries reclaimed by
	// job_rm. A job is only counted after its complete record is removed.
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

type StorageScope struct {
	Name          string          `json:"name"`
	Root          string          `json:"root"`
	UsedBytes     int64           `json:"used_bytes"`
	FreeBytes     int64           `json:"free_bytes,omitempty"`
	MaxBytes      int64           `json:"max_bytes"`
	TargetBytes   int64           `json:"target_bytes,omitempty"`
	MinFreeBytes  int64           `json:"min_free_bytes,omitempty"`
	HighWatermark float64         `json:"high_watermark"`
	LowWatermark  float64         `json:"low_watermark"`
	RetentionSec  int64           `json:"retention_sec"`
	KeepLastJobs  int             `json:"keep_last_jobs"`
	JobCount      int             `json:"job_count"`
	RunningJobs   int             `json:"running_jobs"`
	Pressure      bool            `json:"pressure"`
	PolicySource  string          `json:"policy_source"`
	Metrics       *StorageMetrics `json:"metrics,omitempty"`
}

// StorageMetrics is a bounded, low-cardinality telemetry snapshot. Scope is
// represented by fixed fields (local/remote_state), never arbitrary labels.
type StorageMetrics struct {
	SchemaVersion  int                    `json:"schema_version"`
	Local          StorageMetricsScope    `json:"local"`
	RemoteState    StorageMetricsScope    `json:"remote_state"`
	Logs           StorageLogMetrics      `json:"logs"`
	GC             StorageGCMetrics       `json:"gc"`
	PressureEvents []StoragePressureEvent `json:"pressure_events,omitempty"`
	QuotaHits      uint64                 `json:"quota_hits_total"`
}

type StorageMetricsScope struct {
	UsedBytes     int64  `json:"used_bytes"`
	FreeBytes     int64  `json:"free_bytes,omitempty"`
	BudgetBytes   int64  `json:"budget_bytes"`
	Pressure      bool   `json:"pressure"`
	PressureLevel string `json:"pressure_level"`
}

type StorageLogMetrics struct {
	OriginalBytes uint64 `json:"original_bytes"`
	RetainedBytes uint64 `json:"retained_bytes"`
	DroppedBytes  uint64 `json:"dropped_bytes"`
}

type StorageGCMetrics struct {
	ScannedJobs    uint64            `json:"scanned_jobs"`
	RemovedJobs    uint64            `json:"removed_jobs"`
	FreedBytes     uint64            `json:"freed_bytes"`
	Errors         uint64            `json:"errors"`
	DurationMS     uint64            `json:"duration_ms"`
	Runs           map[string]uint64 `json:"runs"`
	FailureReasons map[string]uint64 `json:"failure_reasons"`
}

type StoragePressureEvent struct {
	Scope string `json:"scope"`
	State string `json:"state"`
	At    string `json:"at"`
}

type StorageResult struct {
	OperationID string               `json:"operation_id,omitempty"`
	Terminal    bool                 `json:"terminal"`
	Execution   ExecutionState       `json:"execution_state"`
	Status      *StorageScope        `json:"status,omitempty"`
	GC          *StorageGCReport     `json:"gc,omitempty"`
	Doctor      *StorageDoctorReport `json:"doctor,omitempty"`
}

type StorageGCItem struct {
	ID        string `json:"id"`
	Bytes     int64  `json:"bytes"`
	Reason    string `json:"reason"`
	StartedAt string `json:"started_at,omitempty"`
	EndedAt   string `json:"ended_at,omitempty"`
}

type StorageGCReport struct {
	Root          string          `json:"root"`
	UsedBytes     int64           `json:"used_bytes"`
	FreeBytes     int64           `json:"free_bytes,omitempty"`
	TargetBytes   int64           `json:"target_bytes,omitempty"`
	Scanned       int             `json:"scanned"`
	ScanTruncated bool            `json:"scan_truncated,omitempty"`
	Pressure      bool            `json:"pressure"`
	DryRun        bool            `json:"dry_run"`
	Candidates    []StorageGCItem `json:"candidates,omitempty"`
	Removed       []StorageGCItem `json:"removed,omitempty"`
	Skipped       []string        `json:"skipped,omitempty"`
	FreedBytes    int64           `json:"freed_bytes"`
	Errors        []string        `json:"errors,omitempty"`
	Metrics       *StorageMetrics `json:"metrics,omitempty"`
}

type StorageDoctorFinding struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Path     string `json:"path,omitempty"`
	Message  string `json:"message"`
	Action   string `json:"action,omitempty"`
}

type StorageDoctorReport struct {
	Root     string                 `json:"root"`
	OK       bool                   `json:"ok"`
	Findings []StorageDoctorFinding `json:"findings,omitempty"`
	Metrics  *StorageMetrics        `json:"metrics,omitempty"`
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
	Logs           string     `json:"logs,omitempty"`
	LogsTruncation Truncation `json:"logs_truncation"`
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
	// MaxEntries is an explicit alias for Limit used by bounded callers.
	MaxEntries int `json:"max_entries,omitempty"`
	// MaxBytes caps the encoded listing payload. 0 means the agent default.
	MaxBytes int `json:"max_bytes,omitempty"`
	// Cursor resumes after the entry named by the previous page's NextCursor.
	Cursor string `json:"cursor,omitempty"`
}

// ListResult carries a directory listing.
type ListResult struct {
	OperationID string         `json:"operation_id,omitempty"`
	Terminal    bool           `json:"terminal"`
	Execution   ExecutionState `json:"execution_state"`
	Path        string         `json:"path"`
	Entries     []DirEntry     `json:"entries"`
	// Total is the full entry count before Limit was applied, so a caller can
	// tell a listing was cut short.
	Total     int  `json:"total"`
	Truncated bool `json:"truncated,omitempty"`
	// NextCursor is opaque and can be passed back in ListParams.Cursor.
	NextCursor string `json:"next_cursor,omitempty"`
}
