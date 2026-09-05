// Package mcpsrv exposes rdev over the Model Context Protocol.
//
// Tool inputs are typed structs, so the SDK derives JSON schemas and validates
// arguments before a handler runs. Every command-shaped tool takes argv as a
// string array; none accepts a shell string. That is deliberate and is the
// property that removes quoting bugs from the whole workflow.
package mcpsrv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/CIPFZ/rdev/internal/buildinfo"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Version is reported to MCP clients. Sourced from buildinfo so the release
// version is stamped in one place rather than declared twice; it is a var, not a
// const, because -ldflags -X can only write to a variable.
var Version = buildinfo.Version

// New builds a server with all rdev tools registered.
func New(c *client.Client) *mcp.Server {
	return newServer(c, c.Hosts.ApproveProject)
}

func newServer(c *client.Client, approveProject func(string) (session.ProjectTrust, error)) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{
		Name:    "rdev",
		Title:   "Remote dev environment proxy",
		Version: Version,
	}, nil)

	// Redaction backstop. Every handler already scrubs the fields it returns, but
	// that is a per-field discipline and it has failed once: SyncResult.Command
	// shipped a credential verbatim while Stdout, two lines above it in the same
	// struct literal, was scrubbed. Forgetting produces no error anywhere, and the
	// cost of forgetting is a plaintext credential in the transcript.
	//
	// This runs over the serialized result instead, so a field added later -- or a
	// whole new tool -- is covered without anyone remembering. It is a second line
	// of defence, not a replacement: per-field scrubbing stays, because a caller
	// using internal/client directly (the CLI does) never passes through here.
	s.AddReceivingMiddleware(redactResults(c))

	registerExec(s, c)
	registerJobs(s, c)
	registerFiles(s, c)
	registerSync(s, c)
	registerSession(s, c, approveProject)
	registerSecrets(s, c)
	return s
}

// redactResults scrubs registered secrets from tool results on their way out.
//
// By the time a result reaches middleware the SDK has turned the typed Out value
// into JSON. The middleware decodes it, recursively redacts original string values,
// and serializes again so JSON escaping cannot hide a secret.
//
// Only tools/call is touched. Other methods carry no remote output, and running the
// scan over list responses would cost bytes for nothing.
func redactResults(c *client.Client) mcp.Middleware {
	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			res, err := next(ctx, method, req)
			if method != "tools/call" {
				return res, err
			}
			// Stable rdev errors are tool results, not opaque MCP protocol errors.
			// This preserves code/retry/execution/operation/terminal fields so an
			// Agent can make the same decision as the CLI.
			if err != nil {
				var envelope *proto.ErrorEnvelope
				if errors.As(err, &envelope) {
					copy := *envelope
					ctr := &mcp.CallToolResult{
						IsError: true, StructuredContent: &copy,
						Content: []mcp.Content{&mcp.TextContent{Text: copy.Message}},
					}
					redactCallToolResult(c, ctr)
					return ctr, nil
				}
				err = errors.New(c.Secrets.Redact(err.Error()))
			}
			ctr, ok := res.(*mcp.CallToolResult)
			if !ok || ctr == nil {
				return res, err
			}
			// AddTool converts ordinary handler errors into CallToolResult before
			// receiving middleware runs. GetError retains the original typed error
			// specifically for middleware, so recover the stable envelope here.
			if underlying := ctr.GetError(); underlying != nil {
				var envelope *proto.ErrorEnvelope
				if errors.As(underlying, &envelope) {
					copy := *envelope
					ctr.IsError = true
					ctr.StructuredContent = &copy
					ctr.Content = []mcp.Content{&mcp.TextContent{Text: copy.Message}}
				}
			}
			redactCallToolResult(c, ctr)
			return ctr, err
		}
	}
}

// redactCallToolResult rewrites the two places a result carries text.
//
// StructuredContent holds the JSON the model reads; Content holds the text
// fallback the SDK derives from it. Both have to be scrubbed -- a client reading
// either one must not see a credential, and missing one of them would make the leak
// depend on which field the client happens to prefer.
func redactCallToolResult(c *client.Client, ctr *mcp.CallToolResult) {
	if raw, ok := ctr.StructuredContent.(json.RawMessage); ok {
		// Decode first so redaction sees the original string values rather than
		// their JSON-escaped representation. Quotes, backslashes, newlines and
		// Unicode escapes otherwise evade a literal scan of serialized bytes.
		if decoded, ok := decodeJSONValue(raw); ok {
			if scrubbed, err := json.Marshal(c.Secrets.RedactValue(decoded)); err == nil {
				ctr.StructuredContent = json.RawMessage(scrubbed)
			} else {
				ctr.StructuredContent = json.RawMessage(`{"error":"structured output redaction failed"}`)
			}
		} else {
			ctr.StructuredContent = json.RawMessage(`{"error":"structured output redaction failed"}`)
		}
	} else if ctr.StructuredContent != nil {
		ctr.StructuredContent = c.Secrets.RedactValue(ctr.StructuredContent)
	}
	for i, content := range ctr.Content {
		tc, ok := content.(*mcp.TextContent)
		if !ok {
			continue
		}
		scrubbed := ""
		if decoded, ok := decodeJSONValue([]byte(tc.Text)); ok {
			if raw, err := json.Marshal(c.Secrets.RedactValue(decoded)); err == nil {
				scrubbed = string(raw)
			}
		} else {
			scrubbed = c.Secrets.Redact(tc.Text)
		}
		if scrubbed != "" && scrubbed != tc.Text {
			ctr.Content[i] = &mcp.TextContent{Meta: tc.Meta, Annotations: tc.Annotations, Text: scrubbed}
		}
	}
}

func decodeJSONValue(raw []byte) (any, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, false
	}
	return decoded, true
}

// ---------- exec ----------

type ExecIn struct {
	Host string   `json:"host" jsonschema:"Host alias or ssh destination such as user@1.2.3.4:2222"`
	Argv []string `json:"argv" jsonschema:"Command and arguments as separate array elements. Never a shell string: argv is exec'd directly so quotes and $(...) are passed through literally. Use argv ['sh','-c','a | b'] only when you genuinely need a pipeline."`

	Cwd            string            `json:"cwd,omitempty" jsonschema:"Working directory. Supports a leading ~. Defaults to the host's session cwd."`
	Env            map[string]string `json:"env,omitempty" jsonschema:"Extra environment variables. Use the value 'secret:NAME' to inject a registered secret without exposing it."`
	LoginShell     *bool             `json:"login_shell,omitempty" jsonschema:"Source the login profile first so tools in ~/.local/bin (uv, pipx, cargo) resolve. Defaults to true."`
	Stdin          string            `json:"stdin,omitempty" jsonschema:"Data written to the command's stdin."`
	TimeoutSec     int               `json:"timeout_sec,omitempty" jsonschema:"Kill the command after this many seconds. Default 60. Use rdev_job_start for anything longer."`
	MaxOutputBytes int               `json:"max_output_bytes,omitempty" jsonschema:"Cap stdout and stderr each at this many bytes. Default 16000."`
}

type ExecOut struct {
	ExitCode         int                  `json:"exit_code"`
	Stdout           string               `json:"stdout"`
	Stderr           string               `json:"stderr"`
	StdoutB64        bool                 `json:"stdout_b64,omitempty"`
	StderrB64        bool                 `json:"stderr_b64,omitempty"`
	StdoutBytes      int64                `json:"stdout_bytes"`
	StderrBytes      int64                `json:"stderr_bytes"`
	Truncated        bool                 `json:"truncated,omitempty"`
	TimedOut         bool                 `json:"timed_out,omitempty"`
	DurationMS       int64                `json:"duration_ms"`
	Cwd              string               `json:"cwd,omitempty"`
	StdoutTruncation proto.Truncation     `json:"stdout_truncation"`
	StderrTruncation proto.Truncation     `json:"stderr_truncation"`
	OperationID      string               `json:"operation_id"`
	Terminal         bool                 `json:"terminal"`
	ExecutionState   proto.ExecutionState `json:"execution_state"`
}

const (
	defaultExecTimeoutSec = 60
	defaultExecMaxOutput  = 16000
)

func registerExec(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_exec",
		Description: "Run a command on a remote host and wait for it. " +
			"argv is a string array that is exec'd directly, so no shell parses it and quoting is never needed. " +
			"A non-zero exit_code is returned as data, not an error. " +
			"On timeout you still get whatever the command printed before it was killed, with timed_out set, " +
			"so a bounded exec is a reasonable way to peek at a slow command. " +
			"For anything expected to outlive the timeout use rdev_job_start instead, since a killed command loses its work.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ExecIn) (*mcp.CallToolResult, ExecOut, error) {
		timeout := in.TimeoutSec
		if timeout == 0 {
			timeout = defaultExecTimeoutSec
		}
		maxOut := in.MaxOutputBytes
		if maxOut == 0 {
			maxOut = defaultExecMaxOutput
		}

		res, err := c.Exec(ctx, client.ExecOptions{
			Host:           in.Host,
			Argv:           in.Argv,
			Cwd:            in.Cwd,
			Env:            in.Env,
			LoginShell:     in.LoginShell,
			Stdin:          in.Stdin,
			TimeoutSec:     timeout,
			MaxOutputBytes: maxOut,
		})
		if err != nil {
			return nil, ExecOut{}, err
		}
		return nil, toExecOut(res), nil
	})
}

func toExecOut(res *client.ExecResult) ExecOut {
	if res == nil {
		return ExecOut{}
	}
	return ExecOut{
		ExitCode: res.ExitCode, Stdout: res.Stdout, Stderr: res.Stderr,
		StdoutB64: res.StdoutB64, StderrB64: res.StderrB64,
		StdoutBytes: res.StdoutBytes, StderrBytes: res.StderrBytes,
		Truncated: res.Truncated, TimedOut: res.TimedOut, DurationMS: res.DurationMS, Cwd: res.Cwd,
		StdoutTruncation: res.StdoutTruncation, StderrTruncation: res.StderrTruncation,
		OperationID: res.OperationID, Terminal: res.Terminal, ExecutionState: res.Execution,
	}
}

// ---------- jobs ----------

type JobStartIn struct {
	Host       string            `json:"host"`
	Argv       []string          `json:"argv" jsonschema:"Command and arguments as separate array elements."`
	Cwd        string            `json:"cwd,omitempty"`
	Env        map[string]string `json:"env,omitempty" jsonschema:"Extra environment variables. 'secret:NAME' injects a registered secret."`
	LoginShell *bool             `json:"login_shell,omitempty"`
	Label      string            `json:"label,omitempty" jsonschema:"Short human-readable tag, e.g. 'swe-oracle-20'."`
}

type JobOut struct {
	ID        string   `json:"id"`
	Label     string   `json:"label,omitempty"`
	Argv      []string `json:"argv"`
	Cwd       string   `json:"cwd,omitempty"`
	PID       int      `json:"pid"`
	State     string   `json:"state" jsonschema:"running, exited, killed, or unknown"`
	ExitCode  int      `json:"exit_code,omitempty"`
	StartedAt string   `json:"started_at"`
	EndedAt   string   `json:"ended_at,omitempty"`
	// Orphaned and ChildPID surface a job whose supervisor died while the work
	// kept running. Without them an orphaned job is indistinguishable from a
	// healthy one here, even though no exit code will ever be recorded for it.
	Orphaned       bool                 `json:"orphaned,omitempty" jsonschema:"The supervisor died but the command is still running. The job is observable and stoppable, but its exit code is lost."`
	ChildPID       int                  `json:"child_pid,omitempty" jsonschema:"The surviving command's pid, reported only when the job is orphaned."`
	OperationID    string               `json:"operation_id,omitempty"`
	Terminal       bool                 `json:"terminal"`
	ExecutionState proto.ExecutionState `json:"execution_state"`
	StdoutLedger   proto.LogLedger      `json:"stdout_ledger"`
	StderrLedger   proto.LogLedger      `json:"stderr_ledger"`
}

func toJobOut(j *proto.JobInfo) JobOut {
	if j == nil {
		return JobOut{}
	}
	return JobOut{
		ID: j.ID, Label: j.Label, Argv: j.Argv, Cwd: j.Cwd, PID: j.PID,
		State: j.State, ExitCode: j.ExitCode, StartedAt: j.StartedAt, EndedAt: j.EndedAt,
		Orphaned: j.Orphaned, ChildPID: j.ChildPID,
		OperationID: j.OperationID, Terminal: j.Terminal, ExecutionState: j.Execution,
		StdoutLedger: j.StdoutLedger, StderrLedger: j.StderrLedger,
	}
}

type JobListIn struct {
	Host  string `json:"host"`
	Limit int    `json:"limit,omitempty" jsonschema:"Max jobs to return, newest first. Default 100. Applied before metadata is read, so a small limit stays cheap on a host with many jobs."`
}

type JobListOut struct {
	Jobs      []JobOut `json:"jobs"`
	Total     int      `json:"total" jsonschema:"Jobs on the host before limit was applied."`
	Truncated bool     `json:"truncated,omitempty"`
}

type JobRefIn struct {
	Host string `json:"host"`
	ID   string `json:"id" jsonschema:"Job id returned by rdev_job_start."`
}

type JobLogsIn struct {
	Host        string `json:"host"`
	ID          string `json:"id"`
	Stream      string `json:"stream,omitempty" jsonschema:"stdout or stderr. Default stdout."`
	TailLines   int    `json:"tail_lines,omitempty" jsonschema:"Return only the last N lines. Default 200."`
	Grep        string `json:"grep,omitempty" jsonschema:"Keep only lines containing this substring. Filtering runs on the remote host so large logs never cross the network."`
	SinceOffset int64  `json:"since_offset,omitempty" jsonschema:"Resume from a byte offset; pass the previous next_offset to poll incrementally."`
}

type JobLogsOut struct {
	Logs           string               `json:"logs"`
	NextOffset     int64                `json:"next_offset"`
	LogSize        int64                `json:"log_size"`
	Matched        int                  `json:"matched,omitempty" jsonschema:"How many lines matched grep in total. tail_lines is applied afterwards, so logs may contain fewer lines than this."`
	Returned       int                  `json:"returned" jsonschema:"How many lines are actually in logs."`
	Truncation     proto.Truncation     `json:"truncation"`
	OperationID    string               `json:"operation_id"`
	Terminal       bool                 `json:"terminal"`
	ExecutionState proto.ExecutionState `json:"execution_state"`
	Ledger         proto.LogLedger      `json:"ledger"`
}

type JobStopIn struct {
	Host     string `json:"host"`
	ID       string `json:"id"`
	Signal   string `json:"signal,omitempty" jsonschema:"TERM or KILL. Default TERM."`
	GraceSec int    `json:"grace_sec,omitempty" jsonschema:"Seconds to wait after TERM before sending KILL."`
}

type JobWaitIn struct {
	Host       string   `json:"host"`
	ID         string   `json:"id,omitempty" jsonschema:"Wait on one job. Use ids to wait on several in a single call."`
	IDs        []string `json:"ids,omitempty" jsonschema:"Wait on several jobs under one shared deadline. Much cheaper than one call per job."`
	WaitAny    bool     `json:"wait_any,omitempty" jsonschema:"With ids, return as soon as any one job finishes instead of waiting for all. Use this to react to the first failure in a batch."`
	TimeoutSec int      `json:"timeout_sec,omitempty" jsonschema:"How long to block, in seconds. Default 300, capped at 3600. If the job is still running when this expires you get timed_out=true and can call again."`
	TailOnExit int      `json:"tail_on_exit,omitempty" jsonschema:"Return this many trailing stdout lines with the final status, saving a follow-up rdev_job_logs call."`
}

type WaitedJobOut struct {
	ID             string           `json:"id"`
	Job            JobOut           `json:"job"`
	Err            string           `json:"err,omitempty" jsonschema:"Why this job could not be waited on, usually an unknown id. Other jobs in the same call still report normally."`
	Logs           string           `json:"logs,omitempty"`
	LogsTruncation proto.Truncation `json:"logs_truncation"`
}

type JobWaitOut struct {
	Job JobOut `json:"job,omitempty" jsonschema:"Set when waiting on a single id."`
	// Waited is set when ids was used.
	Waited         []WaitedJobOut       `json:"waited,omitempty"`
	TimedOut       bool                 `json:"timed_out,omitempty" jsonschema:"True when the wait budget expired while a job was still running. The jobs are unaffected."`
	WaitedMS       int64                `json:"waited_ms"`
	Logs           string               `json:"logs,omitempty"`
	LogsTruncation proto.Truncation     `json:"logs_truncation"`
	OperationID    string               `json:"operation_id"`
	Terminal       bool                 `json:"terminal"`
	ExecutionState proto.ExecutionState `json:"execution_state"`
}

type JobRmIn struct {
	Host         string `json:"host"`
	ID           string `json:"id,omitempty" jsonschema:"Remove this one job. Omit to sweep using the filters below."`
	OlderThanSec int    `json:"older_than_sec,omitempty" jsonschema:"Remove finished jobs that ended more than this many seconds ago."`
	KeepLast     int    `json:"keep_last,omitempty" jsonschema:"Retain this many of the newest finished jobs. With older_than_sec, a job must satisfy both filters to be removed."`
}

type JobRmOut struct {
	Removed      []string `json:"removed,omitempty"`
	RemovedCount int      `json:"removed_count"`
	Skipped      []string `json:"skipped,omitempty" jsonschema:"Jobs left alone because they are still running."`
	Missing      []string `json:"missing,omitempty" jsonschema:"Jobs whose records were already gone. Removal is idempotent, so this is not an error."`
	FreedBytes   int64    `json:"freed_bytes"`
}

type StorageStatusIn struct {
	Host  string `json:"host"`
	Scope string `json:"scope,omitempty" jsonschema:"local or remote_state; defaults to remote_state"`
}
type StorageGCIn struct {
	Host           string `json:"host"`
	Scope          string `json:"scope,omitempty"`
	DryRun         bool   `json:"dry_run,omitempty"`
	MaxScanJobs    int    `json:"max_scan_jobs,omitempty"`
	MaxDeleteJobs  int    `json:"max_delete_jobs,omitempty"`
	MaxDeleteBytes int64  `json:"max_delete_bytes,omitempty"`
}
type StorageDoctorIn struct {
	Host  string `json:"host"`
	Scope string `json:"scope,omitempty"`
}

func registerJobs(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_job_start",
		Description: "Start a long-running command that survives disconnects. " +
			"The process is detached and supervised on the remote host, so its output and exit code are recorded even if this connection drops. " +
			"Use this for batch runs, builds, and test suites instead of rdev_exec. " +
			"Follow with rdev_job_wait to block until it finishes rather than polling rdev_job_status.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobStartIn) (*mcp.CallToolResult, JobOut, error) {
		info, err := c.JobStart(ctx, client.JobStartOptions{
			Host: in.Host, Argv: in.Argv, Cwd: in.Cwd, Env: in.Env,
			LoginShell: in.LoginShell, Label: in.Label,
		})
		if err != nil {
			return nil, JobOut{}, err
		}
		return nil, toJobOut(info), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "rdev_job_list",
		Description: "List jobs on a host, newest first, including ones started by earlier sessions.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobListIn) (*mcp.CallToolResult, JobListOut, error) {
		res, err := c.JobList(ctx, in.Host, in.Limit)
		if err != nil {
			return nil, JobListOut{}, err
		}
		out := JobListOut{
			Jobs:      make([]JobOut, 0, len(res.Jobs)),
			Total:     res.Total,
			Truncated: res.Truncated,
		}
		for _, j := range res.Jobs {
			out.Jobs = append(out.Jobs, toJobOut(j))
		}
		return nil, out, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "rdev_job_status",
		Description: "Report a job's state and exit code.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobRefIn) (*mcp.CallToolResult, JobOut, error) {
		info, err := c.JobStatus(ctx, in.Host, in.ID)
		if err != nil {
			return nil, JobOut{}, err
		}
		return nil, toJobOut(info), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_job_logs",
		Description: "Read a job's output. grep and tail_lines are applied on the remote host, " +
			"so a multi-megabyte log costs only the lines you ask for. " +
			"Order is grep first, then tail: 'matched' counts all grep hits while 'returned' " +
			"counts the lines you actually got.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobLogsIn) (*mcp.CallToolResult, JobLogsOut, error) {
		res, err := c.JobLogs(ctx, client.JobLogsOptions{
			Host: in.Host, ID: in.ID, Stream: in.Stream,
			TailLines: in.TailLines, Grep: in.Grep, SinceOffset: in.SinceOffset,
		})
		if err != nil {
			return nil, JobLogsOut{}, err
		}
		return nil, toJobLogsOut(res), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_job_stop",
		Description: "Signal a job by its recorded process group, which reaches child processes too. " +
			"This is reliable in a way that pkill pattern matching is not.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobStopIn) (*mcp.CallToolResult, JobOut, error) {
		info, err := c.JobStop(ctx, in.Host, in.ID, in.Signal, in.GraceSec)
		if err != nil {
			return nil, JobOut{}, err
		}
		return nil, toJobOut(info), nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_job_wait",
		Description: "Block until a job finishes, then return its final state. " +
			"Prefer this over repeatedly calling rdev_job_status: one call covers a long batch. " +
			"Pass several ids to wait on a whole batch under one deadline instead of one call per job. " +
			"It runs on a separate connection, so other commands to the same host still work while waiting. " +
			"If the job outlives timeout_sec you get timed_out=true and can call again; the job is never affected.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobWaitIn) (*mcp.CallToolResult, JobWaitOut, error) {
		res, err := c.JobWait(ctx, client.JobWaitOptions{
			Host: in.Host, ID: in.ID, IDs: in.IDs, WaitAny: in.WaitAny,
			TimeoutSec: in.TimeoutSec, TailOnExit: in.TailOnExit,
		})
		if err != nil {
			return nil, JobWaitOut{}, err
		}
		out := JobWaitOut{
			TimedOut:       res.TimedOut,
			WaitedMS:       res.WaitedMS,
			Logs:           res.Logs,
			LogsTruncation: res.LogsTruncation, OperationID: res.OperationID,
			Terminal: res.Terminal, ExecutionState: res.Execution,
		}
		for _, w := range res.Waited {
			out.Waited = append(out.Waited, WaitedJobOut{
				ID: w.ID, Job: toJobOut(w.Info), Err: w.Err, Logs: w.Logs,
				LogsTruncation: w.LogsTruncation,
			})
		}
		if res.Info != nil {
			out.Job = toJobOut(res.Info)
		}
		return nil, out, nil
	})
	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_job_rm",
		Description: "Delete job records to reclaim disk. Pass an id to remove one job, " +
			"or older_than_sec / keep_last to sweep finished ones. " +
			"Job logs are unbounded, so a host running batches needs this periodically. " +
			"Running jobs are never removed and come back in 'skipped'.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobRmIn) (*mcp.CallToolResult, JobRmOut, error) {
		res, err := c.JobRm(ctx, client.JobRmOptions{
			Host: in.Host, ID: in.ID,
			OlderThanSec: in.OlderThanSec, KeepLast: in.KeepLast,
		})
		if err != nil {
			return nil, JobRmOut{}, err
		}
		return nil, JobRmOut{
			Removed: res.Removed, Skipped: res.Skipped, Missing: res.Missing,
			FreedBytes: res.FreedBytes, RemovedCount: len(res.Removed),
		}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_storage_status", Description: "Report managed storage usage, budgets, free space, job counts, and pressure state."}, func(ctx context.Context, _ *mcp.CallToolRequest, in StorageStatusIn) (*mcp.CallToolResult, proto.StorageScope, error) {
		res, err := c.StorageStatus(ctx, in.Host, in.Scope)
		if err != nil {
			return nil, proto.StorageScope{}, err
		}
		return nil, *res, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_storage_gc", Description: "Run owner-safe bounded storage reclamation. Use dry_run to preview exactly the candidates; limits cap scan, jobs, and bytes."}, func(ctx context.Context, _ *mcp.CallToolRequest, in StorageGCIn) (*mcp.CallToolResult, proto.StorageGCReport, error) {
		res, err := c.StorageGC(ctx, client.StorageOptions{Host: in.Host, Scope: in.Scope, DryRun: in.DryRun, MaxScanJobs: in.MaxScanJobs, MaxDeleteJobs: in.MaxDeleteJobs, MaxDeleteBytes: in.MaxDeleteBytes})
		if err != nil {
			return nil, proto.StorageGCReport{}, err
		}
		return nil, *res, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_storage_doctor", Description: "Produce a non-mutating, owner-safe storage diagnosis including policy, permissions, stale tombstones/locks, and free-space findings."}, func(ctx context.Context, _ *mcp.CallToolRequest, in StorageDoctorIn) (*mcp.CallToolResult, proto.StorageDoctorReport, error) {
		res, err := c.StorageDoctor(ctx, in.Host, in.Scope)
		if err != nil {
			return nil, proto.StorageDoctorReport{}, err
		}
		return nil, *res, nil
	})
}

func toJobLogsOut(res *proto.JobResult) JobLogsOut {
	if res == nil {
		return JobLogsOut{}
	}
	returned := 0
	if res.Logs != "" {
		returned = strings.Count(res.Logs, "\n") + 1
	}
	return JobLogsOut{
		Logs: res.Logs, NextOffset: res.NextOffset,
		LogSize: res.LogSize, Matched: res.Matched, Returned: returned,
		Truncation: res.LogsTruncation, OperationID: res.OperationID,
		Terminal: res.Terminal, ExecutionState: res.Execution, Ledger: res.LogLedger,
	}
}

// ---------- files ----------
type ReadIn struct {
	Host   string `json:"host"`
	Path   string `json:"path" jsonschema:"Remote path. Supports a leading ~."`
	Offset int64  `json:"offset,omitempty"`
	Limit  int64  `json:"limit,omitempty" jsonschema:"Max bytes to return. Default 65536."`
}

type ReadOut struct {
	Content        string               `json:"content"`
	Base64         bool                 `json:"base64,omitempty" jsonschema:"True when the content is binary and base64-encoded."`
	Size           int64                `json:"size"`
	EOF            bool                 `json:"eof"`
	Truncation     proto.Truncation     `json:"truncation"`
	OperationID    string               `json:"operation_id"`
	Terminal       bool                 `json:"terminal"`
	ExecutionState proto.ExecutionState `json:"execution_state"`
}

type WriteIn struct {
	Host    string `json:"host"`
	Path    string `json:"path"`
	Content string `json:"content"`
	Mode    uint32 `json:"mode,omitempty" jsonschema:"Octal file mode as a decimal number, e.g. 493 for 0755. Default 0644."`
	Append  bool   `json:"append,omitempty"`
}

type WriteOut struct {
	Path           string               `json:"path"`
	BytesWritten   int                  `json:"bytes_written"`
	OperationID    string               `json:"operation_id"`
	Terminal       bool                 `json:"terminal"`
	ExecutionState proto.ExecutionState `json:"execution_state"`
}

type ListIn struct {
	Host  string `json:"host"`
	Path  string `json:"path" jsonschema:"Remote directory. Supports a leading ~."`
	Limit int    `json:"limit,omitempty" jsonschema:"Max entries to return. Default 1000."`
}

type EntryOut struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	Mode    string `json:"mode"`
	IsDir   bool   `json:"is_dir,omitempty"`
	Symlink bool   `json:"symlink,omitempty" jsonschema:"Size and is_dir describe the link itself, not its target."`
	ModTime string `json:"mod_time"`
}

type ListOut struct {
	Path           string               `json:"path"`
	Entries        []EntryOut           `json:"entries"`
	Total          int                  `json:"total" jsonschema:"Entry count before limit was applied."`
	Truncated      bool                 `json:"truncated,omitempty"`
	OperationID    string               `json:"operation_id"`
	Terminal       bool                 `json:"terminal"`
	ExecutionState proto.ExecutionState `json:"execution_state"`
}

const defaultReadLimit = 65536

func registerFiles(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "rdev_read",
		Description: "Read a remote file. Binary content comes back base64-encoded.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ReadIn) (*mcp.CallToolResult, ReadOut, error) {
		limit := in.Limit
		if limit == 0 {
			limit = defaultReadLimit
		}
		res, err := c.ReadFile(ctx, in.Host, in.Path, in.Offset, limit)
		if err != nil {
			return nil, ReadOut{}, err
		}
		return nil, ReadOut{
			Content: res.Content, Base64: res.ContentB64, Size: res.Size, EOF: res.EOF,
			Truncation: res.Truncation, OperationID: res.OperationID,
			Terminal: res.Terminal, ExecutionState: res.Execution,
		}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_write",
		Description: "Write a remote file, creating parent directories. " +
			"Use this instead of echo/heredoc: content is sent as data and is never parsed by a shell.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in WriteIn) (*mcp.CallToolResult, WriteOut, error) {
		res, err := c.WriteFile(ctx, client.WriteFileOptions{
			Host: in.Host, Path: in.Path, Content: in.Content, Mode: in.Mode, Append: in.Append,
		})
		if err != nil {
			return nil, WriteOut{}, err
		}
		return nil, WriteOut{
			Path: res.Path, BytesWritten: res.BytesWritten, OperationID: res.OperationID,
			Terminal: res.Terminal, ExecutionState: res.Execution,
		}, nil
	})

	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_list",
		Description: "List a remote directory as structured entries. " +
			"Prefer this over exec'ing ls: its output format varies by platform and locale, " +
			"and filenames with spaces or newlines make the parse ambiguous.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListIn) (*mcp.CallToolResult, ListOut, error) {
		res, err := c.List(ctx, in.Host, in.Path, in.Limit)
		if err != nil {
			return nil, ListOut{}, err
		}
		out := ListOut{
			Path: res.Path, Total: res.Total, Truncated: res.Truncated,
			OperationID: res.OperationID, Terminal: res.Terminal, ExecutionState: res.Execution,
			Entries: make([]EntryOut, 0, len(res.Entries)),
		}
		for _, e := range res.Entries {
			out.Entries = append(out.Entries, EntryOut{
				Name: e.Name, Size: e.Size, Mode: e.Mode,
				IsDir: e.IsDir, Symlink: e.Symlink, ModTime: e.ModTime,
			})
		}
		return nil, out, nil
	})
}

// ---------- sync ----------

type SyncIn struct {
	Host           string   `json:"host"`
	Direction      string   `json:"direction" jsonschema:"push sends local to remote; pull fetches remote to local."`
	Local          string   `json:"local" jsonschema:"Local path. A trailing slash on a directory copies its contents rather than the directory itself."`
	Remote         string   `json:"remote"`
	Exclude        []string `json:"exclude,omitempty" jsonschema:"rsync exclude patterns, e.g. ['.git','*.pyc','.venv']."`
	DryRun         bool     `json:"dry_run,omitempty" jsonschema:"Show what would transfer without changing anything."`
	Delete         bool     `json:"delete,omitempty" jsonschema:"Delete destination files missing from the source. Destructive: prefer a dry_run first."`
	ConfirmDelete  bool     `json:"confirm_delete,omitempty" jsonschema:"Required acknowledgement for a mutating delete; always preview with dry_run first."`
	SymlinkPolicy  string   `json:"symlink_policy,omitempty" jsonschema:"Symlink handling: preserve (default), follow only within the source root, or skip."`
	ConflictPolicy string   `json:"conflict_policy,omitempty" jsonschema:"Conflict handling: overwrite (default), skip, or fail closed after a bounded preflight."`
	MaxOutputBytes int64    `json:"max_output_bytes,omitempty" jsonschema:"Per-stream stdout/stderr retention cap. Zero uses the bounded default; values may only lower the hard cap."`
}

type SyncOut struct {
	Stdout           string           `json:"stdout"`
	Stderr           string           `json:"stderr,omitempty"`
	StdoutB64        bool             `json:"stdout_b64,omitempty"`
	StderrB64        bool             `json:"stderr_b64,omitempty"`
	StdoutTruncation proto.Truncation `json:"stdout_truncation"`
	StderrTruncation proto.Truncation `json:"stderr_truncation"`
	Truncated        bool             `json:"truncated,omitempty"`
	ExitCode         int              `json:"exit_code"`
	DryRun           bool             `json:"dry_run,omitempty"`
	Command          string           `json:"command"`
	ManifestDigest   string           `json:"manifest_digest,omitempty"`
	ManifestEntries  int              `json:"manifest_entries,omitempty"`
	ManifestComplete bool             `json:"manifest_complete,omitempty"`
	PlanDigest       string           `json:"plan_digest,omitempty"`
}

func registerSync(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "rdev_sync",
		Description: "Transfer files with rsync over the shared ssh connection. Use dry_run before any sync with delete enabled.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in SyncIn) (*mcp.CallToolResult, SyncOut, error) {
		res, err := c.Sync(ctx, client.SyncOptions{
			Host: in.Host, Direction: in.Direction, Local: in.Local, Remote: in.Remote,
			Exclude: in.Exclude, DryRun: in.DryRun, Delete: in.Delete, ConfirmDelete: in.ConfirmDelete,
			SymlinkPolicy: in.SymlinkPolicy, ConflictPolicy: in.ConflictPolicy, MaxOutputBytes: in.MaxOutputBytes,
		})
		if err != nil {
			return nil, SyncOut{}, err
		}
		return nil, SyncOut{
			Stdout: res.Stdout, Stderr: res.Stderr, ExitCode: res.ExitCode,
			StdoutB64: res.StdoutB64, StderrB64: res.StderrB64,
			StdoutTruncation: res.StdoutTruncation, StderrTruncation: res.StderrTruncation,
			Truncated: res.Truncated, DryRun: res.DryRun, Command: res.Command,
			ManifestDigest: res.ManifestDigest, ManifestEntries: res.ManifestEntries,
			ManifestComplete: res.ManifestComplete, PlanDigest: res.PlanDigest,
		}, nil
	})
}

// ---------- session ----------

type SessionIn struct {
	Host string `json:"host,omitempty" jsonschema:"Host to inspect or update. Omit to list all hosts."`

	Addr                 string            `json:"addr,omitempty" jsonschema:"Register or update the ssh destination, e.g. user@1.2.3.4."`
	Port                 int               `json:"port,omitempty"`
	RemoteDir            string            `json:"remote_dir,omitempty" jsonschema:"Directory holding the agent binary and job records. Defaults to ~/.cache/rdev. Changing it starts a fresh job history, since existing jobs live under the old path."`
	Cwd                  string            `json:"cwd,omitempty" jsonschema:"Sticky working directory inherited by later calls on this host."`
	Env                  map[string]string `json:"env,omitempty" jsonschema:"Sticky environment variables merged into later calls."`
	LoginShell           *bool             `json:"login_shell,omitempty" jsonschema:"Default login-shell behaviour for this host."`
	Secrets              map[string]string `json:"secrets,omitempty" jsonschema:"Credential files on this host to register for redaction on first connect, as {\"gftoken\": \"~/.nexus/auth/gongfeng/key\"}. Only the path is saved; the value is read over the connection each session and never reaches local disk."`
	Scope                string            `json:"scope,omitempty" jsonschema:"Where to save: 'project' writes ./.rdev/hosts.json so the host is only visible while working in this directory, 'global' writes ~/.rdev/hosts.json. Defaults to the host's current scope, or project for a new host."`
	Persist              bool              `json:"persist,omitempty" jsonschema:"Save the host registry to the scope's file."`
	ApproveProjectDigest string            `json:"approve_project_digest,omitempty" jsonschema:"Approve and load the current .rdev/hosts.json only when this exact SHA-256 matches. Call with no updates first to inspect the pending path and digest."`
}

type HostOut struct {
	Name       string            `json:"name"`
	Addr       string            `json:"addr"`
	Port       int               `json:"port,omitempty"`
	RemoteDir  string            `json:"remote_dir,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	LoginShell bool              `json:"login_shell"`
	// Secrets reports the configured credential paths, never their values.
	Secrets            map[string]string               `json:"secrets,omitempty"`
	Scope              string                          `json:"scope"`
	Connected          bool                            `json:"connected" jsonschema:"True when a pooled ssh connection to this host is already open, so the next call skips setup."`
	ConnectionSecurity client.ConnectionSecurityStatus `json:"connection_security"`
}

type SessionOut struct {
	Hosts        []HostOut            `json:"hosts"`
	ProjectTrust session.ProjectTrust `json:"project_trust"`
	Security     observe.Snapshot     `json:"security"`
	Saved        bool                 `json:"saved,omitempty"`
	SavedTo      string               `json:"saved_to,omitempty"`
	SavedNote    string               `json:"saved_note,omitempty"`
	Warning      string               `json:"warning,omitempty"`
}

func registerSession(s *mcp.Server, c *client.Client, approveProject func(string) (session.ProjectTrust, error)) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_session",
		Description: "Inspect or update per-host state. Setting cwd once removes the need to repeat it. " +
			"Hosts saved with scope 'project' live in ./.rdev/hosts.json and are only reachable while " +
			"working in that directory. A project file is ignored until approve_project_digest matches its " +
			"reported absolute path and SHA-256; any content change requires a new approval. " +
			"Use 'secrets' to declare credential files that should be registered for redaction automatically on " +
			"every connect, so a token is masked without having to remember rdev_secrets each session.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in SessionIn) (*mcp.CallToolResult, SessionOut, error) {
		boundarySnapshot := c.Secrets.Snapshot()
		approvalWarning := ""
		hasHostFields := in.Addr != "" || in.Port != 0 || in.RemoteDir != "" || in.Cwd != "" ||
			len(in.Env) > 0 || in.LoginShell != nil || len(in.Secrets) > 0 || in.Scope != "" || in.Persist
		if in.Host == "" && hasHostFields {
			return nil, SessionOut{}, errors.New("host is required for session updates")
		}
		if in.ApproveProjectDigest != "" && (in.Host != "" || hasHostFields) {
			return nil, SessionOut{}, errors.New("approve_project_digest must be submitted separately from host updates")
		}
		if in.ApproveProjectDigest != "" {
			if _, err := approveProject(in.ApproveProjectDigest); err != nil {
				warning, committed := session.ConfigWriteCommittedWarning(err)
				if !committed {
					return nil, SessionOut{}, err
				}
				approvalWarning = warning
			}
		}
		scope := session.Scope("")
		setScope := in.Scope != ""
		switch strings.ToLower(in.Scope) {
		case "global":
			scope = session.ScopeGlobal
		case "project":
			scope = session.ScopeProject
		case "":
		default:
			return nil, SessionOut{}, fmt.Errorf("scope must be project or global, got %q", in.Scope)
		}
		if in.Port != 0 && in.Addr == "" {
			return nil, SessionOut{}, errors.New("port requires addr")
		}

		var updateResult session.HostUpdateResult
		if in.Host != "" && hasHostFields {
			update := session.HostUpdate{
				Name: in.Host, Scope: scope, SetScope: setScope, DefaultScope: session.ScopeProject,
				Env: in.Env, LoginShell: in.LoginShell, Secrets: in.Secrets, Persist: in.Persist,
			}
			if in.Addr != "" {
				h := transport.Host{Name: in.Host, Addr: in.Addr, Port: in.Port, RemoteDir: in.RemoteDir}
				update.Host = &h
			} else if in.RemoteDir != "" {
				update.RemoteDir = &in.RemoteDir
			}
			if in.Cwd != "" {
				update.Cwd = &in.Cwd
			}
			var err error
			updateResult, err = c.Hosts.ApplyHostUpdate(update)
			if err != nil {
				warning, committed := session.ConfigWriteCommittedWarning(err)
				if !committed {
					return nil, SessionOut{}, err
				}
				approvalWarning = warning
			}
		}

		out := SessionOut{
			ProjectTrust: c.Hosts.ProjectTrustStatus(),
			Security:     c.Hosts.SecuritySnapshot(),
			Warning:      approvalWarning,
		}
		if updateResult.SavedTo != "" {
			out.Saved = true
			out.SavedTo = updateResult.SavedTo
			if updateResult.Scope == session.ScopeProject {
				out.SavedNote = "visible only when working in this directory"
			} else {
				out.SavedNote = "visible in every project"
			}
		}

		out = secureSessionSnapshot(c, out, in.Host, boundarySnapshot)
		return nil, out, nil
	})
}

// secureSessionSnapshot holds every reported host identity until the complete
// typed result has been recursively redacted. Middleware runs later, after a
// handler returns; relying on it alone lets a concurrent host redefinition purge
// the old value between snapshot construction and redaction.
func secureSessionSnapshot(c *client.Client, out SessionOut, selected string, boundarySnapshot *secrets.Store) SessionOut {
	if boundarySnapshot == nil {
		boundarySnapshot = c.Secrets.Snapshot()
	}
	for {
		names := c.Hosts.Names()
		if selected != "" {
			names = []string{selected}
		}
		type pinned struct {
			resolved session.ResolvedHost
			release  func()
		}
		pins := make([]pinned, 0, len(names))
		valid := true
		for _, name := range names {
			resolved, err := c.Hosts.Resolve(name)
			if err != nil {
				if selected != "" {
					safe := boundarySnapshot.RedactValue(out)
					return c.Secrets.RedactValue(safe).(SessionOut)
				}
				valid = false
				break
			}
			release, ok := c.Hosts.AcquireIdentity(resolved.Host.Name, resolved.Generation, resolved.Fingerprint)
			if !ok {
				valid = false
				break
			}
			pins = append(pins, pinned{resolved: resolved, release: release})
		}
		if !valid {
			for i := len(pins) - 1; i >= 0; i-- {
				pins[i].release()
			}
			continue
		}
		redactionSnapshot := c.Secrets.Snapshot()
		out.Hosts = out.Hosts[:0]
		for _, pin := range pins {
			h := pin.resolved.Host
			st := c.Hosts.State(h.Name)
			out.Hosts = append(out.Hosts, HostOut{
				Name: h.Name, Addr: h.Addr, Port: h.Port, RemoteDir: h.RemoteDir,
				Cwd: st.Cwd, Env: st.Env, LoginShell: st.LoginShell,
				Secrets:            st.Secrets,
				Scope:              string(pin.resolved.Scope),
				Connected:          c.IsConnected(h.Name),
				ConnectionSecurity: c.ConnectionSecurity(h.Name),
			})
		}
		safe := boundarySnapshot.RedactValue(out)
		safe = redactionSnapshot.RedactValue(safe)
		safe = c.Secrets.RedactValue(safe)
		for i := len(pins) - 1; i >= 0; i-- {
			pins[i].release()
		}
		return safe.(SessionOut)
	}
}

// ---------- secrets ----------

type SecretsIn struct {
	Action string `json:"action" jsonschema:"set, set_from_file, list, or delete."`
	Name   string `json:"name,omitempty"`
	Value  string `json:"value,omitempty" jsonschema:"Plaintext value for action=set. Avoid when possible: it puts the secret in the tool call."`
	Path   string `json:"path,omitempty" jsonschema:"File to read for action=set_from_file, e.g. ~/.nexus/auth/gongfeng/key."`
	Host   string `json:"host,omitempty" jsonschema:"Host scope for set, set_from_file, or delete. Hostless values are redaction-only and can never be injected into a remote environment."`
}

type SecretsOut struct {
	Names   []string             `json:"names" jsonschema:"Compatibility list of unique names; use secrets for unambiguous scope and host identity."`
	Secrets []secrets.Descriptor `json:"secrets"`
	Changed bool                 `json:"changed,omitempty"`
	Source  string               `json:"source,omitempty" jsonschema:"Value provenance without credential contents or paths."`
}

func currentSecrets(c *client.Client) SecretsOut {
	return SecretsOut{Names: c.Secrets.Names(), Secrets: c.Secrets.Descriptors()}
}

func registerSecrets(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "rdev_secrets",
		Description: "Register credentials so they are masked in all tool output. " +
			"A host-scoped value is replaced with <redacted:name> everywhere, and can be injected only into that exact host identity's " +
			"environment by passing env {\"VAR\":\"secret:name\"} without ever revealing the plaintext. " +
			"Set host explicitly for injectable credentials. Omitting host is compatible redaction-only registration and never falls back during injection. " +
			"Prefer set_from_file with a host, so the value is read over the connection and never enters " +
			"a tool call, a transcript, or the local disk. Note that a value registered from the local file " +
			"will not mask remote output if the two machines hold different credentials. " +
			"Masking covers the value verbatim and split across lines, but not a fragment or a transformed " +
			"form of it: cutting, re-encoding, or hashing a credential before printing it will leak.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in SecretsIn) (*mcp.CallToolResult, SecretsOut, error) {
		switch strings.ToLower(in.Action) {
		case "set":
			if err := c.SetSecret(in.Host, in.Name, in.Value); err != nil {
				return nil, SecretsOut{}, err
			}
			out := currentSecrets(c)
			out.Changed, out.Source = true, "inline value"
			return nil, out, nil
		case "set_from_file":
			if in.Host != "" {
				if err := c.SetSecretFromRemoteFile(ctx, in.Host, in.Name, in.Path); err != nil {
					return nil, SecretsOut{}, err
				}
				out := currentSecrets(c)
				out.Changed, out.Source = true, "remote host file"
				return nil, out, nil
			}
			if err := c.SetOutputSecretFromFile(in.Name, in.Path); err != nil {
				return nil, SecretsOut{}, err
			}
			out := currentSecrets(c)
			out.Changed, out.Source = true, "local file"
			return nil, out, nil
		case "delete":
			changed, err := c.DeleteSecret(in.Host, in.Name)
			if err != nil {
				return nil, SecretsOut{}, err
			}
			out := currentSecrets(c)
			out.Changed = changed
			return nil, out, nil
		case "list", "":
			return nil, currentSecrets(c), nil
		default:
			return nil, SecretsOut{}, fmt.Errorf("unknown action %q", in.Action)
		}
	})
}
