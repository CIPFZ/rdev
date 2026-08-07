// Package client is the shared core behind both the MCP server and the CLI.
//
// It owns connection pooling, applies session state and secret redaction, and
// exposes one method per operation. Both front ends call these methods, so
// behaviour cannot drift between them.
package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

// AgentLookup resolves an agent build for a remote platform.
type AgentLookup func(goos, goarch string) (*transport.AgentBinary, error)

// Client is the entry point for remote operations.
type Client struct {
	Hosts   *session.Registry
	Secrets *secrets.Store

	lookup AgentLookup

	// warn reports a non-fatal problem. It exists so the quiet paths -- notably a
	// credential file that could not be read -- are observable in a test rather
	// than only on a terminal. Nil means os.Stderr.
	warn func(format string, args ...any)

	mu    sync.Mutex
	conns map[string]*transport.Conn
	// dialing serializes connection setup per host. MCP dispatches tool calls
	// concurrently, and without this several goroutines would bootstrap the same
	// host at once, racing on the agent upload's temp file.
	dialing map[string]*sync.Mutex
}

func New(lookup AgentLookup) *Client {
	return &Client{
		Hosts:   session.NewRegistry(),
		Secrets: secrets.New(),
		lookup:  lookup,
		conns:   make(map[string]*transport.Conn),
		dialing: make(map[string]*sync.Mutex),
	}
}

// warnf reports a non-fatal problem, defaulting to stderr.
//
// Stderr rather than the response: an unreadable credential must not fail the
// call, but it must not vanish either, and stdout carries the MCP stream.
func (c *Client) warnf(format string, args ...any) {
	if c.warn != nil {
		c.warn(format, args...)
		return
	}
	fmt.Fprintf(os.Stderr, format, args...)
}

// dialLock returns the per-host setup mutex, creating it on first use.
func (c *Client) dialLock(name string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	m, ok := c.dialing[name]
	if !ok {
		m = &sync.Mutex{}
		c.dialing[name] = m
	}
	return m
}

// conn returns a pooled connection, dialing on first use.
func (c *Client) conn(ctx context.Context, hostName string) (*transport.Conn, error) {
	host, err := c.Hosts.Host(hostName)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	existing, ok := c.conns[host.Name]
	c.mu.Unlock()
	if ok {
		return existing, nil
	}

	// Serialize setup for this host: bootstrap writes a shared temp file on the
	// remote, so two concurrent dials would clobber each other.
	lock := c.dialLock(host.Name)
	lock.Lock()
	defer lock.Unlock()

	// Re-check under the lock: another goroutine may have finished dialing
	// while we waited, in which case we reuse its connection.
	c.mu.Lock()
	existing, ok = c.conns[host.Name]
	c.mu.Unlock()
	if ok {
		return existing, nil
	}

	conn, err := transport.Dial(ctx, host, c.lookup)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.conns[host.Name] = conn
	c.mu.Unlock()

	// Register this host's declared credential files before returning the
	// connection, so the very first command already has redaction in place. Doing
	// it lazily would leave a window where a token could be echoed verbatim.
	c.loadHostSecrets(ctx, host.Name)
	return conn, nil
}

// loadHostSecrets reads the credential files a host declares and registers them
// for redaction.
//
// Only paths are persisted, so the plaintext is fetched over the agent connection
// and never touches local disk -- that is what makes an in-memory store workable
// across sessions instead of requiring a manual re-register every time.
//
// Failures are deliberately quiet: a missing or unreadable credential file must not
// stop the host from being usable, and the caller learns about it from the
// unredacted output rather than from a failed dial. Already-registered names are
// left alone so an explicit rdev_secrets call wins over the config.
func (c *Client) loadHostSecrets(ctx context.Context, hostName string) {
	st := c.Hosts.State(hostName)
	if len(st.Secrets) == 0 {
		return
	}
	for name, path := range st.Secrets {
		if name == "" || path == "" {
			continue
		}
		if _, exists := c.Secrets.Get(name); exists {
			continue
		}
		if err := c.SetSecretFromRemoteFile(ctx, hostName, name, path); err != nil {
			// Surfaced rather than swallowed: an unredacted credential is worth a
			// warning, and stderr does not corrupt the MCP stdout stream.
			c.warnf("rdev: warning: secret %q from %s:%s not registered: %v\n",
				name, hostName, path, err)
		}
	}
}

// do sends a request, retrying once on transport failure.
//
// A pooled connection can be dead for benign reasons: ControlPersist expired,
// the network blipped, the remote rebooted. Retrying once turns that into a
// hiccup instead of an error the caller has to interpret.
func (c *Client) do(ctx context.Context, hostName string, req *proto.Request) (*proto.Response, error) {
	conn, err := c.conn(ctx, hostName)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Do(ctx, req)
	if err == nil {
		return resp, nil
	}
	// A remote-reported error is a real answer, not a broken pipe: return it.
	if resp != nil {
		return resp, err
	}
	if ctx.Err() != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.conns[conn.Host().Name] == conn {
		delete(c.conns, conn.Host().Name)
	}
	c.mu.Unlock()
	conn.Close()

	fresh, dialErr := c.conn(ctx, hostName)
	if dialErr != nil {
		return nil, fmt.Errorf("%w (reconnect failed: %v)", err, dialErr)
	}
	return fresh.Do(ctx, req)
}

// ExecOptions describes a foreground command.
type ExecOptions struct {
	Host           string
	Argv           []string
	Cwd            string
	Env            map[string]string
	LoginShell     *bool // nil inherits the host's session default
	Stdin          string
	TimeoutSec     int
	MaxOutputBytes int
}

// ExecResult is a redacted ExecResult plus the effective working directory.
type ExecResult struct {
	*proto.ExecResult
	Cwd string
}

// Exec runs a command and waits for it.
func (c *Client) Exec(ctx context.Context, opts ExecOptions) (*ExecResult, error) {
	if len(opts.Argv) == 0 {
		return nil, errors.New("argv must not be empty")
	}

	params, err := c.buildExecParams(opts.Host, opts.Argv, opts.Cwd, opts.Env, opts.LoginShell)
	if err != nil {
		return nil, err
	}
	params.Stdin = opts.Stdin
	params.TimeoutSec = opts.TimeoutSec
	params.MaxOutputBytes = opts.MaxOutputBytes

	resp, err := c.do(ctx, opts.Host, &proto.Request{Op: proto.OpExec, Exec: params})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Exec == nil {
		return nil, errors.New("agent returned no exec result")
	}

	resp.Exec.Stdout = c.Secrets.Redact(resp.Exec.Stdout)
	resp.Exec.Stderr = c.Secrets.Redact(resp.Exec.Stderr)
	// Cwd too: it is echoed back from the request, and a path under a credential
	// directory is a plausible way for a registered value to appear here.
	return &ExecResult{ExecResult: resp.Exec, Cwd: c.Secrets.Redact(params.Cwd)}, nil
}

// buildExecParams layers session state under per-call values and resolves any
// "secret:NAME" env references.
func (c *Client) buildExecParams(host string, argv []string, cwd string, env map[string]string, login *bool) (*proto.ExecParams, error) {
	st := c.Hosts.State(host)

	effCwd := cwd
	if effCwd == "" {
		effCwd = st.Cwd
	}
	effLogin := st.LoginShell
	if login != nil {
		effLogin = *login
	}

	merged := session.MergeEnv(st.Env, env)
	resolved, err := c.Secrets.ResolveEnv(merged)
	if err != nil {
		return nil, err
	}

	return &proto.ExecParams{
		Argv:       argv,
		Cwd:        effCwd,
		Env:        resolved,
		LoginShell: effLogin,
	}, nil
}

// redactErr scrubs secrets from an error message. Remote errors can quote the
// failing request, which may include a resolved credential.
func (c *Client) redactErr(err error) error {
	if err == nil {
		return nil
	}
	msg := c.Secrets.Redact(err.Error())
	if msg == err.Error() {
		return err
	}
	return errors.New(msg)
}

// JobStartOptions describes a detached job.
type JobStartOptions struct {
	Host       string
	Argv       []string
	Cwd        string
	Env        map[string]string
	LoginShell *bool
	Label      string
}

// JobStart launches a job that outlives the connection.
func (c *Client) JobStart(ctx context.Context, opts JobStartOptions) (*proto.JobInfo, error) {
	if len(opts.Argv) == 0 {
		return nil, errors.New("argv must not be empty")
	}
	params, err := c.buildExecParams(opts.Host, opts.Argv, opts.Cwd, opts.Env, opts.LoginShell)
	if err != nil {
		return nil, err
	}

	resp, err := c.do(ctx, opts.Host, &proto.Request{
		Op:  proto.OpJobStart,
		Job: &proto.JobParams{Spec: params, Label: opts.Label},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil || resp.Job.Info == nil {
		return nil, errors.New("agent returned no job info")
	}
	return c.redactJob(resp.Job.Info), nil
}

// redactJob scrubs the recorded argv, which may contain a credential passed as
// a command-line flag.
func (c *Client) redactJob(j *proto.JobInfo) *proto.JobInfo {
	if j == nil {
		return nil
	}
	for i, a := range j.Argv {
		j.Argv[i] = c.Secrets.Redact(a)
	}
	return j
}

// JobListResult is a page of jobs plus how many exist in total.
type JobListResult struct {
	Jobs      []*proto.JobInfo
	Total     int
	Truncated bool
}

// JobList reports jobs, newest first, bounded by limit.
//
// The limit is applied on the remote side before metadata is read, so listing a
// host that has accumulated thousands of jobs stays cheap.
func (c *Client) JobList(ctx context.Context, host string, limit int) (*JobListResult, error) {
	resp, err := c.do(ctx, host, &proto.Request{
		Op:  proto.OpJobList,
		Job: &proto.JobParams{Limit: limit},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil {
		return &JobListResult{}, nil
	}
	for _, j := range resp.Job.List {
		c.redactJob(j)
	}
	return &JobListResult{
		Jobs:      resp.Job.List,
		Total:     resp.Job.Total,
		Truncated: resp.Job.Truncated,
	}, nil
}

func (c *Client) JobStatus(ctx context.Context, host, id string) (*proto.JobInfo, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: id}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil || resp.Job.Info == nil {
		return nil, fmt.Errorf("job %s not found", id)
	}
	return c.redactJob(resp.Job.Info), nil
}

// JobLogsOptions selects a slice of a job's output.
type JobLogsOptions struct {
	Host        string
	ID          string
	Stream      string
	TailLines   int
	Grep        string
	SinceOffset int64
}

func (c *Client) JobLogs(ctx context.Context, opts JobLogsOptions) (*proto.JobResult, error) {
	resp, err := c.do(ctx, opts.Host, &proto.Request{
		Op: proto.OpJobLogs,
		Job: &proto.JobParams{
			ID:          opts.ID,
			Stream:      opts.Stream,
			TailLines:   opts.TailLines,
			Grep:        opts.Grep,
			SinceOffset: opts.SinceOffset,
		},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil {
		return nil, errors.New("agent returned no logs")
	}
	resp.Job.Logs = c.Secrets.Redact(resp.Job.Logs)
	return resp.Job, nil
}

func (c *Client) JobStop(ctx context.Context, host, id, signal string, graceSec int) (*proto.JobInfo, error) {
	resp, err := c.do(ctx, host, &proto.Request{
		Op:  proto.OpJobStop,
		Job: &proto.JobParams{ID: id, Signal: signal, GraceSec: graceSec},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil || resp.Job.Info == nil {
		return nil, fmt.Errorf("job %s not found", id)
	}
	return c.redactJob(resp.Job.Info), nil
}

// JobWaitOptions bounds a blocking wait.
type JobWaitOptions struct {
	Host string
	ID   string
	// IDs waits on several jobs in one call. Takes precedence over ID.
	IDs []string
	// WaitAny returns as soon as one of IDs finishes rather than all of them.
	WaitAny bool
	// TimeoutSec bounds the wait. The agent clamps it to one hour.
	TimeoutSec int
	// TailOnExit returns this many trailing stdout lines with the final status.
	TailOnExit int
}

// WaitedJob is one job's outcome in a multi-job wait.
type WaitedJob struct {
	ID   string
	Info *proto.JobInfo
	Err  string
	Logs string
}

// JobWaitResult is a finished (or still-running) job plus wait bookkeeping.
type JobWaitResult struct {
	Info     *proto.JobInfo
	TimedOut bool
	WaitedMS int64
	Logs     string
	// Waited is populated instead of Info when several ids were requested.
	Waited []WaitedJob
}

// JobWait blocks until a job finishes or the wait budget expires.
//
// It shares the host's pooled connection: requests are multiplexed by ID, so a
// multi-minute wait no longer blocks other commands and needs no separate pipe.
// A TimedOut result leaves the job untouched; call again to keep waiting.
//
// With several ids, one call covers the batch under a shared deadline rather than
// costing one blocking round trip per job.
func (c *Client) JobWait(ctx context.Context, opts JobWaitOptions) (*JobWaitResult, error) {
	if opts.ID == "" && len(opts.IDs) == 0 {
		return nil, errors.New("job id required")
	}

	resp, err := c.do(ctx, opts.Host, &proto.Request{
		Op: proto.OpJobWait,
		Job: &proto.JobParams{
			ID:             opts.ID,
			IDs:            opts.IDs,
			WaitAny:        opts.WaitAny,
			WaitTimeoutSec: opts.TimeoutSec,
			TailOnExit:     opts.TailOnExit,
		},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil {
		return nil, errors.New("agent returned no wait result")
	}

	out := &JobWaitResult{
		TimedOut: resp.Job.TimedOut,
		WaitedMS: resp.Job.WaitedMS,
		Logs:     c.Secrets.Redact(resp.Job.Logs),
	}
	if len(resp.Job.Waited) > 0 {
		for _, w := range resp.Job.Waited {
			out.Waited = append(out.Waited, WaitedJob{
				ID:   w.ID,
				Info: c.redactJob(w.Info),
				Err:  c.Secrets.Redact(w.Err),
				Logs: c.Secrets.Redact(w.Logs),
			})
		}
		return out, nil
	}
	if resp.Job.Info == nil {
		return nil, fmt.Errorf("job %s not found", opts.ID)
	}
	out.Info = c.redactJob(resp.Job.Info)
	return out, nil
}

// SetSecretFromRemoteFile registers a secret read from a file on a remote host.
//
// Without this, registering a remote credential means copying it locally first
// and deciding where the plaintext lands. The value is read over the agent
// connection and goes straight into the store, so it never reaches a tool
// result, a transcript, or the local filesystem.
func (c *Client) SetSecretFromRemoteFile(ctx context.Context, host, name, path string) error {
	if name == "" {
		return errors.New("secret name required")
	}
	if path == "" {
		return errors.New("path required")
	}

	// Read directly rather than via c.ReadFile: that method redacts its result,
	// which would corrupt a value that happens to contain an existing secret.
	resp, err := c.do(ctx, host, &proto.Request{
		Op:   proto.OpReadFile,
		Read: &proto.ReadParams{Path: path, Limit: maxSecretFileBytes},
	})
	if err != nil {
		return c.redactErr(err)
	}
	if resp.Read == nil {
		return errors.New("agent returned no content")
	}
	if resp.Read.ContentB64 {
		return fmt.Errorf("%s looks binary; a credential file should be text", path)
	}

	// Credential files usually end with a newline, and sending it along breaks
	// HTTP headers in confusing ways.
	value := strings.TrimSpace(resp.Read.Content)
	if value == "" {
		return fmt.Errorf("%s on %s is empty", path, host)
	}
	return c.Secrets.Set(name, value)
}

// maxSecretFileBytes bounds a credential read. Tokens and keys are small; a
// larger file is a sign the wrong path was given.
const maxSecretFileBytes = 64 << 10

func (c *Client) ReadFile(ctx context.Context, host, path string, offset, limit int64) (*proto.ReadResult, error) {
	resp, err := c.do(ctx, host, &proto.Request{
		Op:   proto.OpReadFile,
		Read: &proto.ReadParams{Path: path, Offset: offset, Limit: limit},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Read == nil {
		return nil, errors.New("agent returned no content")
	}
	// Only redact text: base64 payloads would be corrupted by substitution, and
	// a secret is not recognizable inside them anyway.
	if !resp.Read.ContentB64 {
		resp.Read.Content = c.Secrets.Redact(resp.Read.Content)
	}
	return resp.Read, nil
}

// WriteFileOptions describes a remote write.
type WriteFileOptions struct {
	Host    string
	Path    string
	Content string
	Mode    uint32
	Append  bool
}

func (c *Client) WriteFile(ctx context.Context, opts WriteFileOptions) (*proto.WriteResult, error) {
	resp, err := c.do(ctx, opts.Host, &proto.Request{
		Op: proto.OpWriteFile,
		Cat: &proto.WriteParams{
			Path:    opts.Path,
			Content: opts.Content,
			Mode:    opts.Mode,
			Append:  opts.Append,
		},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Cat == nil {
		return nil, errors.New("agent returned no write result")
	}
	return resp.Cat, nil
}

// SyncOptions describes an rsync transfer.
type SyncOptions struct {
	Host      string
	Direction string // "push" (local->remote) or "pull" (remote->local)
	Local     string
	Remote    string
	Exclude   []string
	DryRun    bool
	Delete    bool
}

// SyncResult reports rsync's outcome.
type SyncResult struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
	DryRun   bool   `json:"dry_run,omitempty"`
	Command  string `json:"command"`
}

// Sync transfers files with rsync over the multiplexed ssh connection.
//
// rsync runs locally rather than through the agent protocol: it already solves
// delta transfer and permissions, and reimplementing that would be strictly
// worse. Reusing the ControlMaster socket keeps it from re-authenticating.
func (c *Client) Sync(ctx context.Context, opts SyncOptions) (*SyncResult, error) {
	if opts.Local == "" || opts.Remote == "" {
		return nil, errors.New("local and remote paths required")
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		return nil, errors.New("rsync not found on the local host")
	}

	// Dial first so the ControlMaster exists and the remote host is validated
	// before rsync tries to use the socket.
	conn, err := c.conn(ctx, opts.Host)
	if err != nil {
		return nil, err
	}

	sshCmd := append([]string{"ssh"}, conn.SSHArgs()...)
	// Only long-standing flags: macOS ships openrsync, which rejects newer
	// options like --info=stats1 that samba rsync accepts. -v gives a
	// transferred-file list on both implementations.
	args := []string{"-az", "-v", "-e", strings.Join(sshCmd, " ")}
	if opts.DryRun {
		args = append(args, "--dry-run")
	}
	if opts.Delete {
		args = append(args, "--delete")
	}
	for _, ex := range opts.Exclude {
		args = append(args, "--exclude", ex)
	}

	remoteSpec := conn.Host().Addr + ":" + opts.Remote
	switch opts.Direction {
	case "push", "":
		args = append(args, opts.Local, remoteSpec)
	case "pull":
		args = append(args, remoteSpec, opts.Local)
	default:
		return nil, fmt.Errorf("direction must be push or pull, got %q", opts.Direction)
	}

	cmd := exec.CommandContext(ctx, "rsync", args...)
	var out, errBuf strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	runErr := cmd.Run()

	res := &SyncResult{
		Stdout: c.Secrets.Redact(out.String()),
		Stderr: c.Secrets.Redact(errBuf.String()),
		DryRun: opts.DryRun,
		// Redacted like the streams above. This echoes the assembled argv, and argv
		// is caller-supplied: an --exclude pattern or a path can carry a credential.
		// Leaving one field of the same struct unscrubbed is exactly the accident
		// decision 6 exists to prevent -- redaction has to be at the boundary, not
		// per field, or the next field added inherits the gap.
		Command: c.Secrets.Redact("rsync " + strings.Join(args, " ")),
	}
	var ee *exec.ExitError
	if errors.As(runErr, &ee) {
		res.ExitCode = ee.ExitCode()
	} else if runErr != nil {
		return nil, fmt.Errorf("run rsync: %w", runErr)
	}
	return res, nil
}

// Ping verifies connectivity and reports agent identity.
func (c *Client) Ping(ctx context.Context, host string) (*proto.PingResult, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpPing})
	if err != nil {
		return nil, c.redactErr(err)
	}
	return resp.Ping, nil
}

// JobRmOptions selects jobs to delete. Either ID, or a filtered sweep.
type JobRmOptions struct {
	Host string
	// ID removes one job. When empty, the filters below drive a sweep.
	ID string
	// OlderThanSec removes finished jobs that ended more than this long ago.
	OlderThanSec int
	// KeepLast retains this many of the newest finished jobs. Combined with
	// OlderThanSec, a job must satisfy both filters to be removed.
	KeepLast int
}

// JobRmResult reports what a removal freed.
type JobRmResult struct {
	Removed    []string
	Skipped    []string
	FreedBytes int64
}

// JobRm deletes job records to reclaim disk.
//
// Job logs are unbounded, so a machine running batches accumulates them until the
// disk fills. Running jobs are never removed; they come back in Skipped.
func (c *Client) JobRm(ctx context.Context, opts JobRmOptions) (*JobRmResult, error) {
	if opts.ID == "" && opts.OlderThanSec <= 0 && opts.KeepLast <= 0 {
		return nil, errors.New("job_rm needs an id, older_than_sec, or keep_last")
	}
	resp, err := c.do(ctx, opts.Host, &proto.Request{
		Op: proto.OpJobRm,
		Job: &proto.JobParams{
			ID:           opts.ID,
			OlderThanSec: opts.OlderThanSec,
			KeepLast:     opts.KeepLast,
		},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil {
		return nil, errors.New("agent returned no removal result")
	}
	return &JobRmResult{
		Removed:    resp.Job.Removed,
		Skipped:    resp.Job.Skipped,
		FreedBytes: resp.Job.FreedBytes,
	}, nil
}

// List reads a remote directory as structured entries.
//
// Prefer this over exec'ing `ls`: output format varies by platform and locale,
// and filenames containing spaces or newlines make the parse ambiguous.
func (c *Client) List(ctx context.Context, host, path string, limit int) (*proto.ListResult, error) {
	resp, err := c.do(ctx, host, &proto.Request{
		Op:   proto.OpList,
		List: &proto.ListParams{Path: path, Limit: limit},
	})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.List == nil {
		return nil, errors.New("agent returned no listing")
	}
	// A path can carry a credential (a token in a directory name), and so can a
	// filename, so redact both.
	resp.List.Path = c.Secrets.Redact(resp.List.Path)
	for i := range resp.List.Entries {
		resp.List.Entries[i].Name = c.Secrets.Redact(resp.List.Entries[i].Name)
	}
	return resp.List, nil
}

// IsConnected reports whether a pooled connection to the host is already open.
//
// Callers use this to show whether the next request pays setup cost. It does not
// probe the network: a connection can be pooled but dead, which `do` handles by
// reconnecting on first failure.
//
// Deliberately does not resolve through Hosts.Host: that auto-registers anything
// shaped like an ssh destination, so a status query on a typo'd name would leave
// a permanent phantom host in the listing.
func (c *Client) IsConnected(hostName string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.conns[hostName]
	return ok
}

// Disconnect closes and forgets a host's pooled connection.
//
// Needed when a host's definition changes: the open agent session was started
// against the old address and state directory, so reusing it would silently
// apply stale settings. Reports whether anything was open.
//
// Like IsConnected, this looks the name up in the pool directly rather than
// through Hosts.Host, which would auto-register an unknown ssh-style name.
// Both are only called with names that are already registered.
func (c *Client) Disconnect(hostName string) bool {
	c.mu.Lock()
	conn, ok := c.conns[hostName]
	if ok {
		delete(c.conns, hostName)
	}
	c.mu.Unlock()

	if !ok {
		return false
	}
	conn.Close()
	return true
}

// Close tears down all pooled connections.
func (c *Client) Close() {
	c.mu.Lock()
	conns := make([]*transport.Conn, 0, len(c.conns))
	for _, conn := range c.conns {
		conns = append(conns, conn)
	}
	c.conns = make(map[string]*transport.Conn)
	c.mu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}

// LocalHostname is used in log lines to identify this machine.
func LocalHostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "localhost"
	}
	return h
}
