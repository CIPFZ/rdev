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

	"github.com/tonynyyan/rdev/internal/proto"
	"github.com/tonynyyan/rdev/internal/secrets"
	"github.com/tonynyyan/rdev/internal/session"
	"github.com/tonynyyan/rdev/internal/transport"
)

// AgentLookup resolves an agent build for a remote platform.
type AgentLookup func(goos, goarch string) (*transport.AgentBinary, error)

// Client is the entry point for remote operations.
type Client struct {
	Hosts   *session.Registry
	Secrets *secrets.Store

	lookup AgentLookup

	mu    sync.Mutex
	conns map[string]*transport.Conn
	// waitConns are separate connections used only for blocking job_wait calls.
	//
	// Conn.Do serializes request/response pairs over a single agent pipe, so a
	// multi-minute wait on the shared connection would block every other command
	// to that host. Waiting gets its own pipe so the caller can still inspect
	// logs or run commands while a batch finishes.
	waitConns map[string]*transport.Conn
	// dialing serializes connection setup per host. MCP dispatches tool calls
	// concurrently, and without this several goroutines would bootstrap the same
	// host at once, racing on the agent upload's temp file.
	dialing map[string]*sync.Mutex
}

func New(lookup AgentLookup) *Client {
	return &Client{
		Hosts:     session.NewRegistry(),
		Secrets:   secrets.New(),
		lookup:    lookup,
		conns:     make(map[string]*transport.Conn),
		waitConns: make(map[string]*transport.Conn),
		dialing:   make(map[string]*sync.Mutex),
	}
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
	return conn, nil
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
	return &ExecResult{ExecResult: resp.Exec, Cwd: params.Cwd}, nil
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

func (c *Client) JobList(ctx context.Context, host string) ([]*proto.JobInfo, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpJobList, Job: &proto.JobParams{}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Job == nil {
		return nil, nil
	}
	for _, j := range resp.Job.List {
		c.redactJob(j)
	}
	return resp.Job.List, nil
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
	// TimeoutSec bounds the wait. The agent clamps it to one hour.
	TimeoutSec int
	// TailOnExit returns this many trailing stdout lines with the final status.
	TailOnExit int
}

// JobWaitResult is a finished (or still-running) job plus wait bookkeeping.
type JobWaitResult struct {
	Info     *proto.JobInfo
	TimedOut bool
	WaitedMS int64
	Logs     string
}

// JobWait blocks until a job finishes or the wait budget expires.
//
// It runs on a dedicated connection so it does not stall other commands to the
// same host. A TimedOut result leaves the job untouched; call again to keep
// waiting.
func (c *Client) JobWait(ctx context.Context, opts JobWaitOptions) (*JobWaitResult, error) {
	if opts.ID == "" {
		return nil, errors.New("job id required")
	}

	conn, err := c.waitConn(ctx, opts.Host)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Do(ctx, &proto.Request{
		Op: proto.OpJobWait,
		Job: &proto.JobParams{
			ID:             opts.ID,
			WaitTimeoutSec: opts.TimeoutSec,
			TailOnExit:     opts.TailOnExit,
		},
	})
	if err != nil {
		// Drop the wait connection: a failed blocking call may have left the
		// stream out of sync, and the next wait should start clean.
		c.dropWaitConn(conn, opts.Host)
		return nil, c.redactErr(err)
	}
	if resp.Job == nil || resp.Job.Info == nil {
		return nil, fmt.Errorf("job %s not found", opts.ID)
	}
	return &JobWaitResult{
		Info:     c.redactJob(resp.Job.Info),
		TimedOut: resp.Job.TimedOut,
		WaitedMS: resp.Job.WaitedMS,
		Logs:     c.Secrets.Redact(resp.Job.Logs),
	}, nil
}

// waitConn returns the host's dedicated wait connection, dialing on first use.
func (c *Client) waitConn(ctx context.Context, hostName string) (*transport.Conn, error) {
	host, err := c.Hosts.Host(hostName)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	existing, ok := c.waitConns[host.Name]
	c.mu.Unlock()
	if ok {
		return existing, nil
	}

	// Reuse the same per-host setup lock as the main pool: bootstrap writes a
	// shared remote temp file regardless of which pool the connection lands in.
	lock := c.dialLock(host.Name)
	lock.Lock()
	defer lock.Unlock()

	c.mu.Lock()
	existing, ok = c.waitConns[host.Name]
	c.mu.Unlock()
	if ok {
		return existing, nil
	}

	conn, err := transport.Dial(ctx, host, c.lookup)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.waitConns[host.Name] = conn
	c.mu.Unlock()
	return conn, nil
}

func (c *Client) dropWaitConn(conn *transport.Conn, hostName string) {
	c.mu.Lock()
	if host, err := c.Hosts.Host(hostName); err == nil {
		if c.waitConns[host.Name] == conn {
			delete(c.waitConns, host.Name)
		}
	}
	c.mu.Unlock()
	conn.Close()
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
		Stdout:  c.Secrets.Redact(out.String()),
		Stderr:  c.Secrets.Redact(errBuf.String()),
		DryRun:  opts.DryRun,
		Command: "rsync " + strings.Join(args, " "),
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

// Close tears down all pooled connections.
func (c *Client) Close() {
	c.mu.Lock()
	conns := make([]*transport.Conn, 0, len(c.conns)+len(c.waitConns))
	for _, conn := range c.conns {
		conns = append(conns, conn)
	}
	for _, conn := range c.waitConns {
		conns = append(conns, conn)
	}
	c.conns = make(map[string]*transport.Conn)
	c.waitConns = make(map[string]*transport.Conn)
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
