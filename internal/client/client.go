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
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

// AgentLookup resolves an agent build for a remote platform.
type AgentLookup func(goos, goarch string) (*transport.AgentBinary, error)

type remoteConnection interface {
	Do(context.Context, *proto.Request) (*proto.Response, error)
	Host() transport.Host
	SSHArgs() []string
	Close() error
}

type pooledConnection struct {
	conn                  remoteConnection
	fingerprint           string
	connectionFingerprint string
	generation            uint64
	scope                 secrets.Scope
	host                  secrets.HostIdentity
}

// ConnectionSecurityStatus is the externally visible security initialization
// state for one alias. A connection is reusable only in the ready state.
type ConnectionSecurityStatus struct {
	State      observe.ConnectionSecurityState `json:"state"`
	Generation uint64                          `json:"generation,omitempty"`
	Declared   int                             `json:"declared_secrets"`
	Loaded     int                             `json:"loaded_secrets"`
	Reason     observe.SecretReason            `json:"reason,omitempty"`
}

type dialFunc func(context.Context, transport.Host, AgentLookup) (remoteConnection, error)
type rsyncRunner func(context.Context, []string) (string, string, error)

// Client is the entry point for remote operations.
type Client struct {
	Hosts   *session.Registry
	Secrets *secrets.Store

	lookup AgentLookup
	dial   dialFunc
	rsync  rsyncRunner

	mu       sync.Mutex
	conns    map[string]pooledConnection
	security map[string]ConnectionSecurityStatus
	// dialing serializes connection setup per host. MCP dispatches tool calls
	// concurrently, and without this several goroutines would bootstrap the same
	// host at once, racing on the agent upload's temp file.
	dialing map[string]*sync.Mutex
}

func New(lookup AgentLookup) *Client {
	c := &Client{
		Hosts:   session.NewRegistry(),
		Secrets: secrets.New(),
		lookup:  lookup,
		dial: func(ctx context.Context, host transport.Host, lookup AgentLookup) (remoteConnection, error) {
			return transport.Dial(ctx, host, lookup)
		},
		conns:    make(map[string]pooledConnection),
		security: make(map[string]ConnectionSecurityStatus),
		dialing:  make(map[string]*sync.Mutex),
	}
	c.Hosts.SetHostChangeHook(c.invalidateHost)
	c.Secrets.SetRedactionHook(c.Hosts.RecordRedactionHit)
	return c
}

func (c *Client) invalidateHost(name string, generation uint64) {
	c.Disconnect(name)
	if resolved, err := c.Hosts.Resolve(name); err == nil {
		c.Secrets.DeleteStaleHost(secrets.Scope(resolved.Scope), secretHostIdentity(resolved))
	} else {
		c.Secrets.DeleteHost(name)
	}
	c.setConnectionSecurity(name, ConnectionSecurityStatus{State: observe.SecurityCold, Generation: generation})
}

func (c *Client) setConnectionSecurity(name string, status ConnectionSecurityStatus) {
	c.mu.Lock()
	previous := c.security[name]
	c.security[name] = status
	c.mu.Unlock()
	if previous.State != status.State || previous.Generation != status.Generation {
		c.Hosts.RecordConnectionSecurityState(status.State, name)
	}
}

func (c *Client) ConnectionSecurity(host string) ConnectionSecurityStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	if status, ok := c.security[host]; ok {
		return status
	}
	return ConnectionSecurityStatus{State: observe.SecurityCold}
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
func (c *Client) conn(ctx context.Context, hostName string) (remoteConnection, error) {
	resolved, err := c.Hosts.Resolve(hostName)
	if err != nil {
		return nil, err
	}

	// Serialize setup for this host: bootstrap writes a shared temp file on the
	// remote, so two concurrent dials would clobber each other.
	lock := c.dialLock(resolved.Host.Name)
	lock.Lock()
	defer lock.Unlock()

	for {
		resolved, err = c.Hosts.Resolve(hostName)
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		existing, ok := c.conns[resolved.Host.Name]
		if ok && existing.generation == resolved.Generation && existing.fingerprint == resolved.Fingerprint && existing.connectionFingerprint == resolved.ConnectionFingerprint {
			c.mu.Unlock()
			return existing.conn, nil
		}
		if ok {
			delete(c.conns, resolved.Host.Name)
		}
		c.mu.Unlock()
		if ok {
			_ = existing.conn.Close()
		}

		c.mu.Lock()
		failed := c.security[resolved.Host.Name]
		c.mu.Unlock()
		if failed.State == observe.SecurityFailed && failed.Generation == resolved.Generation {
			return nil, fmt.Errorf("connection security initialization failed (%s); update the host secret declaration or explicitly register the scoped value", failed.Reason)
		}

		releaseIdentity, acquired := c.Hosts.AcquireIdentity(resolved.Host.Name, resolved.Generation, resolved.Fingerprint)
		if !acquired {
			continue
		}
		st := c.Hosts.State(resolved.Host.Name)
		if err := secrets.ValidateDeclarations(st.Secrets); err != nil {
			releaseIdentity()
			c.Hosts.RecordSecretLoadFailure(observe.ReasonSecretInvalid, resolved.Host.Name)
			c.setConnectionSecurity(resolved.Host.Name, ConnectionSecurityStatus{
				State: observe.SecurityFailed, Generation: resolved.Generation,
				Declared: len(st.Secrets), Reason: observe.ReasonSecretInvalid,
			})
			return nil, errors.New("connection security initialization failed (invalid secret declaration)")
		}
		status := ConnectionSecurityStatus{
			State: observe.SecurityInitializing, Generation: resolved.Generation,
			Declared: len(st.Secrets),
		}
		c.setConnectionSecurity(resolved.Host.Name, status)

		conn, dialErr := c.dial(ctx, resolved.Host, c.lookup)
		if dialErr != nil {
			releaseIdentity()
			if len(st.Secrets) > 0 {
				// Declared values are intentionally not available until the secure
				// connection can read them. Bootstrap diagnostics may nevertheless
				// echo one (for example from a remote shell profile), so returning the
				// raw dial error here would create an unredactable pre-init leak.
				c.Hosts.RecordSecretLoadFailure(observe.ReasonSecretReadFailed, resolved.Host.Name)
				c.setConnectionSecurity(resolved.Host.Name, ConnectionSecurityStatus{
					State: observe.SecurityFailed, Generation: resolved.Generation,
					Declared: len(st.Secrets), Reason: observe.ReasonSecretReadFailed,
				})
				return nil, errors.New("connection setup failed before declared secrets could be protected")
			}
			c.setConnectionSecurity(resolved.Host.Name, ConnectionSecurityStatus{State: observe.SecurityCold, Generation: resolved.Generation})
			return nil, dialErr
		}
		loaded, reason, loadErr := c.loadHostSecrets(ctx, resolved, st, conn)
		if loadErr != nil {
			_ = conn.Close()
			releaseIdentity()
			c.Hosts.RecordSecretLoadFailure(reason, resolved.Host.Name)
			c.setConnectionSecurity(resolved.Host.Name, ConnectionSecurityStatus{
				State: observe.SecurityFailed, Generation: resolved.Generation,
				Declared: len(st.Secrets), Loaded: loaded, Reason: reason,
			})
			return nil, fmt.Errorf("connection security initialization failed (%s)", reason)
		}

		hostIdentity := secretHostIdentity(resolved)
		pooled := pooledConnection{
			conn: conn, fingerprint: resolved.Fingerprint, connectionFingerprint: resolved.ConnectionFingerprint, generation: resolved.Generation,
			scope: secrets.Scope(resolved.Scope), host: hostIdentity,
		}
		// Publication is the commit point: all declared secrets are present and
		// the identity read lease still prevents a redefinition.
		c.mu.Lock()
		c.conns[resolved.Host.Name] = pooled
		c.mu.Unlock()
		c.setConnectionSecurity(resolved.Host.Name, ConnectionSecurityStatus{
			State: observe.SecurityReady, Generation: resolved.Generation,
			Declared: len(st.Secrets), Loaded: loaded,
		})
		releaseIdentity()
		return conn, nil
	}
}

func secretHostIdentity(resolved session.ResolvedHost) secrets.HostIdentity {
	return secrets.HostIdentity{Alias: resolved.Host.Name, Fingerprint: resolved.Fingerprint, Generation: resolved.Generation}
}

func (c *Client) leasedConn(ctx context.Context, hostName string) (pooledConnection, session.State, func(), error) {
	for {
		conn, err := c.conn(ctx, hostName)
		if err != nil {
			return pooledConnection{}, session.State{}, nil, err
		}
		name := conn.Host().Name
		c.mu.Lock()
		pooled, ok := c.conns[name]
		c.mu.Unlock()
		if !ok || pooled.conn != conn {
			continue
		}
		release, ok := c.Hosts.AcquireIdentity(name, pooled.generation, pooled.fingerprint)
		if !ok {
			continue
		}
		c.mu.Lock()
		current, stillPublished := c.conns[name]
		c.mu.Unlock()
		if !stillPublished || current.conn != conn || current.generation != pooled.generation {
			release()
			continue
		}
		return pooled, c.Hosts.State(name), release, nil
	}
}

// loadHostSecrets reads the credential files a host declares and registers them
// for redaction.
//
// Only paths are persisted, so the plaintext is fetched over the agent connection
// and never touches local disk -- that is what makes an in-memory store workable
// across sessions instead of requiring a manual re-register every time.
//
// Initialization is fail-closed: every declared value must be present before the
// connection can be published. Already-registered values for this exact immutable
// identity are left alone so an explicit rdev_secrets call wins over config.
func (c *Client) loadHostSecrets(ctx context.Context, resolved session.ResolvedHost, st session.State, conn remoteConnection) (int, observe.SecretReason, error) {
	if len(st.Secrets) == 0 {
		return 0, "", nil
	}
	hostIdentity := secretHostIdentity(resolved)
	scope := secrets.Scope(resolved.Scope)
	names := make([]string, 0, len(st.Secrets))
	for name := range st.Secrets {
		names = append(names, name)
	}
	sort.Strings(names)
	pending := make(map[secrets.Key]string)
	loaded := 0
	for _, name := range names {
		path := st.Secrets[name]
		key := secrets.HostKey(scope, hostIdentity, name)
		if source, exists := c.Secrets.SourceOf(key); exists && source != secrets.SourceDeclarative {
			loaded++
			continue
		}
		if conn == nil {
			return loaded, observe.ReasonSecretReadFailed, errors.New("no active connection")
		}
		// Read one byte beyond the accepted cap. EOF/Size are advisory remote
		// metadata and can be stale if the file grows between stat and read;
		// observing the extra byte makes the boundary independently enforceable.
		resp, err := conn.Do(ctx, &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: path, Limit: maxSecretFileBytes + 1}})
		if err != nil || resp == nil || resp.Read == nil {
			return loaded, observe.ReasonSecretReadFailed, errors.New("secret read failed")
		}
		value, reason, err := validateSecretRead(resp.Read)
		if err != nil {
			return loaded, reason, err
		}
		pending[key] = value
		loaded++
	}
	if err := c.Secrets.SetDeclarativeBatch(pending); err != nil {
		return 0, observe.ReasonSecretTooShort, err
	}
	return loaded, "", nil
}

func validateSecretRead(read *proto.ReadResult) (string, observe.SecretReason, error) {
	if read == nil {
		return "", observe.ReasonSecretReadFailed, errors.New("agent returned no content")
	}
	if !read.EOF || read.Size > maxSecretFileBytes || len(read.Content) > maxSecretFileBytes {
		return "", observe.ReasonSecretTruncated, errors.New("secret file exceeds the maximum size")
	}
	if read.ContentB64 {
		return "", observe.ReasonSecretBinary, errors.New("secret file must be text")
	}
	value := strings.TrimSpace(read.Content)
	if value == "" {
		return "", observe.ReasonSecretEmpty, errors.New("secret file is empty")
	}
	if len(value) < secrets.MinValueBytes {
		return "", observe.ReasonSecretTooShort, fmt.Errorf("secret value must be at least %d bytes", secrets.MinValueBytes)
	}
	return value, "", nil
}

type operationIdentity struct {
	Scope secrets.Scope
	Host  secrets.HostIdentity
	State session.State
}

type builtRequest struct {
	Request *proto.Request
	Echo    map[string]string
}

// do sends a request, retrying once on transport failure.
//
// A pooled connection can be dead for benign reasons: ControlPersist expired,
// the network blipped, the remote rebooted. Retrying once turns that into a
// hiccup instead of an error the caller has to interpret.
func (c *Client) do(ctx context.Context, hostName string, req *proto.Request) (*proto.Response, error) {
	resp, _, err := c.doBuilt(ctx, hostName, func(identity operationIdentity) (*builtRequest, error) {
		return &builtRequest{Request: req}, nil
	})
	return resp, err
}

// doBuilt holds one immutable identity lease across state capture, secret
// resolution, request construction, transport I/O, recursive response/error
// redaction, and any echoed request fields. A retry rebuilds the request only
// for the same immutable identity; an alias redefinition aborts rather than
// replaying argv, stdin, labels, paths, or content from host A to host B.
func (c *Client) doBuilt(ctx context.Context, hostName string, build func(operationIdentity) (*builtRequest, error)) (*proto.Response, map[string]string, error) {
	var firstErr error
	var firstIdentity secrets.HostIdentity
	for attempt := 0; attempt < 2; attempt++ {
		redactionSnapshot := c.Secrets.Snapshot()
		pooled, st, release, err := c.leasedConn(ctx, hostName)
		if err != nil {
			if firstErr != nil {
				return nil, nil, fmt.Errorf("%w (reconnect failed: %v)", firstErr, c.redactErrWith(redactionSnapshot, err))
			}
			return nil, nil, c.redactErrWith(redactionSnapshot, err)
		}
		identity := operationIdentity{Scope: pooled.scope, Host: pooled.host, State: st}
		if firstIdentity == (secrets.HostIdentity{}) {
			firstIdentity = identity.Host
		} else if firstIdentity != identity.Host {
			release()
			return nil, nil, errors.New("host identity changed while retrying request")
		}
		built, buildErr := build(identity)
		if buildErr != nil {
			redacted := c.redactErrWith(redactionSnapshot, buildErr)
			release()
			return nil, nil, redacted
		}
		resp, doErr := pooled.conn.Do(ctx, built.Request)
		var safeResp *proto.Response
		if resp != nil {
			safeResp = c.redactResponseWith(redactionSnapshot, resp)
		}
		safeEcho := make(map[string]string, len(built.Echo))
		for key, value := range built.Echo {
			safeEcho[key] = c.redactTextWith(redactionSnapshot, value)
		}
		safeErr := c.redactErrWith(redactionSnapshot, doErr)
		release()
		if doErr == nil {
			return safeResp, safeEcho, nil
		}
		// A remote-reported error is a real answer, not a broken pipe.
		if resp != nil || ctx.Err() != nil || attempt == 1 {
			return safeResp, safeEcho, safeErr
		}
		firstErr = safeErr

		c.mu.Lock()
		if current, ok := c.conns[pooled.host.Alias]; ok && current.conn == pooled.conn {
			delete(c.conns, pooled.host.Alias)
		}
		c.mu.Unlock()
		_ = pooled.conn.Close()
		c.setConnectionSecurity(pooled.host.Alias, ConnectionSecurityStatus{State: observe.SecurityCold, Generation: pooled.generation})
	}
	return nil, nil, firstErr
}

func (c *Client) redactResponse(resp *proto.Response) *proto.Response {
	return c.redactResponseWith(nil, resp)
}

func (c *Client) redactResponseWith(snapshot *secrets.Store, resp *proto.Response) *proto.Response {
	if resp == nil {
		return nil
	}
	// ContentB64 is untrusted remote metadata, not permission to bypass the
	// output boundary. A buggy or malicious agent could otherwise label literal
	// plaintext as base64 and make the client return it unchanged.
	var value any = resp
	if snapshot != nil {
		value = snapshot.RedactValue(value)
	}
	out := c.Secrets.RedactValue(value).(*proto.Response)
	if resp.Read != nil && resp.Read.ContentB64 && out.Read != nil && out.Read.Content != resp.Read.Content {
		// Do not leave a redaction placeholder mislabeled as valid base64. This
		// rare collision sacrifices the payload rather than disclosing an exact
		// registered value or returning silently corrupted encoded data.
		out.Read.ContentB64 = false
	}
	return out
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

	resp, echo, err := c.doBuilt(ctx, opts.Host, func(identity operationIdentity) (*builtRequest, error) {
		params, err := c.buildExecParams(identity, opts.Argv, opts.Cwd, opts.Env, opts.LoginShell)
		if err != nil {
			return nil, err
		}
		params.Stdin = opts.Stdin
		params.TimeoutSec = opts.TimeoutSec
		params.MaxOutputBytes = opts.MaxOutputBytes
		return &builtRequest{Request: &proto.Request{Op: proto.OpExec, Exec: params}, Echo: map[string]string{"cwd": params.Cwd}}, nil
	})
	if err != nil {
		return nil, err
	}
	if resp.Exec == nil {
		return nil, errors.New("agent returned no exec result")
	}

	return &ExecResult{ExecResult: resp.Exec, Cwd: echo["cwd"]}, nil
}

// buildExecParams layers session state under per-call values and resolves any
// "secret:NAME" env references.
func (c *Client) buildExecParams(identity operationIdentity, argv []string, cwd string, env map[string]string, login *bool) (*proto.ExecParams, error) {
	st := identity.State

	effCwd := cwd
	if effCwd == "" {
		effCwd = st.Cwd
	}
	effLogin := st.LoginShell
	if login != nil {
		effLogin = *login
	}

	merged := session.MergeEnv(st.Env, env)
	resolved, err := c.Secrets.ResolveEnv(identity.Scope, identity.Host, merged)
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
	return c.redactErrWith(nil, err)
}

func (c *Client) redactTextWith(snapshot *secrets.Store, text string) string {
	if snapshot != nil {
		text = snapshot.Redact(text)
	}
	return c.Secrets.Redact(text)
}

func (c *Client) redactErrWith(snapshot *secrets.Store, err error) error {
	if err == nil {
		return nil
	}
	msg := c.redactTextWith(snapshot, err.Error())
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
	resp, _, err := c.doBuilt(ctx, opts.Host, func(identity operationIdentity) (*builtRequest, error) {
		params, err := c.buildExecParams(identity, opts.Argv, opts.Cwd, opts.Env, opts.LoginShell)
		if err != nil {
			return nil, err
		}
		return &builtRequest{Request: &proto.Request{
			Op:  proto.OpJobStart,
			Job: &proto.JobParams{Spec: params, Label: opts.Label},
		}}, nil
	})
	if err != nil {
		return nil, err
	}
	if resp.Job == nil || resp.Job.Info == nil {
		return nil, errors.New("agent returned no job info")
	}
	return c.redactJob(resp.Job.Info), nil
}

// redactJob scrubs the recorded job fields that can carry a credential.
//
// Argv is the obvious one -- a token passed as a command-line flag. Label and Cwd
// are caller-supplied too and were missed initially, which is the same omission as
// SyncResult.Command: scrubbing chosen per field means the fields nobody thought
// about stay in the clear. The MCP boundary now backstops this, but the CLI calls
// straight into this package and never passes through that, so the fix belongs here.
func (c *Client) redactJob(j *proto.JobInfo) *proto.JobInfo {
	if j == nil {
		return nil
	}
	for i, a := range j.Argv {
		j.Argv[i] = c.Secrets.Redact(a)
	}
	j.Label = c.Secrets.Redact(j.Label)
	j.Cwd = c.Secrets.Redact(j.Cwd)
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

	// Read directly rather than via c.ReadFile: the raw value must be validated
	// and registered under the same identity lease before any redefinition can
	// publish. Passing through the normal response redactor would corrupt a value
	// that happens to contain an existing secret.
	pooled, release, err := c.mutationConn(ctx, host)
	if err != nil {
		// The prospective value is not in Store yet, so setup diagnostics cannot
		// be proven redactable. Keep this boundary fixed-text and expose detail
		// only through low-cardinality security telemetry.
		c.Hosts.RecordSecretLoadFailure(observe.ReasonSecretReadFailed, host)
		return errors.New("connection setup failed before secret registration")
	}
	resp, readErr := pooled.conn.Do(ctx, &proto.Request{
		Op:   proto.OpReadFile,
		Read: &proto.ReadParams{Path: path, Limit: maxSecretFileBytes + 1},
	})
	if readErr != nil {
		release()
		c.Hosts.RecordSecretLoadFailure(observe.ReasonSecretReadFailed, pooled.host.Alias)
		return errors.New("remote secret read failed before the value could be protected")
	}
	if resp == nil || resp.Read == nil {
		release()
		c.Hosts.RecordSecretLoadFailure(observe.ReasonSecretReadFailed, pooled.host.Alias)
		return errors.New("agent returned no content")
	}
	value, reason, validateErr := validateSecretRead(resp.Read)
	if validateErr != nil {
		release()
		c.Hosts.RecordSecretRejection(reason, pooled.host.Alias)
		return validateErr
	}
	setErr := c.Secrets.Set(secrets.HostKey(pooled.scope, pooled.host, name), value)
	if setErr != nil {
		c.Hosts.RecordSecretRejection(observe.ReasonSecretTooShort, pooled.host.Alias)
	}
	release()
	return setErr
}

func (c *Client) mutationConn(ctx context.Context, host string) (pooledConnection, func(), error) {
	for {
		if _, err := c.conn(ctx, host); err != nil {
			return pooledConnection{}, nil, err
		}
		resolved, err := c.Hosts.Resolve(host)
		if err != nil {
			return pooledConnection{}, nil, err
		}
		release, ok := c.Hosts.AcquireIdentityWrite(resolved.Host.Name, resolved.Generation, resolved.Fingerprint)
		if !ok {
			continue
		}
		c.mu.Lock()
		pooled, published := c.conns[resolved.Host.Name]
		c.mu.Unlock()
		if !published || pooled.generation != resolved.Generation || pooled.fingerprint != resolved.Fingerprint {
			release()
			continue
		}
		return pooled, release, nil
	}
}

// SetSecret registers an inline value. Hostless registrations remain available
// for output redaction compatibility, but are deliberately non-injectable.
func (c *Client) SetSecret(host, name, value string) error {
	if host == "" {
		if err := c.Secrets.Set(secrets.OutputKey(name), value); err != nil {
			c.Hosts.RecordSecretRejection(secretReasonForSetError(err), "output")
			return err
		}
		return nil
	}
	for {
		resolved, err := c.Hosts.Resolve(host)
		if err != nil {
			return err
		}
		release, ok := c.Hosts.AcquireIdentityWrite(resolved.Host.Name, resolved.Generation, resolved.Fingerprint)
		if !ok {
			continue
		}
		key := secrets.HostKey(secrets.Scope(resolved.Scope), secretHostIdentity(resolved), name)
		err = c.Secrets.Set(key, value)
		if err != nil {
			c.Hosts.RecordSecretRejection(secretReasonForSetError(err), resolved.Host.Name)
			release()
			return err
		}
		if c.ConnectionSecurity(resolved.Host.Name).State == observe.SecurityFailed {
			c.setConnectionSecurity(resolved.Host.Name, ConnectionSecurityStatus{State: observe.SecurityCold, Generation: resolved.Generation})
		}
		release()
		return nil
	}
}

func secretReasonForSetError(err error) observe.SecretReason {
	if err != nil && strings.Contains(err.Error(), "at least") {
		return observe.ReasonSecretTooShort
	}
	return observe.ReasonSecretInvalid
}

func (c *Client) SetOutputSecretFromFile(name, path string) error {
	err := c.Secrets.SetFromFile(secrets.OutputKey(name), path)
	if err != nil {
		c.Hosts.RecordSecretRejection(secretReasonForSetError(err), "output")
	}
	return err
}

func (c *Client) DeleteSecret(host, name string) (bool, error) {
	if host == "" {
		return c.Secrets.Delete(secrets.OutputKey(name)), nil
	}
	for {
		resolved, err := c.Hosts.Resolve(host)
		if err != nil {
			return false, err
		}
		release, ok := c.Hosts.AcquireIdentityWrite(resolved.Host.Name, resolved.Generation, resolved.Fingerprint)
		if !ok {
			continue
		}
		key := secrets.HostKey(secrets.Scope(resolved.Scope), secretHostIdentity(resolved), name)
		changed := c.Secrets.Delete(key)
		if changed {
			// Keep the identity write lease until the old connection is gone. If
			// the lease were released first, a new request could briefly reuse a
			// connection after its redaction value had been deleted.
			c.Disconnect(resolved.Host.Name)
		}
		release()
		return changed, nil
	}
}

func (c *Client) SecretLength(host, name string) (int, bool, error) {
	if host == "" {
		value, ok := c.Secrets.Get(secrets.OutputKey(name))
		return len(value), ok, nil
	}
	for {
		resolved, err := c.Hosts.Resolve(host)
		if err != nil {
			return 0, false, err
		}
		release, ok := c.Hosts.AcquireIdentity(resolved.Host.Name, resolved.Generation, resolved.Fingerprint)
		if !ok {
			continue
		}
		value, found := c.Secrets.Get(secrets.HostKey(secrets.Scope(resolved.Scope), secretHostIdentity(resolved), name))
		release()
		return len(value), found, nil
	}
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
	// redactResponse already applies the mandatory boundary even when the remote
	// marks Content as base64; that flag is not trusted as a disclosure bypass.
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
	if opts.Direction != "" && opts.Direction != "push" && opts.Direction != "pull" {
		return nil, fmt.Errorf("direction must be push or pull, got %q", opts.Direction)
	}
	if err := validateLocalSyncPath(opts.Local); err != nil {
		return nil, err
	}
	if err := validateRemoteSyncPath(opts.Remote); err != nil {
		return nil, err
	}
	if _, err := exec.LookPath("rsync"); err != nil {
		return nil, errors.New("rsync not found on the local host")
	}

	// Dial first so the ControlMaster exists and the remote host is validated
	// before rsync tries to use the socket.
	redactionSnapshot := c.Secrets.Snapshot()
	pooled, _, release, err := c.leasedConn(ctx, opts.Host)
	if err != nil {
		return nil, c.redactErrWith(redactionSnapshot, err)
	}
	defer release()

	args := buildSyncArgs(pooled.conn.Host(), pooled.conn.SSHArgs(), opts)

	var stdout, stderr string
	var runErr error
	if c.rsync != nil {
		stdout, stderr, runErr = c.rsync(ctx, args)
	} else {
		cmd := exec.CommandContext(ctx, "rsync", args...)
		var out, errBuf strings.Builder
		cmd.Stdout = &out
		cmd.Stderr = &errBuf
		runErr = cmd.Run()
		stdout, stderr = out.String(), errBuf.String()
	}

	res := &SyncResult{
		Stdout: c.redactTextWith(redactionSnapshot, stdout),
		Stderr: c.redactTextWith(redactionSnapshot, stderr),
		DryRun: opts.DryRun,
		// Redacted like the streams above. This echoes the assembled argv, and argv
		// is caller-supplied: an --exclude pattern or a path can carry a credential.
		// Leaving one field of the same struct unscrubbed is exactly the accident
		// decision 6 exists to prevent -- redaction has to be at the boundary, not
		// per field, or the next field added inherits the gap.
		Command: c.redactTextWith(redactionSnapshot, "rsync "+strings.Join(args, " ")),
	}
	var ee *exec.ExitError
	if errors.As(runErr, &ee) {
		res.ExitCode = ee.ExitCode()
	} else if runErr != nil {
		return nil, c.redactErrWith(redactionSnapshot, fmt.Errorf("run rsync: %w", runErr))
	}
	return res, nil
}

func buildSyncArgs(host transport.Host, sshArgs []string, opts SyncOptions) []string {
	sshCmd := append([]string{"ssh"}, sshArgs...)
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

	remoteSpec := host.Addr + ":" + opts.Remote
	switch opts.Direction {
	case "push", "":
		args = append(args, "--", opts.Local, remoteSpec)
	case "pull":
		args = append(args, "--", remoteSpec, opts.Local)
	}

	return args
}

// validateLocalSyncPath rejects only values that cannot be represented safely as
// a local rsync operand. A leading '-' is legitimate because Sync inserts '--'.
func validateLocalSyncPath(p string) error {
	if p == "" {
		return errors.New("local sync path required")
	}
	for _, r := range p {
		if r == 0 || unicode.IsControl(r) {
			return errors.New("local sync path contains a control character")
		}
	}
	return nil
}

// validateRemoteSyncPath is deliberately narrower because rsync sends the
// operand through a remote shell. The supported spelling covers ordinary
// absolute, relative, and ~/ paths without relying on remote-shell quoting.
func validateRemoteSyncPath(p string) error {
	if p == "" {
		return errors.New("remote sync path required")
	}
	if strings.HasPrefix(p, "-") {
		return fmt.Errorf("remote sync path %q must not start with '-'", p)
	}
	for _, r := range p {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') && r != '.' && r != '_' && r != '-' &&
			r != '/' && r != '~' {
			return fmt.Errorf("remote sync path %q contains unsupported character %q", p, r)
		}
	}
	return nil
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
	Removed []string
	Skipped []string
	// Missing holds IDs whose records were already gone. A removal is idempotent,
	// so a repeated or concurrent rm reports this rather than failing.
	Missing    []string
	FreedBytes int64
}

// JobRm deletes job records to reclaim disk.
//
// Job logs are unbounded, so a machine running batches accumulates them until the
// disk fills. Running jobs are never removed; they come back in Skipped. A job
// that was already gone comes back in Missing, not as an error.
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
		Missing:    resp.Job.Missing,
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
	conn.conn.Close()
	c.setConnectionSecurity(hostName, ConnectionSecurityStatus{State: observe.SecurityCold, Generation: conn.generation})
	return true
}

// Close tears down all pooled connections.
func (c *Client) Close() {
	c.mu.Lock()
	conns := make([]remoteConnection, 0, len(c.conns))
	for _, conn := range c.conns {
		conns = append(conns, conn.conn)
	}
	c.conns = make(map[string]pooledConnection)
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
