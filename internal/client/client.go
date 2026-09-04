// Package client is the shared core behind both the MCP server and the CLI.
//
// It owns connection pooling, applies session state and secret redaction, and
// exposes one method per operation. Both front ends call these methods, so
// behaviour cannot drift between them.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

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

type negotiatedConnection interface {
	NegotiatedVersion() int
	SupportsFeature(proto.Feature) bool
}

type pooledConnection struct {
	conn                  remoteConnection
	fingerprint           string
	connectionFingerprint string
	generation            uint64
	scope                 secrets.Scope
	host                  secrets.HostIdentity
	publication           uint64
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
type rsyncRunner func(context.Context, []string, io.Writer, io.Writer) error

// Client is the entry point for remote operations.
type Client struct {
	Hosts   *session.Registry
	Secrets *secrets.Store
	// callerID is stable for the lifetime of this Client and scopes remote
	// operation deduplication. Generation can fail only if the platform random
	// source fails; retain that error and fail requests closed instead of falling
	// back to a predictable identity that could collide with another caller.
	callerID    string
	callerIDErr error

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
	// A monotonically increasing publication token prevents teardown of an old
	// connection from overwriting the security state of a newer one.
	nextPublication   uint64
	latestPublication map[string]uint64
	capabilities      map[string]capabilityCacheEntry
}

type capabilityCacheEntry struct {
	result     *proto.CapabilityResult
	expiresAt  time.Time
	generation uint64
}

func New(lookup AgentLookup) *Client {
	callerID, callerIDErr := proto.NewOperationID()
	if callerIDErr == nil {
		callerID = "client_" + strings.TrimPrefix(callerID, "op_")
	}
	c := &Client{
		Hosts:       session.NewRegistry(),
		Secrets:     secrets.New(),
		callerID:    callerID,
		callerIDErr: callerIDErr,
		lookup:      lookup,
		dial: func(ctx context.Context, host transport.Host, lookup AgentLookup) (remoteConnection, error) {
			return transport.Dial(ctx, host, lookup)
		},
		conns:             make(map[string]pooledConnection),
		security:          make(map[string]ConnectionSecurityStatus),
		dialing:           make(map[string]*sync.Mutex),
		latestPublication: make(map[string]uint64),
		capabilities:      make(map[string]capabilityCacheEntry),
	}
	c.Hosts.SetHostChangeHook(c.invalidateHost)
	c.Secrets.SetRedactionHook(c.Hosts.RecordRedactionHit)
	return c
}

func (c *Client) invalidateHost(name string, generation uint64) {
	c.mu.Lock()
	delete(c.capabilities, name)
	c.mu.Unlock()
	detached := c.disconnectWithStatus(name, ConnectionSecurityStatus{State: observe.SecurityCold, Generation: generation})
	if resolved, err := c.Hosts.Resolve(name); err == nil {
		c.Secrets.DeleteStaleHost(secrets.Scope(resolved.Scope), secretHostIdentity(resolved))
	} else {
		c.Secrets.DeleteHost(name)
	}
	if detached {
		return
	}
	// The registry publishes before invoking this hook. A request may already
	// have started initialization for the new generation, so do not replace its
	// initializing/failed state (or a new ready connection) with cold.
	c.mu.Lock()
	previous := c.security[name]
	_, connected := c.conns[name]
	if connected || previous.Generation == generation {
		c.mu.Unlock()
		return
	}
	// Supersede any already-detached teardown that may still be blocked in
	// Close. Without a new token it could return later and restore its old
	// generation even though the registry invalidation already published.
	c.nextPublication++
	c.latestPublication[name] = c.nextPublication
	status := ConnectionSecurityStatus{State: observe.SecurityCold, Generation: generation}
	c.security[name] = status
	c.mu.Unlock()
	c.recordConnectionSecurityTransition(name, previous, status)
}

func (c *Client) setConnectionSecurity(name string, status ConnectionSecurityStatus) {
	c.mu.Lock()
	previous := c.security[name]
	c.security[name] = status
	c.mu.Unlock()
	c.recordConnectionSecurityTransition(name, previous, status)
}

// publishConnectionSecurityIfCurrent is the only post-Close publication
// boundary. Close may block while another goroutine reserves or publishes a
// replacement. The closing owner may update status only while its publication
// token is still current and the alias has no replacement connection.
func (c *Client) publishConnectionSecurityIfCurrent(name string, publication uint64, status ConnectionSecurityStatus) bool {
	c.mu.Lock()
	if _, connected := c.conns[name]; connected || c.latestPublication[name] != publication {
		c.mu.Unlock()
		return false
	}
	previous := c.security[name]
	c.security[name] = status
	c.mu.Unlock()
	c.recordConnectionSecurityTransition(name, previous, status)
	return true
}

// detachConnection removes exactly the expected published connection. An
// instance check at detach plus a token check after Close prevents both ABA
// replacement and stale status publication.
func (c *Client) detachConnection(name string, expected pooledConnection) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.conns[name]
	if !ok || current.conn != expected.conn || current.publication != expected.publication {
		return false
	}
	delete(c.conns, name)
	return true
}

func (c *Client) closeDetachedConnection(name string, detached pooledConnection, status ConnectionSecurityStatus) {
	_ = detached.conn.Close()
	c.publishConnectionSecurityIfCurrent(name, detached.publication, status)
}

func (c *Client) detachAndCloseConnection(name string, expected pooledConnection, status ConnectionSecurityStatus) bool {
	if !c.detachConnection(name, expected) {
		return false
	}
	c.closeDetachedConnection(name, expected, status)
	return true
}

func (c *Client) recordConnectionSecurityTransition(name string, previous, status ConnectionSecurityStatus) {
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
		c.mu.Unlock()
		if ok {
			c.detachAndCloseConnection(resolved.Host.Name, existing, ConnectionSecurityStatus{
				State: observe.SecurityCold, Generation: existing.generation,
			})
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
		// Reserve the next publication token before validation or dialing. A
		// detached predecessor may still be blocked in Close; from this point its
		// teardown is stale even if this setup ultimately fails closed.
		c.mu.Lock()
		c.nextPublication++
		setupPublication := c.nextPublication
		c.latestPublication[resolved.Host.Name] = setupPublication
		c.mu.Unlock()
		st := c.Hosts.State(resolved.Host.Name)
		if err := secrets.ValidateDeclarations(st.Secrets); err != nil {
			c.Hosts.RecordSecretLoadFailure(observe.ReasonSecretInvalid, resolved.Host.Name)
			c.publishConnectionSecurityIfCurrent(resolved.Host.Name, setupPublication, ConnectionSecurityStatus{
				State: observe.SecurityFailed, Generation: resolved.Generation,
				Declared: len(st.Secrets), Reason: observe.ReasonSecretInvalid,
			})
			releaseIdentity()
			return nil, errors.New("connection security initialization failed (invalid secret declaration)")
		}
		status := ConnectionSecurityStatus{
			State: observe.SecurityInitializing, Generation: resolved.Generation,
			Declared: len(st.Secrets),
		}
		c.publishConnectionSecurityIfCurrent(resolved.Host.Name, setupPublication, status)

		conn, dialErr := c.dial(ctx, resolved.Host, c.lookup)
		if dialErr != nil {
			if len(st.Secrets) > 0 {
				// Declared values are intentionally not available until the secure
				// connection can read them. Bootstrap diagnostics may nevertheless
				// echo one (for example from a remote shell profile), so returning the
				// raw dial error here would create an unredactable pre-init leak.
				c.Hosts.RecordSecretLoadFailure(observe.ReasonSecretReadFailed, resolved.Host.Name)
				c.publishConnectionSecurityIfCurrent(resolved.Host.Name, setupPublication, ConnectionSecurityStatus{
					State: observe.SecurityFailed, Generation: resolved.Generation,
					Declared: len(st.Secrets), Reason: observe.ReasonSecretReadFailed,
				})
				releaseIdentity()
				return nil, errors.New("connection setup failed before declared secrets could be protected")
			}
			c.publishConnectionSecurityIfCurrent(resolved.Host.Name, setupPublication, ConnectionSecurityStatus{State: observe.SecurityCold, Generation: resolved.Generation})
			releaseIdentity()
			return nil, dialErr
		}
		loaded, reason, loadErr := c.loadHostSecrets(ctx, resolved, st, conn)
		if loadErr != nil {
			c.closeDetachedConnection(resolved.Host.Name, pooledConnection{
				conn: conn, generation: resolved.Generation, publication: setupPublication,
			}, ConnectionSecurityStatus{
				State: observe.SecurityFailed, Generation: resolved.Generation,
				Declared: len(st.Secrets), Loaded: loaded, Reason: reason,
			})
			releaseIdentity()
			c.Hosts.RecordSecretLoadFailure(reason, resolved.Host.Name)
			return nil, fmt.Errorf("connection security initialization failed (%s)", reason)
		}

		hostIdentity := secretHostIdentity(resolved)
		pooled := pooledConnection{
			conn: conn, fingerprint: resolved.Fingerprint, connectionFingerprint: resolved.ConnectionFingerprint, generation: resolved.Generation,
			scope: secrets.Scope(resolved.Scope), host: hostIdentity,
		}
		// Publication is the commit point: all declared secrets are present and
		// the identity read lease still prevents a redefinition.
		ready := ConnectionSecurityStatus{
			State: observe.SecurityReady, Generation: resolved.Generation,
			Declared: len(st.Secrets), Loaded: loaded,
		}
		c.mu.Lock()
		pooled.publication = setupPublication
		previous := c.security[resolved.Host.Name]
		c.latestPublication[resolved.Host.Name] = pooled.publication
		c.conns[resolved.Host.Name] = pooled
		c.security[resolved.Host.Name] = ready
		c.mu.Unlock()
		c.recordConnectionSecurityTransition(resolved.Host.Name, previous, ready)
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
		resp, err := c.doRawOnConnection(ctx, conn, &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: path, Limit: maxSecretFileBytes + 1}})
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

// doRawOnConnection supplies protocol-3 identity without passing the response
// through normal redaction. Secret bootstrap needs the prospective plaintext
// under the existing immutable identity lease so it can validate and register
// the value before any caller-visible projection occurs.
func (c *Client) doRawOnConnection(ctx context.Context, conn remoteConnection, request *proto.Request) (*proto.Response, error) {
	if conn == nil || request == nil || c.callerIDErr != nil || c.callerID == "" {
		return nil, proto.NewError(proto.CodeInternalFailure, "", proto.StateNotSent)
	}
	descriptor, err := proto.RequireOperation(request.Op)
	if err != nil {
		return nil, err
	}
	if negotiated, ok := conn.(negotiatedConnection); ok && negotiated.NegotiatedVersion() >= 3 {
		for _, feature := range descriptor.RequiredFeatures {
			if !negotiated.SupportsFeature(feature) {
				return nil, proto.NewError(proto.CodeUnsupportedFeature, "", proto.StateNotSent)
			}
		}
		operationID, idErr := proto.NewOperationID()
		if idErr != nil {
			return nil, proto.NewError(proto.CodeInternalFailure, "", proto.StateNotSent)
		}
		request.OperationID = operationID
		request.ClientID = c.callerID
	} else {
		request.OperationID = ""
		request.ClientID = ""
		request.Replay = false
		request.DeadlineUnixMilli = 0
		request.StreamWindowBytes = 0
	}
	return conn.Do(ctx, request)
}

func validateSecretRead(read *proto.ReadResult) (string, observe.SecretReason, error) {
	if read == nil {
		return "", observe.ReasonSecretReadFailed, errors.New("agent returned no content")
	}
	if !read.EOF || read.Size > maxSecretFileBytes {
		return "", observe.ReasonSecretTruncated, errors.New("secret file exceeds the maximum size")
	}
	if read.ContentB64 {
		return "", observe.ReasonSecretBinary, errors.New("secret file must be text")
	}
	if len(read.Content) > maxSecretFileBytes {
		return "", observe.ReasonSecretTruncated, errors.New("secret file exceeds the maximum size")
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

// do sends a request according to the retry policy in proto's shared operation
// registry. Unknown operations fail closed before any transport is acquired.
func (c *Client) do(ctx context.Context, hostName string, req *proto.Request) (*proto.Response, error) {
	if req == nil {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if _, err := proto.RequireOperation(req.Op); err != nil {
		return nil, err
	}
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
	if c.callerIDErr != nil || c.callerID == "" {
		return nil, nil, proto.NewError(proto.CodeInternalFailure, "", proto.StateNotSent)
	}
	operationID, err := proto.NewOperationID()
	if err != nil {
		return nil, nil, proto.NewError(proto.CodeInternalFailure, "", proto.StateNotSent)
	}

	// A deadline is semantic request state, so capture it once. Rebuilding a
	// request after reconnect must not silently extend its execution budget.
	var deadlineUnixMilli int64
	if deadline, ok := ctx.Deadline(); ok {
		deadlineUnixMilli = deadline.UnixMilli()
	}

	var firstErr error
	var firstIdentity secrets.HostIdentity
	var descriptor proto.OperationDescriptor
	var operationName string
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
		if built == nil || built.Request == nil {
			release()
			return nil, nil, proto.NewError(proto.CodeInvalidRequest, operationID, proto.StateNotSent)
		}
		if attempt == 0 {
			operationName = built.Request.Op
			var descriptorErr error
			descriptor, descriptorErr = proto.RequireOperation(operationName)
			if descriptorErr != nil {
				release()
				return nil, nil, descriptorErr
			}
			deadlineSupported := false
			for _, feature := range descriptor.RequiredFeatures {
				deadlineSupported = deadlineSupported || feature == proto.FeatureDeadline
			}
			if built.Request.DeadlineUnixMilli != 0 && !deadlineSupported {
				release()
				return nil, nil, proto.NewError(proto.CodeInvalidRequest, operationID, proto.StateNotSent)
			}
			// An explicit protocol deadline wins when the context has none, but it
			// is still frozen here and reused verbatim on every attempt.
			if deadlineSupported && deadlineUnixMilli == 0 {
				deadlineUnixMilli = built.Request.DeadlineUnixMilli
			}
		} else if built.Request.Op != operationName {
			release()
			return nil, nil, proto.NewError(proto.CodeInvalidRequest, operationID, proto.StateNotSent)
		}
		if negotiated, ok := pooled.conn.(negotiatedConnection); ok && negotiated.NegotiatedVersion() >= 3 {
			for _, feature := range descriptor.RequiredFeatures {
				if !negotiated.SupportsFeature(feature) {
					release()
					return nil, nil, proto.NewError(proto.CodeUnsupportedFeature, operationID, proto.StateNotSent)
				}
			}
		}

		built.Request.OperationID = operationID
		built.Request.ClientID = c.callerID
		built.Request.Replay = attempt > 0
		built.Request.DeadlineUnixMilli = 0
		for _, feature := range descriptor.RequiredFeatures {
			if feature == proto.FeatureDeadline {
				built.Request.DeadlineUnixMilli = deadlineUnixMilli
				break
			}
		}
		if built.Request.StreamWindowBytes == 0 {
			if negotiated, ok := pooled.conn.(negotiatedConnection); ok &&
				negotiated.NegotiatedVersion() >= 3 && negotiated.SupportsFeature(proto.FeatureStreaming) {
				built.Request.StreamWindowBytes = 64 << 10
			}
		}
		if connectionUsesLegacyUnary(pooled.conn) {
			built.Request.OperationID = ""
			built.Request.ClientID = ""
			built.Request.Replay = false
			built.Request.DeadlineUnixMilli = 0
			built.Request.StreamWindowBytes = 0
		}
		c.Hosts.RecordRequestEvent(observe.RequestQueued)
		resp, doErr := pooled.conn.Do(ctx, built.Request)
		var safeResp *proto.Response
		if resp != nil {
			safeResp = c.redactResponseWith(redactionSnapshot, resp)
			if safeResp.OperationID == "" {
				safeResp.OperationID = operationID
			}
			if connectionUsesLegacyUnary(pooled.conn) && safeResp.Type == "" {
				safeResp.Terminal = true
				if safeResp.Execution == "" {
					safeResp.Execution = proto.StateCompleted
				}
			}
			stampResponseMetadata(safeResp)
		}
		safeEcho := make(map[string]string, len(built.Echo))
		for key, value := range built.Echo {
			safeEcho[key] = c.redactTextWith(redactionSnapshot, value)
		}
		safeErr := c.redactErrWith(redactionSnapshot, doErr)
		release()
		if doErr == nil {
			c.Hosts.RecordRequestEvent(observe.RequestCompleted)
			return safeResp, safeEcho, nil
		}
		// A remote-reported error is a real answer, not a broken pipe. Context
		// cancellation is handled by protocol-level cancel in the transport and
		// must never be converted into a transparent replay here.
		if resp != nil || ctx.Err() != nil || attempt == 1 {
			if ctx.Err() != nil {
				c.Hosts.RecordRequestEvent(observe.RequestCanceled)
			}
			return safeResp, safeEcho, safeErr
		}
		firstErr = safeErr

		c.detachAndCloseConnection(pooled.host.Alias, pooled, ConnectionSecurityStatus{
			State: observe.SecurityCold, Generation: pooled.generation,
		})

		switch descriptor.Retry {
		case proto.RetrySafe:
			// Read-only and explicitly idempotent operations may reconnect once.
			continue
		case proto.RetryDeduplicated:
			// A transport failure tears down this per-SSH-channel agent process.
			// Its in-memory dedupe cache therefore cannot prove that a mutation
			// did not already execute. Preserve the stable operation identity for
			// diagnosis, but do not send it to a fresh process.
			c.Hosts.RecordRequestEvent(observe.RequestAmbiguous)
			return safeResp, safeEcho, proto.NewError(
				proto.CodeAmbiguousOutcome, operationID, proto.StatePossiblyExecuted,
			)
		default:
			return safeResp, safeEcho, safeErr
		}
	}
	return nil, nil, firstErr
}

func stampResponseMetadata(response *proto.Response) {
	if response == nil {
		return
	}
	stamp := func(operationID *string, terminal *bool, execution *proto.ExecutionState) {
		if response.OperationID != "" {
			*operationID = response.OperationID
		}
		if response.Terminal {
			*terminal = true
		}
		if response.Execution != "" {
			*execution = response.Execution
		}
	}
	if response.Exec != nil {
		stamp(&response.Exec.OperationID, &response.Exec.Terminal, &response.Exec.Execution)
	}
	if response.Read != nil {
		stamp(&response.Read.OperationID, &response.Read.Terminal, &response.Read.Execution)
	}
	if response.Cat != nil {
		stamp(&response.Cat.OperationID, &response.Cat.Terminal, &response.Cat.Execution)
	}
	if response.Job != nil {
		stamp(&response.Job.OperationID, &response.Job.Terminal, &response.Job.Execution)
		stampJobInfo := func(info *proto.JobInfo) {
			if info != nil {
				stamp(&info.OperationID, &info.Terminal, &info.Execution)
			}
		}
		stampJobInfo(response.Job.Info)
		for _, info := range response.Job.List {
			stampJobInfo(info)
		}
		for _, waited := range response.Job.Waited {
			if waited != nil {
				stampJobInfo(waited.Info)
			}
		}
	}
	if response.List != nil {
		stamp(&response.List.OperationID, &response.List.Terminal, &response.List.Execution)
	}
}

func missingResultError(response *proto.Response) error {
	operationID := ""
	state := proto.StateFailed
	if response != nil {
		operationID = response.OperationID
		if proto.ValidExecutionState(response.Execution) {
			state = response.Execution
		}
	}
	return proto.NewError(proto.CodeInvalidFrame, operationID, state)
}

func connectionUsesLegacyUnary(conn remoteConnection) bool {
	negotiated, ok := conn.(negotiatedConnection)
	if !ok {
		return true // test/compatibility adapters predate feature introspection
	}
	// Typed terminal semantics are a protocol-3 baseline. FeatureStreaming gates
	// optional data/progress delivery, not permission to mimic protocol 2's
	// shape or discard operation identity.
	return negotiated.NegotiatedVersion() < 3
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
	if resp.Read != nil && out.Read != nil {
		out.Read.Content, out.Read.ContentB64 = c.redactWirePayload(snapshot, resp.Read.Content, resp.Read.ContentB64)
	}
	if resp.Exec != nil && out.Exec != nil {
		out.Exec.Stdout, out.Exec.StdoutB64 = c.redactWirePayload(snapshot, resp.Exec.Stdout, resp.Exec.StdoutB64)
		out.Exec.Stderr, out.Exec.StderrB64 = c.redactWirePayload(snapshot, resp.Exec.Stderr, resp.Exec.StderrB64)
	}
	return out
}

// redactWirePayload decodes binary-safe wire fields before redaction and then
// chooses a lossless text/base64 projection again. Searching only the encoded
// spelling would let a secret hidden behind base64 reach CLI or MCP unchanged.
// A false base64 flag is treated as untrusted metadata: redact the literal and
// clear the flag rather than returning a mislabeled payload.
func (c *Client) redactWirePayload(snapshot *secrets.Store, value string, encoded bool) (string, bool) {
	raw := []byte(value)
	if encoded {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return c.redactTextWith(snapshot, value), false
		}
		raw = decoded
	}
	return encodeCapturedOutput([]byte(c.redactTextWith(snapshot, string(raw))))
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
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
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
		return nil, missingResultError(resp)
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
	Resources  *proto.ResourceEnvelope
}

// JobStart launches a job that outlives the connection.
func (c *Client) JobStart(ctx context.Context, opts JobStartOptions) (*proto.JobInfo, error) {
	if len(opts.Argv) == 0 {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	resp, _, err := c.doBuilt(ctx, opts.Host, func(identity operationIdentity) (*builtRequest, error) {
		params, err := c.buildExecParams(identity, opts.Argv, opts.Cwd, opts.Env, opts.LoginShell)
		if err != nil {
			return nil, err
		}
		return &builtRequest{Request: &proto.Request{
			Op:  proto.OpJobStart,
			Job: &proto.JobParams{Spec: params, Label: opts.Label, Resources: opts.Resources},
		}}, nil
	})
	if err != nil {
		return nil, err
	}
	if resp.Job == nil || resp.Job.Info == nil {
		return nil, missingResultError(resp)
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
		return nil, missingResultError(resp)
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
		return nil, missingResultError(resp)
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
		return nil, missingResultError(resp)
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
		return nil, missingResultError(resp)
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
	ID             string
	Info           *proto.JobInfo
	Err            string
	Logs           string
	LogsTruncation proto.Truncation
}

// JobWaitResult is a finished (or still-running) job plus wait bookkeeping.
type JobWaitResult struct {
	Info           *proto.JobInfo
	TimedOut       bool
	WaitedMS       int64
	Logs           string
	LogsTruncation proto.Truncation
	OperationID    string
	Terminal       bool
	Execution      proto.ExecutionState
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
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
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
		return nil, missingResultError(resp)
	}

	out := &JobWaitResult{
		TimedOut:       resp.Job.TimedOut,
		WaitedMS:       resp.Job.WaitedMS,
		Logs:           c.Secrets.Redact(resp.Job.Logs),
		LogsTruncation: resp.Job.LogsTruncation,
		OperationID:    resp.Job.OperationID, Terminal: resp.Job.Terminal, Execution: resp.Job.Execution,
	}
	if len(resp.Job.Waited) > 0 {
		for _, w := range resp.Job.Waited {
			out.Waited = append(out.Waited, WaitedJob{
				ID:             w.ID,
				Info:           c.redactJob(w.Info),
				Err:            c.Secrets.Redact(w.Err),
				Logs:           c.Secrets.Redact(w.Logs),
				LogsTruncation: w.LogsTruncation,
			})
		}
		return out, nil
	}
	if resp.Job.Info == nil {
		return nil, missingResultError(resp)
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
	resp, readErr := c.doRawOnConnection(ctx, pooled.conn, &proto.Request{
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
		return nil, missingResultError(resp)
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
		return nil, missingResultError(resp)
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
	// ConfirmDelete is required for a mutating --delete transfer. Dry-runs are
	// always allowed; this gate prevents an accidental destructive invocation.
	ConfirmDelete bool
	// SymlinkPolicy is preserve (default), follow, or skip.
	SymlinkPolicy string
	// ConflictPolicy is overwrite (default), skip, or fail. fail performs a
	// dry-run preflight and refuses when rsync reports conflicts.
	ConflictPolicy string
	// MaxOutputBytes may only lower the per-stream system cap. Zero uses the
	// bounded default.
	MaxOutputBytes int64
}

// SyncResult reports rsync's outcome.
type SyncResult struct {
	Stdout           string           `json:"stdout"`
	Stderr           string           `json:"stderr"`
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
}

const defaultSyncOutputBytes int64 = 256 << 10

// Sync transfers files with rsync over the multiplexed ssh connection.
//
// rsync runs locally rather than through the agent protocol: it already solves
// delta transfer and permissions, and reimplementing that would be strictly
// worse. Reusing the ControlMaster socket keeps it from re-authenticating.
func (c *Client) Sync(ctx context.Context, opts SyncOptions) (*SyncResult, error) {
	if opts.Local == "" || opts.Remote == "" {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if opts.Direction != "" && opts.Direction != "push" && opts.Direction != "pull" {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if opts.Delete && !opts.DryRun && !opts.ConfirmDelete {
		return nil, proto.NewError(proto.CodeInvalidRequest, "sync --delete requires explicit confirmation", proto.StateNotSent)
	}
	if opts.SymlinkPolicy == "" {
		opts.SymlinkPolicy = "preserve"
	}
	if opts.SymlinkPolicy != "preserve" && opts.SymlinkPolicy != "follow" && opts.SymlinkPolicy != "skip" {
		return nil, proto.NewError(proto.CodeInvalidRequest, "invalid symlink policy", proto.StateNotSent)
	}
	if opts.ConflictPolicy == "" {
		opts.ConflictPolicy = "overwrite"
	}
	if opts.ConflictPolicy != "overwrite" && opts.ConflictPolicy != "skip" && opts.ConflictPolicy != "fail" {
		return nil, proto.NewError(proto.CodeInvalidRequest, "invalid conflict policy", proto.StateNotSent)
	}
	limit := opts.MaxOutputBytes
	if limit < 0 || limit > proto.AbsoluteOutputBytes {
		return nil, proto.NewError(proto.CodeLimitExceeded, "", proto.StateNotSent)
	}
	if limit == 0 {
		limit = defaultSyncOutputBytes
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
	// A mutating conflict=fail transfer must first perform a read-only rsync
	// plan.  Treat any itemized update to an existing path as a conflict and
	// never proceed to the mutating invocation when the preflight is uncertain.
	// This is intentionally done after leasing the connection so both probes use
	// the same validated host identity and ControlMaster.
	if opts.ConflictPolicy == "fail" && !opts.DryRun {
		preflightArgs := buildSyncArgsWith(pooled.conn.Host(), pooled.conn.SSHArgs(), opts, true)
		preflightOut := newBoundedCapture(limit)
		preflightErrOut := newBoundedCapture(limit)
		var preflightErr error
		if c.rsync != nil {
			preflightErr = c.rsync(ctx, preflightArgs, preflightOut, preflightErrOut)
		} else {
			cmd := exec.CommandContext(ctx, "rsync", preflightArgs...)
			cmd.Stdout = preflightOut
			cmd.Stderr = preflightErrOut
			preflightErr = cmd.Run()
		}
		if preflightErr != nil {
			if ctx.Err() != nil {
				return nil, c.redactErrWith(redactionSnapshot, ctx.Err())
			}
			return nil, c.redactErrWith(redactionSnapshot, fmt.Errorf("sync conflict preflight: %w", preflightErr))
		}
		preflightRaw, preflightTruncation := preflightOut.payload()
		// A bounded capture that dropped any part of the plan cannot establish
		// that no conflict exists. Refuse the mutation rather than allowing a
		// conflict hidden after the retention boundary to proceed.
		if preflightTruncation.Truncated || syncPlanHasConflicts(preflightRaw) {
			return nil, proto.NewError(proto.CodeInvalidRequest, "sync conflict detected", proto.StateNotSent)
		}
	}
	manifest := syncManifest{}
	if opts.Direction == "" || opts.Direction == "push" {
		manifest, err = buildSyncManifest(opts.Local, opts.SymlinkPolicy)
		if err != nil {
			// Preserve rsync's own diagnostics for a missing source path. The
			// manifest is an audit aid, not a second path-validation mechanism.
			if os.IsNotExist(err) {
				manifest = syncManifest{}
			} else {
				return nil, c.redactErrWith(redactionSnapshot, fmt.Errorf("build sync manifest: %w", err))
			}
		}
	}

	stdoutCapture := newBoundedCapture(limit)
	stderrCapture := newBoundedCapture(limit)
	var runErr error
	if c.rsync != nil {
		runErr = c.rsync(ctx, args, stdoutCapture, stderrCapture)
	} else {
		cmd := exec.CommandContext(ctx, "rsync", args...)
		cmd.Stdout = stdoutCapture
		cmd.Stderr = stderrCapture
		runErr = cmd.Run()
	}
	stdoutRaw, stdoutTruncation := stdoutCapture.payload()
	stderrRaw, stderrTruncation := stderrCapture.payload()
	redactedStdout, stdoutB64 := encodeCapturedOutput([]byte(c.redactTextWith(redactionSnapshot, string(stdoutRaw))))
	redactedStderr, stderrB64 := encodeCapturedOutput([]byte(c.redactTextWith(redactionSnapshot, string(stderrRaw))))

	res := &SyncResult{
		Stdout: redactedStdout, Stderr: redactedStderr,
		StdoutB64: stdoutB64, StderrB64: stderrB64,
		StdoutTruncation: stdoutTruncation, StderrTruncation: stderrTruncation,
		Truncated: stdoutTruncation.Truncated || stderrTruncation.Truncated,
		DryRun:    opts.DryRun,
		// Redacted like the streams above. This echoes the assembled argv, and argv
		// is caller-supplied: an --exclude pattern or a path can carry a credential.
		// Leaving one field of the same struct unscrubbed is exactly the accident
		// decision 6 exists to prevent -- redaction has to be at the boundary, not
		// per field, or the next field added inherits the gap.
		Command:        c.redactTextWith(redactionSnapshot, "rsync "+strings.Join(args, " ")),
		ManifestDigest: manifest.Digest, ManifestEntries: manifest.Entries,
	}
	var ee *exec.ExitError
	if errors.As(runErr, &ee) {
		res.ExitCode = ee.ExitCode()
	} else if runErr != nil {
		if ctx.Err() != nil {
			return nil, c.redactErrWith(redactionSnapshot, ctx.Err())
		}
		return nil, c.redactErrWith(redactionSnapshot, fmt.Errorf("run rsync: %w", runErr))
	}
	return res, nil
}

type boundedCapture struct {
	mu    sync.Mutex
	buf   []byte
	total int64
	limit int64
}

func newBoundedCapture(limit int64) *boundedCapture {
	if limit <= 0 || limit > proto.AbsoluteOutputBytes {
		limit = defaultSyncOutputBytes
	}
	return &boundedCapture{limit: limit}
}

func (b *boundedCapture) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf == nil {
		b.buf = make([]byte, 0, int(b.limit))
	}
	if int64(len(p)) > int64(^uint64(0)>>1)-b.total {
		b.total = int64(^uint64(0) >> 1)
	} else {
		b.total += int64(len(p))
	}
	if remaining := b.limit - int64(len(b.buf)); remaining > 0 {
		keep := min(int64(len(p)), remaining)
		b.buf = append(b.buf, p[:int(keep)]...)
	}
	return len(p), nil
}

func (b *boundedCapture) payload() ([]byte, proto.Truncation) {
	b.mu.Lock()
	defer b.mu.Unlock()
	raw := append([]byte(nil), b.buf...)
	truncation, _ := proto.NewTruncation(b.total, int64(len(raw)))
	return raw, truncation
}

func encodeCapturedOutput(raw []byte) (string, bool) {
	if utf8.Valid(raw) && !bytes.ContainsRune(raw, 0) {
		return string(raw), false
	}
	return base64.StdEncoding.EncodeToString(raw), true
}

func buildSyncArgs(host transport.Host, sshArgs []string, opts SyncOptions) []string {
	return buildSyncArgsWith(host, sshArgs, opts, false)
}

func buildSyncArgsWith(host transport.Host, sshArgs []string, opts SyncOptions, preflight bool) []string {
	sshCmd := append([]string{"ssh"}, sshArgs...)
	// Only long-standing flags: macOS ships openrsync, which rejects newer
	// options like --info=stats1 that samba rsync accepts. -v gives a
	// transferred-file list on both implementations.
	args := []string{"-az", "-v", "-e", strings.Join(sshCmd, " ")}
	if opts.DryRun || preflight {
		args = append(args, "--dry-run")
	}
	if opts.Delete {
		args = append(args, "--delete")
	}
	if preflight {
		args = append(args, "--itemize-changes", "--out-format=%i %n%L")
	}
	switch opts.SymlinkPolicy {
	case "follow":
		args = append(args, "--copy-links")
	case "skip":
		args = append(args, "--no-links")
	}
	if opts.ConflictPolicy == "skip" {
		args = append(args, "--ignore-existing")
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

// syncPlanHasConflicts examines rsync's itemized dry-run output.  New paths
// are represented by a ten-character run of '+' after the two-character type
// prefix (for example >f+++++++++); updates to an existing path contain one or
// more change markers instead. Deletions are expected under --delete and are
// not conflicts. Unknown/non-itemized lines are ignored as diagnostics.
func syncPlanHasConflicts(raw []byte) bool {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 64<<10), int(proto.AbsoluteOutputBytes))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "*deleting") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 || len(fields[0]) != 11 {
			continue
		}
		code := fields[0]
		if strings.HasSuffix(code, "+++++++++") {
			continue
		}
		// It is an itemized change to an existing object. Fail closed if the
		// format ever changes in a way we cannot classify.
		if code[0] == '>' || code[0] == '<' || code[0] == 'c' || code[0] == 'h' || code[0] == '.' {
			return true
		}
	}
	return scanner.Err() != nil
}

// validateLocalSyncPath rejects only values that cannot be represented safely as
// a local rsync operand. A leading '-' is legitimate because Sync inserts '--'.
func validateLocalSyncPath(p string) error {
	if p == "" {
		return proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	for _, r := range p {
		if r == 0 || unicode.IsControl(r) {
			return proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
	}
	return nil
}

// validateRemoteSyncPath is deliberately narrower because rsync sends the
// operand through a remote shell. The supported spelling covers ordinary
// absolute, relative, and ~/ paths without relying on remote-shell quoting.
func validateRemoteSyncPath(p string) error {
	if p == "" {
		return proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if strings.HasPrefix(p, "-") {
		return proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	for _, r := range p {
		if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') &&
			!(r >= '0' && r <= '9') && r != '.' && r != '_' && r != '-' &&
			r != '/' && r != '~' {
			return proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
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
	if resp.Ping == nil {
		return nil, missingResultError(resp)
	}
	return resp.Ping, nil
}

// CapabilityProbe asks the remote agent for its verified resource controls and
// execution profile. The result is intentionally read-only; callers can use
// it to decide whether a requested policy is supported before starting work.
func (c *Client) CapabilityProbe(ctx context.Context, host string, refresh bool) (*proto.CapabilityResult, error) {
	resolved, resolveErr := c.Hosts.Resolve(host)
	if resolveErr != nil {
		return nil, c.redactErr(resolveErr)
	}
	if !refresh {
		c.mu.Lock()
		entry, ok := c.capabilities[host]
		if ok && entry.generation == resolved.Generation && time.Now().Before(entry.expiresAt) {
			cached := cloneCapability(entry.result)
			c.mu.Unlock()
			return cached, nil
		}
		delete(c.capabilities, host)
		c.mu.Unlock()
	}
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpCapabilityProbe, Capability: &proto.CapabilityParams{Refresh: refresh}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Capability == nil {
		return nil, missingResultError(resp)
	}
	result := cloneCapability(resp.Capability)
	c.mu.Lock()
	if c.capabilities == nil {
		c.capabilities = make(map[string]capabilityCacheEntry)
	}
	c.capabilities[host] = capabilityCacheEntry{result: cloneCapability(result), generation: resolved.Generation, expiresAt: time.Now().Add(30 * time.Second)}
	c.mu.Unlock()
	return result, nil
}

func cloneCapability(in *proto.CapabilityResult) *proto.CapabilityResult {
	if in == nil {
		return nil
	}
	out := *in
	if in.Profile != nil {
		p := *in.Profile
		out.Profile = &p
	}
	return &out
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

type StorageOptions struct {
	Host           string
	Scope          string
	DryRun         bool
	MaxScanJobs    int
	MaxDeleteJobs  int
	MaxDeleteBytes int64
}

func (c *Client) StorageStatus(ctx context.Context, host, scope string) (*proto.StorageScope, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpStorageStatus, Storage: &proto.StorageParams{Scope: scope}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Storage == nil || resp.Storage.Status == nil {
		return nil, missingResultError(resp)
	}
	return resp.Storage.Status, nil
}

func (c *Client) StorageGC(ctx context.Context, opts StorageOptions) (*proto.StorageGCReport, error) {
	resp, err := c.do(ctx, opts.Host, &proto.Request{Op: proto.OpStorageGC, Storage: &proto.StorageParams{Scope: opts.Scope, DryRun: opts.DryRun, MaxScanJobs: opts.MaxScanJobs, MaxDeleteJobs: opts.MaxDeleteJobs, MaxDeleteBytes: opts.MaxDeleteBytes}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Storage == nil || resp.Storage.GC == nil {
		return nil, missingResultError(resp)
	}
	return resp.Storage.GC, nil
}

func (c *Client) StorageDoctor(ctx context.Context, host, scope string) (*proto.StorageDoctorReport, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpStorageDoctor, Storage: &proto.StorageParams{Scope: scope}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.Storage == nil || resp.Storage.Doctor == nil {
		return nil, missingResultError(resp)
	}
	return resp.Storage.Doctor, nil
}

// StateInspect returns a root-relative, non-mutating state report.
func (c *Client) StateInspect(ctx context.Context, host string) (*proto.StateResult, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpStateInspect, State: &proto.StateParams{}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.State == nil {
		return nil, missingResultError(resp)
	}
	return resp.State, nil
}

func (c *Client) StateMigrate(ctx context.Context, host string, dryRun bool) (*proto.StateResult, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpStateMigrate, State: &proto.StateParams{DryRun: dryRun}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.State == nil {
		return nil, missingResultError(resp)
	}
	return resp.State, nil
}

func (c *Client) StateRepair(ctx context.Context, host string, dryRun bool) (*proto.StateResult, error) {
	resp, err := c.do(ctx, host, &proto.Request{Op: proto.OpStateRepair, State: &proto.StateParams{DryRun: dryRun}})
	if err != nil {
		return nil, c.redactErr(err)
	}
	if resp.State == nil {
		return nil, missingResultError(resp)
	}
	return resp.State, nil
}

// JobRm deletes job records to reclaim disk.
//
// Job logs are unbounded, so a machine running batches accumulates them until the
// disk fills. Running jobs are never removed; they come back in Skipped. A job
// that was already gone comes back in Missing, not as an error.
func (c *Client) JobRm(ctx context.Context, opts JobRmOptions) (*JobRmResult, error) {
	if opts.ID == "" && opts.OlderThanSec <= 0 && opts.KeepLast <= 0 {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
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
		return nil, missingResultError(resp)
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
		return nil, missingResultError(resp)
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
	return c.disconnectWithStatus(hostName, ConnectionSecurityStatus{})
}

func (c *Client) disconnectWithStatus(hostName string, status ConnectionSecurityStatus) bool {
	c.mu.Lock()
	conn, ok := c.conns[hostName]
	c.mu.Unlock()

	if !ok {
		return false
	}
	if status.State == "" {
		status = ConnectionSecurityStatus{State: observe.SecurityCold, Generation: conn.generation}
	}
	return c.detachAndCloseConnection(hostName, conn, status)
}

// Close tears down all pooled connections.
func (c *Client) Close() {
	c.mu.Lock()
	conns := make(map[string]pooledConnection, len(c.conns))
	for name, conn := range c.conns {
		conns[name] = conn
	}
	c.conns = make(map[string]pooledConnection)
	c.mu.Unlock()

	for name, conn := range conns {
		c.closeDetachedConnection(name, conn, ConnectionSecurityStatus{
			State: observe.SecurityCold, Generation: conn.generation,
		})
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
