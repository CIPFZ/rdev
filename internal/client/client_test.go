package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

type fakeRemoteConn struct {
	host    transport.Host
	mu      sync.Mutex
	ops     []string
	closed  bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	handler func(*proto.Request) (*proto.Response, error)
	closeFn func()
}

type noStreamingConn struct{ *fakeRemoteConn }

func (c *noStreamingConn) SupportsFeature(feature proto.Feature) bool {
	return feature != proto.FeatureStreaming && proto.IsKnownFeature(feature)
}

func (f *fakeRemoteConn) Host() transport.Host   { return f.host }
func (f *fakeRemoteConn) SSHArgs() []string      { return []string{"-p", fmt.Sprint(f.host.Port)} }
func (f *fakeRemoteConn) NegotiatedVersion() int { return proto.Version }
func (f *fakeRemoteConn) SupportsFeature(feature proto.Feature) bool {
	return proto.IsKnownFeature(feature)
}
func (f *fakeRemoteConn) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	if f.closeFn != nil {
		f.closeFn()
	}
	return nil
}
func (f *fakeRemoteConn) Do(_ context.Context, req *proto.Request) (*proto.Response, error) {
	if f.entered != nil {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	f.mu.Lock()
	f.ops = append(f.ops, req.Op)
	f.mu.Unlock()
	if f.handler != nil {
		return f.handler(req)
	}
	r := &proto.Response{
		OperationID: req.OperationID, Type: proto.EventFinal,
		Seq: 1, Terminal: true, Execution: proto.StateCompleted, OK: true,
	}
	switch req.Op {
	case proto.OpExec:
		r.Exec = &proto.ExecResult{}
	case proto.OpReadFile:
		r.Read = &proto.ReadResult{EOF: true}
	case proto.OpWriteFile:
		r.Cat = &proto.WriteResult{}
	case proto.OpPing:
		r.Ping = &proto.PingResult{}
	}
	return r, nil
}

func newTestClient() *Client {
	return New(func(goos, goarch string) (*transport.AgentBinary, error) {
		return &transport.AgentBinary{Data: []byte("fake")}, nil
	})
}

func testOperation(c *Client, host string) operationIdentity {
	resolved, err := c.Hosts.Resolve(host)
	if err != nil {
		panic(err)
	}
	return operationIdentity{
		Scope: secrets.Scope(resolved.Scope), Host: secretHostIdentity(resolved), State: c.Hosts.State(host),
	}
}

func testHostSecretKey(c *Client, host, name string) secrets.Key {
	identity := testOperation(c, host)
	return secrets.HostKey(identity.Scope, identity.Host, name)
}

func TestExecRejectsEmptyArgv(t *testing.T) {
	c := newTestClient()
	// Checked before any connection is attempted, so an obvious mistake fails
	// fast rather than after an ssh round trip.
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "user@h", Argv: nil}); err == nil {
		t.Error("empty argv should be rejected")
	}
}

func TestJobStartRejectsEmptyArgv(t *testing.T) {
	c := newTestClient()
	if _, err := c.JobStart(t.Context(), JobStartOptions{Host: "user@h"}); err == nil {
		t.Error("empty argv should be rejected")
	}
}

func TestBuildExecParamsInheritsSessionState(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "user@h"})
	c.Hosts.Update("dev", func(s *session.State) {
		s.Cwd = "~/nexus"
		s.Env = map[string]string{"BASE": "1"}
	})

	// Per-call values win; unset ones fall back to the sticky session state.
	params, err := c.buildExecParams(testOperation(c, "dev"), []string{"pwd"}, "", map[string]string{"EXTRA": "2"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if params.Cwd != "~/nexus" {
		t.Errorf("Cwd = %q, want the session cwd ~/nexus", params.Cwd)
	}
	if params.Env["BASE"] != "1" || params.Env["EXTRA"] != "2" {
		t.Errorf("Env = %v, want both session and per-call entries", params.Env)
	}
	if !params.LoginShell {
		t.Error("LoginShell should inherit the session default of true")
	}
}

func TestBuildExecParamsPerCallOverrides(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "user@h"})
	c.Hosts.Update("dev", func(s *session.State) { s.Cwd = "~/nexus" })

	no := false
	params, err := c.buildExecParams(testOperation(c, "dev"), []string{"pwd"}, "/tmp", nil, &no)
	if err != nil {
		t.Fatal(err)
	}
	if params.Cwd != "/tmp" {
		t.Errorf("Cwd = %q, want the per-call /tmp", params.Cwd)
	}
	if params.LoginShell {
		t.Error("explicit login_shell=false should override the session default")
	}
}

func TestBuildExecParamsResolvesSecretRef(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "user@h"})
	c.Secrets.Set(testHostSecretKey(c, "dev", "tok"), "realvalue123")

	params, err := c.buildExecParams(testOperation(c, "dev"), []string{"env"}, "", map[string]string{"T": "secret:tok"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The plaintext reaches the remote environment without appearing in the
	// caller's request.
	if params.Env["T"] != "realvalue123" {
		t.Errorf("Env[T] = %q, want the resolved secret", params.Env["T"])
	}
}

func TestBuildExecParamsUnknownSecretErrors(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "user@h"})

	_, err := c.buildExecParams(testOperation(c, "dev"), []string{"env"}, "", map[string]string{"T": "secret:missing"}, nil)
	if err == nil {
		t.Error("an unknown secret reference should error")
	}
}

func TestSyncRejectsMissingPaths(t *testing.T) {
	c := newTestClient()
	if _, err := c.Sync(t.Context(), SyncOptions{Host: "user@h", Direction: "push"}); err == nil {
		t.Error("missing local/remote paths should be rejected")
	}
}

func TestSyncDeleteRequiresExplicitConfirmation(t *testing.T) {
	c := newTestClient()
	_, err := c.Sync(t.Context(), SyncOptions{Host: "user@h", Direction: "push", Local: t.TempDir(), Remote: "dst", Delete: true})
	var env *proto.ErrorEnvelope
	if err == nil || !errors.As(err, &env) || env.Code != proto.CodeInvalidRequest {
		t.Fatalf("delete without confirmation error = %v", err)
	}
}

func TestBuildSyncArgsPolicies(t *testing.T) {
	args := buildSyncArgs(transport.Host{Addr: "u@h"}, nil, SyncOptions{Direction: "push", Local: "src", Remote: "dst", SymlinkPolicy: "follow", ConflictPolicy: "skip"})
	joined := strings.Join(args, " ")
	for _, want := range []string{"--copy-links", "--ignore-existing"} {
		if !strings.Contains(joined, want) {
			t.Errorf("args %q missing %s", joined, want)
		}
	}
}

func TestSyncConflictFailRunsReadOnlyPreflightAndRefusesUpdate(t *testing.T) {
	c := syncTestClient(t)
	local := t.TempDir()
	if err := os.WriteFile(filepath.Join(local, "file"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls int
	c.rsync = func(_ context.Context, args []string, stdout, _ io.Writer) error {
		calls++
		if !containsArg(args, "--itemize-changes") || !containsArg(args, "--dry-run") {
			t.Fatalf("preflight args missing read-only gates: %v", args)
		}
		_, _ = io.WriteString(stdout, ">f.st...... file\n")
		return nil
	}
	_, err := c.Sync(t.Context(), SyncOptions{
		Host: "dev", Direction: "push", Local: local, Remote: "dst", ConflictPolicy: "fail",
	})
	var env *proto.ErrorEnvelope
	if err == nil || !errors.As(err, &env) || env.Code != proto.CodeInvalidRequest {
		t.Fatalf("conflict fail error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("mutating rsync ran after conflict preflight: calls=%d", calls)
	}
}

func TestSyncConflictFailAllowsOnlyNewPathsThenRunsTransfer(t *testing.T) {
	c := syncTestClient(t)
	local := t.TempDir()
	if err := os.WriteFile(filepath.Join(local, "file"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls int
	c.rsync = func(_ context.Context, args []string, stdout, _ io.Writer) error {
		calls++
		if calls == 1 {
			if !containsArg(args, "--itemize-changes") || !containsArg(args, "--dry-run") {
				t.Fatalf("preflight args missing read-only gates: %v", args)
			}
			_, _ = io.WriteString(stdout, ">f+++++++++ file\n")
			return nil
		}
		if containsArg(args, "--itemize-changes") {
			t.Fatalf("mutating transfer unexpectedly retained preflight args: %v", args)
		}
		return nil
	}
	if _, err := c.Sync(t.Context(), SyncOptions{
		Host: "dev", Direction: "push", Local: local, Remote: "dst", ConflictPolicy: "fail",
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want preflight plus transfer", calls)
	}
}

func TestSyncConflictFailRefusesTruncatedPreflightPlan(t *testing.T) {
	c := syncTestClient(t)
	local := t.TempDir()
	if err := os.WriteFile(filepath.Join(local, "file"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls int
	c.rsync = func(_ context.Context, _ []string, stdout, _ io.Writer) error {
		calls++
		// Hide the conflict beyond the bounded preflight capture. The client must
		// still fail closed based on the truncation ledger.
		_, _ = io.WriteString(stdout, strings.Repeat("diagnostic\n", 256))
		_, _ = io.WriteString(stdout, ">f.st...... file\n")
		return nil
	}
	_, err := c.Sync(t.Context(), SyncOptions{
		Host: "dev", Direction: "push", Local: local, Remote: "dst", ConflictPolicy: "fail", MaxOutputBytes: 1024,
	})
	var env *proto.ErrorEnvelope
	if err == nil || !errors.As(err, &env) || env.Code != proto.CodeInvalidRequest {
		t.Fatalf("truncated preflight error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("mutating rsync ran after truncated preflight: calls=%d", calls)
	}
}

func TestSyncPlanHasConflicts(t *testing.T) {
	for _, tt := range []struct {
		name string
		out  string
		want bool
	}{
		{name: "new file", out: ">f+++++++++ file\n", want: false},
		{name: "new directory", out: "cd+++++++++ dir/\n", want: false},
		{name: "delete", out: "*deleting   old\n", want: false},
		{name: "existing update", out: ">f.st...... file\n", want: true},
		{name: "diagnostic", out: "sending incremental file list\n", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := syncPlanHasConflicts([]byte(tt.out)); got != tt.want {
				t.Fatalf("syncPlanHasConflicts(%q)=%v, want %v", tt.out, got, tt.want)
			}
		})
	}
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func TestBuildSyncManifestDeterministicAndSymlinkPolicy(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, "a"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a", filepath.Join(d, "link")); err != nil {
		t.Fatal(err)
	}
	a, err := buildSyncManifest(d, "preserve")
	if err != nil {
		t.Fatal(err)
	}
	b, err := buildSyncManifest(d, "preserve")
	if err != nil || a != b {
		t.Fatalf("manifest not deterministic: %+v %+v err=%v", a, b, err)
	}
	c, err := buildSyncManifest(d, "skip")
	if err != nil || c.Entries >= a.Entries {
		t.Fatalf("skip symlink manifest = %+v preserve=%+v err=%v", c, a, err)
	}
}

func TestBuildSyncManifestFollowRejectsEscapingSymlink(t *testing.T) {
	d := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(d, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := buildSyncManifest(d, "follow"); err == nil {
		t.Fatal("follow policy must reject a symlink escaping the sync root")
	}
}

func TestSyncDeleteRequiresBoundedPlanAndReturnsDigest(t *testing.T) {
	c := syncTestClient(t)
	local := t.TempDir()
	if err := os.WriteFile(filepath.Join(local, "file"), []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	var calls int
	c.rsync = func(_ context.Context, args []string, stdout, _ io.Writer) error {
		calls++
		if calls == 1 {
			if !containsArg(args, "--dry-run") || !containsArg(args, "--itemize-changes") {
				t.Fatalf("delete preflight missing bounded plan flags: %v", args)
			}
			_, _ = io.WriteString(stdout, ">f+++++++++ file\n")
			return nil
		}
		if containsArg(args, "--dry-run") || containsArg(args, "--itemize-changes") {
			t.Fatalf("mutating transfer retained preflight flags: %v", args)
		}
		return nil
	}
	res, err := c.Sync(t.Context(), SyncOptions{Host: "dev", Direction: "push", Local: local, Remote: "dst", Delete: true, ConfirmDelete: true})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || res.PlanDigest == "" {
		t.Fatalf("calls=%d plan_digest=%q, want two calls and digest", calls, res.PlanDigest)
	}
}

func TestSyncRejectsSourceChangedDuringTransfer(t *testing.T) {
	c := syncTestClient(t)
	local := t.TempDir()
	path := filepath.Join(local, "file")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	c.rsync = func(_ context.Context, _ []string, _, _ io.Writer) error {
		return os.WriteFile(path, []byte("new"), 0o600)
	}
	res, err := c.Sync(t.Context(), SyncOptions{Host: "dev", Direction: "push", Local: local, Remote: "dst"})
	if err == nil || res == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("source mutation result=%+v err=%v, want conflict error with result", res, err)
	}
}

func TestBuildSyncArgsTerminatesOptionsBeforeOperands(t *testing.T) {
	opts := SyncOptions{
		Direction: "push", Local: "-leading-local", Remote: "~/dst",
		Exclude: []string{".git"}, DryRun: true,
	}
	if err := validateLocalSyncPath(opts.Local); err != nil {
		t.Fatalf("leading '-' is safe after --: %v", err)
	}
	args := buildSyncArgs(transport.Host{Addr: "u@h"}, []string{"-o", "BatchMode=yes"}, opts)
	wantTail := []string{"--", "-leading-local", "u@h:~/dst"}
	if len(args) < len(wantTail) || !reflect.DeepEqual(args[len(args)-len(wantTail):], wantTail) {
		t.Errorf("rsync operand tail = %v, want suffix %v", args, wantTail)
	}
}

func TestSyncPathValidationRejectsRemoteShellSyntax(t *testing.T) {
	for _, remote := range []string{
		"-server-option", "~/with space", "~/$(touch-pwned)", "~/`id`", "~/x;id", "~/x\nnext",
	} {
		if err := validateRemoteSyncPath(remote); err == nil {
			t.Errorf("remote path %q should fail", remote)
		}
	}
	for _, local := range []string{"local\npath", "local\x00path"} {
		if err := validateLocalSyncPath(local); err == nil {
			t.Errorf("local path %q should fail", local)
		}
	}
}

func TestSyncRejectsBadDirection(t *testing.T) {
	c := newTestClient()
	_, err := c.Sync(t.Context(), SyncOptions{
		Host: "bareword-unknown", Direction: "sideways", Local: "a", Remote: "b",
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeInvalidRequest || envelope.ExecutionState != proto.StateNotSent {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSyncStreamsIntoBoundedBinarySafeCaptures(t *testing.T) {
	c := syncTestClient(t)
	stdoutChunk := append([]byte("中文"), 0xff, 0x00, 'x')
	stderrChunk := []byte{0xfe, 'e', 'r', 'r', '\n'}
	const writes = 4096
	c.rsync = func(_ context.Context, _ []string, stdout, stderr io.Writer) error {
		for i := 0; i < writes; i++ {
			if _, err := stdout.Write(stdoutChunk); err != nil {
				return err
			}
			if _, err := stderr.Write(stderrChunk); err != nil {
				return err
			}
		}
		return nil
	}
	res, err := c.Sync(t.Context(), SyncOptions{
		Host: "dev", Direction: "push", Local: "local", Remote: "remote", MaxOutputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	stdout, decodeErr := base64.StdEncoding.DecodeString(res.Stdout)
	if decodeErr != nil || !res.StdoutB64 || len(stdout) != 1024 {
		t.Fatalf("stdout retained=%d b64=%v decode=%v", len(stdout), res.StdoutB64, decodeErr)
	}
	stderr, decodeErr := base64.StdEncoding.DecodeString(res.Stderr)
	if decodeErr != nil || !res.StderrB64 || len(stderr) != 1024 {
		t.Fatalf("stderr retained=%d b64=%v decode=%v", len(stderr), res.StderrB64, decodeErr)
	}
	wantStdout := int64(writes * len(stdoutChunk))
	wantStderr := int64(writes * len(stderrChunk))
	if !res.Truncated || res.StdoutTruncation.OriginalBytes != wantStdout ||
		res.StdoutTruncation.RetainedBytes != 1024 || res.StdoutTruncation.DroppedBytes != wantStdout-1024 ||
		res.StderrTruncation.OriginalBytes != wantStderr || res.StderrTruncation.RetainedBytes != 1024 ||
		res.StderrTruncation.DroppedBytes != wantStderr-1024 {
		t.Fatalf("sync truncation accounting = %+v / %+v", res.StdoutTruncation, res.StderrTruncation)
	}
}

func TestSyncBinaryCaptureRedactsBeforeBase64Projection(t *testing.T) {
	c := syncTestClient(t)
	const token = "sync-binary-secret-value"
	if err := c.Secrets.Set(secrets.OutputKey("tok"), token); err != nil {
		t.Fatal(err)
	}
	c.rsync = func(_ context.Context, _ []string, stdout, stderr io.Writer) error {
		_, _ = stdout.Write(append([]byte{0xff, 0x00}, []byte(token)...))
		return nil
	}
	res, err := c.Sync(t.Context(), SyncOptions{
		Host: "dev", Direction: "push", Local: "local", Remote: "remote", MaxOutputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(res.Stdout)
	if err != nil || !res.StdoutB64 {
		t.Fatalf("binary projection b64=%v decode=%v", res.StdoutB64, err)
	}
	if bytes.Contains(decoded, []byte(token)) || !bytes.Contains(decoded, []byte("<redacted:tok>")) {
		t.Fatalf("binary projection was not redacted before base64: %q", decoded)
	}
}

func TestSyncOutputLimitRejectsNegativeAndAboveHardCap(t *testing.T) {
	c := newTestClient()
	for _, limit := range []int64{-1, proto.AbsoluteOutputBytes + 1, int64(^uint64(0) >> 1)} {
		_, err := c.Sync(t.Context(), SyncOptions{
			Host: "dev", Direction: "push", Local: "local", Remote: "remote", MaxOutputBytes: limit,
		})
		var envelope *proto.ErrorEnvelope
		if !errors.As(err, &envelope) || envelope.Code != proto.CodeLimitExceeded || envelope.ExecutionState != proto.StateNotSent {
			t.Fatalf("limit %d error = %v", limit, err)
		}
	}
}

func TestBoundedCaptureAllocationNeverExceedsRetentionCap(t *testing.T) {
	capture := newBoundedCapture(1024)
	payload := bytes.Repeat([]byte("x"), 1<<20)
	if n, err := capture.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("drain write n=%d err=%v", n, err)
	}
	capture.mu.Lock()
	retained, allocated, total := len(capture.buf), cap(capture.buf), capture.total
	capture.mu.Unlock()
	if retained != 1024 || allocated != 1024 || total != int64(len(payload)) {
		t.Fatalf("retained=%d allocated=%d total=%d", retained, allocated, total)
	}
}

func TestSyncContextCancelStopsStreamingRunner(t *testing.T) {
	c := syncTestClient(t)
	entered := make(chan struct{})
	c.rsync = func(ctx context.Context, _ []string, stdout, stderr io.Writer) error {
		close(entered)
		chunk := make([]byte, 32<<10)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				_, _ = stdout.Write(chunk)
				_, _ = stderr.Write(chunk)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.Sync(ctx, SyncOptions{Host: "dev", Direction: "push", Local: "local", Remote: "remote"})
		done <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("sync cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sync did not stop after context cancellation")
	}
}

func TestBusinessErrorEnvelopeSurvivesClientBoundary(t *testing.T) {
	tests := []struct {
		name  string
		code  proto.ErrorCode
		state proto.ExecutionState
		call  func(*Client) error
	}{
		{
			name: "output_limit", code: proto.CodeLimitExceeded, state: proto.StateNotSent,
			call: func(c *Client) error {
				_, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}, MaxOutputBytes: 600000})
				return err
			},
		},
		{
			name: "missing_file", code: proto.CodeObjectNotFound, state: proto.StateFailed,
			call: func(c *Client) error {
				_, err := c.ReadFile(t.Context(), "dev", "private/path", 0, 0)
				return err
			},
		},
		{
			name: "missing_job", code: proto.CodeObjectNotFound, state: proto.StateFailed,
			call: func(c *Client) error {
				_, err := c.JobStatus(t.Context(), "dev", "private-job")
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient()
			host := transport.Host{Name: "dev", Addr: "u@h"}
			if err := c.Hosts.Add(host); err != nil {
				t.Fatal(err)
			}
			c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
				return &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
					envelope := proto.NewError(tt.code, req.OperationID, tt.state)
					response := &proto.Response{
						ID: req.ID, OperationID: req.OperationID, Type: proto.EventError,
						Seq: 1, Terminal: true, Execution: tt.state, Error: envelope,
					}
					return response, envelope
				}}, nil
			}
			err := tt.call(c)
			var envelope *proto.ErrorEnvelope
			if !errors.As(err, &envelope) {
				t.Fatalf("client error = %v", err)
			}
			descriptor, _ := proto.LookupError(tt.code)
			if envelope.Code != tt.code || envelope.Category != descriptor.Category ||
				envelope.Retry != descriptor.Retry || envelope.Retryable != descriptor.Retryable ||
				envelope.ExecutionState != tt.state || envelope.OperationID == "" || !envelope.Terminal {
				t.Fatalf("client envelope = %+v", envelope)
			}
			if strings.Contains(envelope.Message, "private") {
				t.Fatalf("client envelope leaked private input: %+v", envelope)
			}
		})
	}
}

func TestContextDeadlineOnlyProjectsToDeadlineCapableOperation(t *testing.T) {
	c := newTestClient()
	host := transport.Host{Name: "dev", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	var seen []proto.Request
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
			seen = append(seen, *req)
			response := &proto.Response{
				OperationID: req.OperationID, Type: proto.EventFinal, Seq: 1,
				Terminal: true, Execution: proto.StateCompleted, OK: true,
			}
			if req.Op == proto.OpReadFile {
				response.Read = &proto.ReadResult{}
			} else {
				response.Exec = &proto.ExecResult{}
			}
			return response, nil
		}}, nil
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	if _, err := c.ReadFile(ctx, "dev", "synthetic", 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0].DeadlineUnixMilli != 0 || seen[1].DeadlineUnixMilli == 0 {
		t.Fatalf("projected deadlines: %+v", seen)
	}
}

func TestExplicitDeadlineRejectedForUnsupportedOperation(t *testing.T) {
	c := newTestClient()
	host := transport.Host{Name: "dev", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	called := false
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
			called = true
			return nil, nil
		}}, nil
	}
	_, err := c.do(context.Background(), "dev", &proto.Request{
		Op: proto.OpReadFile, Read: &proto.ReadParams{Path: "synthetic"}, DeadlineUnixMilli: 42,
	})
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeInvalidRequest || called {
		t.Fatalf("unsupported explicit deadline error=%v called=%v", err, called)
	}
}

func TestV3WithoutStreamingKeepsTypedOperationIdentity(t *testing.T) {
	c := newTestClient()
	host := transport.Host{Name: "dev", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	var seen proto.Request
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		base := &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
			seen = *req
			return &proto.Response{
				ID: req.ID, OperationID: req.OperationID, Type: proto.EventFinal,
				Seq: 1, Terminal: true, Execution: proto.StateCompleted, OK: true,
				Read: &proto.ReadResult{OperationID: req.OperationID, Terminal: true, Execution: proto.StateCompleted},
			}, nil
		}}
		return &noStreamingConn{fakeRemoteConn: base}, nil
	}
	if _, err := c.ReadFile(t.Context(), "dev", "synthetic", 0, 1); err != nil {
		t.Fatal(err)
	}
	if seen.OperationID == "" || seen.ClientID == "" || seen.StreamWindowBytes != 0 {
		t.Fatalf("v3 no-streaming request = %+v", seen)
	}
}

func syncTestClient(t *testing.T) *Client {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "rsync"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	c := newTestClient()
	host := transport.Host{Name: "dev", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: host}, nil
	}
	return c
}

func TestRedactErrScrubsSecrets(t *testing.T) {
	c := newTestClient()
	c.Secrets.Set(secrets.OutputKey("tok"), "82d9b49359b262b40bdbbfa844891b5e")

	err := c.redactErr(errFromString("failed with token 82d9b49359b262b40bdbbfa844891b5e"))
	if strings.Contains(err.Error(), "82d9b493") {
		t.Errorf("secret leaked through error: %v", err)
	}
	if !strings.Contains(err.Error(), "<redacted:tok>") {
		t.Errorf("expected a placeholder, got %v", err)
	}
}

func TestRedactErrNil(t *testing.T) {
	c := newTestClient()
	if got := c.redactErr(nil); got != nil {
		t.Errorf("redactErr(nil) = %v, want nil", got)
	}
}

type stringError string

func (e stringError) Error() string { return string(e) }

func errFromString(s string) error { return stringError(s) }

func TestIsConnectedDoesNotRegisterHosts(t *testing.T) {
	c := New(func(a, b string) (*transport.AgentBinary, error) { return nil, nil })
	before := len(c.Hosts.Names())

	// Hosts.Host auto-registers anything shaped like an ssh destination, so
	// routing these through it would turn a status query on a typo into a
	// permanent phantom entry in the rdev_session listing.
	c.IsConnected("user@1.2.3.4")
	c.Disconnect("user@5.6.7.8")

	after := c.Hosts.Names()
	if len(after) != before {
		t.Errorf("registry grew from %d to %d: %v", before, len(after), after)
	}
}

// A configured secret path must be read and registered, so redaction is live for
// the first command rather than only after a manual rdev_secrets call.
func TestLoadHostSecretsRegistersFromPath(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	const token = "s3cret-token-value-abcdef"
	os.WriteFile(keyPath, []byte(token+"\n"), 0o600)

	c := New(func(a, b string) (*transport.AgentBinary, error) { return nil, nil })
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(s *session.State) {
		s.Secrets = map[string]string{"tok": keyPath}
	})

	// Read locally: SetSecretFromRemoteFile needs a live agent, so exercise the
	// store's own path-reading here and check redaction end to end.
	if err := c.Secrets.SetFromFile(testHostSecretKey(c, "dev", "tok"), keyPath); err != nil {
		t.Fatal(err)
	}
	got := c.Secrets.Redact("output containing " + token + " inline")
	if strings.Contains(got, token) {
		t.Errorf("token not redacted: %q", got)
	}
	if !strings.Contains(got, "<redacted:tok>") {
		t.Errorf("want placeholder, got %q", got)
	}
}

// An explicit registration must win over the configured path: a caller who just
// set a value should not have it replaced on the next reconnect.
//
// The value assertion alone proves nothing here -- a failed refetch is deliberately
// quiet and leaves the old value in place, so "skipped" and "tried and lost" look
// identical. Confirmed by mutation: with the guard deleted this test still passed.
// The warning is the observable, so both are checked.
func TestLoadHostSecretsKeepsExplicitValue(t *testing.T) {
	c := New(func(a, b string) (*transport.AgentBinary, error) { return nil, nil })
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(s *session.State) {
		s.Secrets = map[string]string{"tok": "/nonexistent/path"}
	})
	if err := c.Secrets.Set(testHostSecretKey(c, "dev", "tok"), "explicitly-set-value"); err != nil {
		t.Fatal(err)
	}

	// Would try to read the bogus path if it did not skip already-registered names.
	resolved, _ := c.Hosts.Resolve("dev")
	if _, _, err := c.loadHostSecrets(context.Background(), resolved, c.Hosts.State("dev"), nil); err != nil {
		t.Fatal(err)
	}

	if v, _ := c.Secrets.Get(testHostSecretKey(c, "dev", "tok")); v != "explicitly-set-value" {
		t.Errorf("value = %q, want the explicit registration preserved", v)
	}
}

// Every string a caller gets back has to be redacted, not just the obvious streams.
// SyncResult.Command echoes the assembled rsync argv, and argv is caller-supplied --
// an --exclude pattern or a path can carry a credential. It shipped unredacted while
// Stdout and Stderr, one struct literal above it, were scrubbed.
//
// Asserted per field via reflection rather than by listing the fields, so a field
// added later is covered without anyone remembering to extend this test. That is the
// actual defect class here: redaction applied per field instead of at the boundary.
func TestSyncResultRedactsEveryStringField(t *testing.T) {
	c := newTestClient()
	const tok = "s3cret-passphrase-value-123456"
	if err := c.Secrets.Set(secrets.OutputKey("tok"), tok); err != nil {
		t.Fatal(err)
	}

	// Shaped like what Sync builds: the secret arrives through --exclude.
	args := []string{"-az", "-v", "--exclude", tok, "/local", "h:/remote"}
	res := &SyncResult{
		Stdout:  c.Secrets.Redact("sending incremental file list " + tok),
		Stderr:  c.Secrets.Redact("rsync warning near " + tok),
		Command: c.Secrets.Redact("rsync " + strings.Join(args, " ")),
		DryRun:  true,
	}

	v := reflect.ValueOf(*res)
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if f.Type.Kind() != reflect.String {
			continue
		}
		got := v.Field(i).String()
		if strings.Contains(got, tok) {
			t.Errorf("SyncResult.%s carries the secret verbatim: %q", f.Name, got)
		}
	}
}

// The same gap in Exec: Cwd is echoed back from the request, so a path under a
// credential directory would surface a registered value next to redacted output.
func TestExecResultRedactsCwd(t *testing.T) {
	c := newTestClient()
	const tok = "credential-dir-name-abcdefgh"
	if err := c.Secrets.Set(secrets.OutputKey("tok"), tok); err != nil {
		t.Fatal(err)
	}
	if got := c.Secrets.Redact("/home/u/" + tok + "/work"); strings.Contains(got, tok) {
		t.Errorf("a cwd carrying a secret must be redacted, got %q", got)
	}
}

// redactJob scrubbed only Argv, leaving Label and Cwd -- both caller-supplied -- in
// the clear. Found by chasing this function's zero coverage, and it is the same
// omission as SyncResult.Command: per-field scrubbing misses the fields nobody
// thought about. The MCP boundary backstops it now, but the CLI calls this package
// directly and never passes through that, so it has to be right here too.
//
// Walks every string field by reflection so a field added to JobInfo later is
// covered without anyone remembering this test exists.
func TestRedactJobScrubsEveryStringField(t *testing.T) {
	c := newTestClient()
	const tok = "job-field-credential-value-99"
	if err := c.Secrets.Set(secrets.OutputKey("tok"), tok); err != nil {
		t.Fatal(err)
	}

	j := c.redactJob(&proto.JobInfo{
		ID:        "j1",
		Label:     "run-" + tok,
		Argv:      []string{"./x", "--token", tok},
		Cwd:       "/data/" + tok,
		State:     "running",
		StartedAt: "2026-01-01T00:00:00Z",
	})

	v := reflect.ValueOf(*j)
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		switch f.Type.Kind() {
		case reflect.String:
			if got := v.Field(i).String(); strings.Contains(got, tok) {
				t.Errorf("JobInfo.%s carries the secret: %q", f.Name, got)
			}
		case reflect.Slice:
			if f.Type.Elem().Kind() != reflect.String {
				continue
			}
			for k := 0; k < v.Field(i).Len(); k++ {
				if got := v.Field(i).Index(k).String(); strings.Contains(got, tok) {
					t.Errorf("JobInfo.%s[%d] carries the secret: %q", f.Name, k, got)
				}
			}
		}
	}
}

func TestRedactJobNil(t *testing.T) {
	if got := newTestClient().redactJob(nil); got != nil {
		t.Errorf("redactJob(nil) = %v, want nil", got)
	}
}

// A host with no configured secrets must not touch the network at all.
func TestLoadHostSecretsNoopWithoutConfig(t *testing.T) {
	c := New(func(a, b string) (*transport.AgentBinary, error) { return nil, nil })
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	resolved, _ := c.Hosts.Resolve("dev")
	if _, _, err := c.loadHostSecrets(context.Background(), resolved, c.Hosts.State("dev"), nil); err != nil {
		t.Fatal(err)
	}
	if n := len(c.Secrets.Names()); n != 0 {
		t.Errorf("registered %d secrets from an empty config", n)
	}
}

// Redaction must survive a reconnect. `do` drops a dead connection and dials
// again, and the store is per-process rather than per-connection, so a value
// registered before the drop still redacts after it. This is the invariant that
// matters most in the whole package: if it broke, output would keep flowing and
// the only visible change would be a plaintext credential in the transcript.
//
// Asserted at the store level because reaching the reconnect branch needs a live
// agent. What is checked is the property that makes the branch safe -- the store
// is not attached to the connection -- so a refactor that moved it there fails here.
func TestSecretsSurviveConnectionLoss(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	const token = "token-that-must-stay-hidden"
	if err := c.Secrets.Set(testHostSecretKey(c, "dev", "tok"), token); err != nil {
		t.Fatal(err)
	}

	// Stand in for the pooled connection being discarded as dead: this is exactly
	// what do() does before redialing.
	c.mu.Lock()
	delete(c.conns, "dev")
	c.mu.Unlock()

	if got := c.Secrets.Redact("leaked " + token); strings.Contains(got, token) {
		t.Errorf("token survived into output after connection loss: %q", got)
	}
}

// The reload path runs on every dial, including the one after a drop, so it must
// skip names that are already registered rather than refetching them.
//
// Asserted through the warning rather than the stored value, which was the first
// version of this test and was worthless: failures here are deliberately quiet, so
// the value survives whether the guard skipped the fetch or attempted it and lost.
// A mutation run confirmed the value-only assertion passed with the guard deleted.
// The warning is the one externally visible difference.
func TestLoadHostSecretsSkipsAlreadyRegistered(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	const token = "reconnect-safe-token-value"
	if err := os.WriteFile(keyPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(s *session.State) {
		s.Secrets = map[string]string{"tok": keyPath}
	})
	if err := c.Secrets.SetFromFile(testHostSecretKey(c, "dev", "tok"), keyPath); err != nil {
		t.Fatal(err)
	}

	// Second pass, as a reconnect would trigger. Reaching the fetch would need a
	// live agent, so it would fail and warn -- silence is the proof it was skipped.
	resolved, _ := c.Hosts.Resolve("dev")
	if _, _, err := c.loadHostSecrets(context.Background(), resolved, c.Hosts.State("dev"), nil); err != nil {
		t.Fatal(err)
	}

	if names := c.Secrets.Names(); len(names) != 1 {
		t.Errorf("names = %v, want exactly one registration", names)
	}
	if v, _ := c.Secrets.Get(testHostSecretKey(c, "dev", "tok")); v != token {
		t.Errorf("value = %q, want the original token preserved", v)
	}
}

func TestAliasIdentityUpdateEvictsEverySink(t *testing.T) {
	c := newTestClient()
	var mu sync.Mutex
	var dialed []*fakeRemoteConn
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		f := &fakeRemoteConn{host: h}
		mu.Lock()
		dialed = append(dialed, f)
		mu.Unlock()
		return f, nil
	}
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@one", Port: 22, RemoteDir: "one"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}

	checks := []struct {
		host transport.Host
		call func() error
	}{
		{transport.Host{Name: "dev", Addr: "u@two", Port: 22, RemoteDir: "one"}, func() error {
			_, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}})
			return err
		}},
		{transport.Host{Name: "dev", Addr: "u@two", Port: 2200, RemoteDir: "one"}, func() error { _, err := c.ReadFile(t.Context(), "dev", "x", 0, 1); return err }},
		{transport.Host{Name: "dev", Addr: "u@two", Port: 2200, RemoteDir: "two"}, func() error {
			_, err := c.WriteFile(t.Context(), WriteFileOptions{Host: "dev", Path: "x", Content: "x"})
			return err
		}},
	}
	for _, check := range checks {
		if err := c.Hosts.Add(check.host); err != nil {
			t.Fatal(err)
		}
		if err := check.call(); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		got := dialed[len(dialed)-1].host
		mu.Unlock()
		if got.Addr != check.host.Addr || got.Port != check.host.Port || got.RemoteDir != check.host.RemoteDir {
			t.Fatalf("dialed stale identity %+v, want %+v", got, check.host)
		}
	}

	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "rsync"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	syncHost := transport.Host{Name: "dev", Addr: "u@sync", Port: 2299, RemoteDir: "sync"}
	if err := c.Hosts.Add(syncHost); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Sync(t.Context(), SyncOptions{Host: "dev", Local: "local", Remote: "remote"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := dialed[len(dialed)-1].host
	all := append([]*fakeRemoteConn(nil), dialed...)
	mu.Unlock()
	if got != syncHost {
		t.Fatalf("sync used stale identity %+v, want %+v", got, syncHost)
	}
	for i, old := range all[:len(all)-1] {
		old.mu.Lock()
		closed := old.closed
		old.mu.Unlock()
		if !closed {
			t.Errorf("old connection %d was not evicted", i)
		}
	}
}

func TestIdentityUpdateWaitsForSecurityInitializationLease(t *testing.T) {
	c := newTestClient()
	started, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var dialed []*fakeRemoteConn
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		f := &fakeRemoteConn{host: h}
		mu.Lock()
		dialed = append(dialed, f)
		n := len(dialed)
		mu.Unlock()
		if n == 1 {
			close(started)
			<-release
		}
		return f, nil
	}
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@old"})
	done := make(chan error, 1)
	go func() {
		_, err := c.Exec(context.Background(), ExecOptions{Host: "dev", Argv: []string{"true"}})
		done <- err
	}()
	<-started
	updateDone := make(chan error, 1)
	go func() { updateDone <- c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@new"}) }()
	select {
	case err := <-updateDone:
		t.Fatalf("identity update bypassed initialization lease: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-updateDone; err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dialed) != 2 || dialed[1].host.Addr != "u@new" {
		t.Fatalf("dial sequence=%+v", dialed)
	}
	dialed[0].mu.Lock()
	closed := dialed[0].closed
	dialed[0].mu.Unlock()
	if !closed {
		t.Fatal("superseded in-flight connection was published")
	}
}

func TestProjectApprovalEvictsWarmAlias(t *testing.T) {
	home, project := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)
	if err := os.MkdirAll(filepath.Join(home, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(project, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".rdev", "hosts.json"), []byte(`{"hosts":[{"name":"dev","addr":"u@old"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, ".rdev", "hosts.json"), []byte(`{"hosts":[{"name":"dev","addr":"u@approved","port":2200,"remote_dir":"approved"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newTestClient()
	var pending *session.UntrustedProjectError
	if err := c.Hosts.Load(); !errors.As(err, &pending) {
		t.Fatalf("Load=%v", err)
	}
	var mu sync.Mutex
	var dialed []*fakeRemoteConn
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		f := &fakeRemoteConn{host: h}
		mu.Lock()
		dialed = append(dialed, f)
		mu.Unlock()
		return f, nil
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Hosts.ApproveProject(pending.Trust.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dialed) != 2 || dialed[1].host.Addr != "u@approved" || dialed[1].host.Port != 2200 || dialed[1].host.RemoteDir != "approved" {
		t.Fatalf("approval dial sequence=%+v", dialed)
	}
	dialed[0].mu.Lock()
	closed := dialed[0].closed
	dialed[0].mu.Unlock()
	if !closed {
		t.Fatal("approval left old connection open")
	}
}

func TestIdentityLeaseCoversExecReadWriteAndSync(t *testing.T) {
	for _, op := range []string{"exec", "read", "write", "sync"} {
		t.Run(op, func(t *testing.T) {
			c := newTestClient()
			entered, release := make(chan struct{}), make(chan struct{})
			f := &fakeRemoteConn{host: transport.Host{Name: "dev", Addr: "u@old"}}
			if op != "sync" {
				f.entered, f.release = entered, release
			}
			c.dial = func(_ context.Context, _ transport.Host, _ AgentLookup) (remoteConnection, error) { return f, nil }
			if err := c.Hosts.Add(f.host); err != nil {
				t.Fatal(err)
			}
			if op == "sync" {
				bin := t.TempDir()
				if err := os.WriteFile(filepath.Join(bin, "rsync"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
					t.Fatal(err)
				}
				t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
				c.rsync = func(context.Context, []string, io.Writer, io.Writer) error { close(entered); <-release; return nil }
			}
			opDone := make(chan error, 1)
			go func() {
				var err error
				switch op {
				case "exec":
					_, err = c.Exec(context.Background(), ExecOptions{Host: "dev", Argv: []string{"true"}})
				case "read":
					_, err = c.ReadFile(context.Background(), "dev", "x", 0, 1)
				case "write":
					_, err = c.WriteFile(context.Background(), WriteFileOptions{Host: "dev", Path: "x", Content: "x"})
				case "sync":
					_, err = c.Sync(context.Background(), SyncOptions{Host: "dev", Local: "x", Remote: "x"})
				}
				opDone <- err
			}()
			<-entered
			updateDone := make(chan error, 1)
			go func() { updateDone <- c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@new"}) }()
			select {
			case err := <-updateDone:
				t.Fatalf("identity update completed during active %s sink: %v", op, err)
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			if err := <-opDone; err != nil {
				t.Fatal(err)
			}
			if err := <-updateDone; err != nil {
				t.Fatal(err)
			}
			if h, _ := c.Hosts.Host("dev"); h.Addr != "u@new" {
				t.Fatalf("published host=%+v", h)
			}
		})
	}
}

func TestHostALeaseDoesNotBlockHostBIdentityUpdate(t *testing.T) {
	c := newTestClient()
	entered, release := make(chan struct{}), make(chan struct{})
	oldA := &fakeRemoteConn{
		host:    transport.Host{Name: "a", Addr: "u@old-a"},
		entered: entered,
		release: release,
	}
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		if h.Name == "a" {
			return oldA, nil
		}
		return &fakeRemoteConn{host: h}, nil
	}
	if err := c.Hosts.Add(oldA.host); err != nil {
		t.Fatal(err)
	}
	if err := c.Hosts.Add(transport.Host{Name: "b", Addr: "u@old-b"}); err != nil {
		t.Fatal(err)
	}

	opDone := make(chan error, 1)
	go func() {
		_, err := c.Exec(context.Background(), ExecOptions{Host: "a", Argv: []string{"true"}})
		opDone <- err
	}()
	<-entered

	updateA := make(chan error, 1)
	go func() { updateA <- c.Hosts.Add(transport.Host{Name: "a", Addr: "u@new-a"}) }()
	select {
	case err := <-updateA:
		t.Fatalf("same-alias update completed during active operation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	updateB := make(chan error, 1)
	go func() { updateB <- c.Hosts.Add(transport.Host{Name: "b", Addr: "u@new-b"}) }()
	select {
	case err := <-updateB:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("host A lease blocked unrelated host B update")
	}
	if h, _ := c.Hosts.Host("b"); h.Addr != "u@new-b" {
		t.Fatalf("host B update published %+v", h)
	}

	close(release)
	if err := <-opDone; err != nil {
		t.Fatal(err)
	}
	if err := <-updateA; err != nil {
		t.Fatal(err)
	}
	if h, _ := c.Hosts.Host("a"); h.Addr != "u@new-a" {
		t.Fatalf("host A update published %+v", h)
	}
	oldA.mu.Lock()
	closed := oldA.closed
	oldA.mu.Unlock()
	if !closed {
		t.Fatal("same-alias publication did not evict old connection")
	}

	var redials int
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		redials++
		return &fakeRemoteConn{host: h}, nil
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "a", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	if redials != 1 {
		t.Fatalf("post-update executions dialed %d times, want one new identity", redials)
	}
}

func TestSameNameSecretsAreResolvedOnlyForExactHost(t *testing.T) {
	c := newTestClient()
	for _, host := range []transport.Host{{Name: "a", Addr: "u@a"}, {Name: "b", Addr: "u@b"}} {
		if err := c.Hosts.Add(host); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SetSecret("a", "tok", "host-a-secret"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetSecret("b", "tok", "host-b-secret"); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]string)
	var mu sync.Mutex
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op == proto.OpExec {
				mu.Lock()
				seen[h.Name] = req.Exec.Env["TOKEN"]
				mu.Unlock()
				return &proto.Response{OK: true, Exec: &proto.ExecResult{}}, nil
			}
			return &proto.Response{OK: true}, nil
		}}, nil
	}
	for _, host := range []string{"a", "b"} {
		if _, err := c.Exec(t.Context(), ExecOptions{Host: host, Argv: []string{"env"}, Env: map[string]string{"TOKEN": "secret:tok"}}); err != nil {
			t.Fatal(err)
		}
	}
	if seen["a"] != "host-a-secret" || seen["b"] != "host-b-secret" {
		t.Fatalf("cross-host resolution: %+v", seen)
	}
}

func TestConcurrentFirstRequestsWaitForSecureInitialization(t *testing.T) {
	c := newTestClient()
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
		t.Fatal(err)
	}
	c.Hosts.Update("dev", func(st *session.State) { st.Secrets = map[string]string{"tok": "~/token"} })
	const token = "initialization-secret-value"
	started, releaseRead := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var execs int
	var mu sync.Mutex
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			switch req.Op {
			case proto.OpReadFile:
				once.Do(func() { close(started) })
				<-releaseRead
				return &proto.Response{OK: true, Read: &proto.ReadResult{Content: token, Size: int64(len(token)), EOF: true}}, nil
			case proto.OpExec:
				mu.Lock()
				execs++
				mu.Unlock()
				return &proto.Response{OK: true, Exec: &proto.ExecResult{Stdout: token}}, nil
			default:
				return &proto.Response{OK: true}, nil
			}
		}}, nil
	}

	results := make(chan *ExecResult, 2)
	errs := make(chan error, 2)
	call := func() {
		res, err := c.Exec(context.Background(), ExecOptions{Host: "dev", Argv: []string{"true"}})
		results <- res
		errs <- err
	}
	go call()
	<-started
	go call()
	time.Sleep(30 * time.Millisecond)
	if c.IsConnected("dev") {
		t.Fatal("connection published before declared secrets finished loading")
	}
	if state := c.ConnectionSecurity("dev").State; state != observe.SecurityInitializing {
		t.Fatalf("state = %s, want initializing", state)
	}
	mu.Lock()
	before := execs
	mu.Unlock()
	if before != 0 {
		t.Fatalf("%d exec requests ran before initialization commit", before)
	}
	close(releaseRead)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		res := <-results
		if strings.Contains(res.Stdout, token) {
			t.Fatalf("secret escaped after initialization: %q", res.Stdout)
		}
	}
	if state := c.ConnectionSecurity("dev"); state.State != observe.SecurityReady || state.Loaded != 1 {
		t.Fatalf("security state = %+v", state)
	}
}

func TestInitializationFailureIsVisibleAndNeverPublished(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(st *session.State) { st.Secrets = map[string]string{"tok": "~/oversized"} })
	dials := 0
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		dials++
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			return &proto.Response{OK: true, Read: &proto.ReadResult{Content: strings.Repeat("x", maxSecretFileBytes), Size: maxSecretFileBytes + 1, EOF: false}}, nil
		}}, nil
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("initialization error = %v", err)
	}
	if c.IsConnected("dev") {
		t.Fatal("failed initialization published a connection")
	}
	status := c.ConnectionSecurity("dev")
	if status.State != observe.SecurityFailed || status.Reason != observe.ReasonSecretTruncated {
		t.Fatalf("status = %+v", status)
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err == nil {
		t.Fatal("cached failure was presented as ready")
	}
	if dials != 1 {
		t.Fatalf("unchanged failed identity redialed %d times", dials)
	}
}

func TestRemoteSecretTruncationKeepsStoreUnchanged(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	if err := c.SetSecret("dev", "tok", "original-secret-value"); err != nil {
		t.Fatal(err)
	}
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op == proto.OpReadFile {
				if req.Read.Limit != maxSecretFileBytes+1 {
					t.Fatalf("secret read limit = %d, want cap+1", req.Read.Limit)
				}
				return &proto.Response{OK: true, Read: &proto.ReadResult{Content: strings.Repeat("z", maxSecretFileBytes), Size: maxSecretFileBytes + 1, EOF: false}}, nil
			}
			return &proto.Response{OK: true}, nil
		}}, nil
	}
	if err := c.SetSecretFromRemoteFile(t.Context(), "dev", "tok", "~/too-large"); err == nil {
		t.Fatal("truncated secret was accepted")
	}
	value, ok := c.Secrets.Get(testHostSecretKey(c, "dev", "tok"))
	if !ok || value != "original-secret-value" {
		t.Fatalf("store changed to %q after truncation", value)
	}
}

func TestSecretReadBoundaryAndOneByteOver(t *testing.T) {
	exact := strings.Repeat("a", maxSecretFileBytes)
	if got, _, err := validateSecretRead(&proto.ReadResult{Content: exact, Size: maxSecretFileBytes, EOF: true}); err != nil || got != exact {
		t.Fatalf("exact boundary rejected: len=%d err=%v", len(got), err)
	}
	if _, reason, err := validateSecretRead(&proto.ReadResult{Content: exact, Size: maxSecretFileBytes + 1, EOF: false}); err == nil || reason != observe.ReasonSecretTruncated {
		t.Fatalf("one byte over accepted: reason=%s err=%v", reason, err)
	}
	// Enforce the cap from observed bytes even if stale or malicious metadata
	// claims the cap+1 read reached EOF at exactly the old size.
	if _, reason, err := validateSecretRead(&proto.ReadResult{Content: exact + "b", Size: maxSecretFileBytes, EOF: true}); err == nil || reason != observe.ReasonSecretTruncated {
		t.Fatalf("observed extra byte accepted with lying metadata: reason=%s err=%v", reason, err)
	}
	exactBinary := base64.StdEncoding.EncodeToString(make([]byte, maxSecretFileBytes))
	if len(exactBinary) <= maxSecretFileBytes {
		t.Fatalf("test probe does not exceed encoded boundary: %d", len(exactBinary))
	}
	if _, reason, err := validateSecretRead(&proto.ReadResult{
		Content: exactBinary, ContentB64: true, Size: maxSecretFileBytes, EOF: true,
	}); err == nil || reason != observe.ReasonSecretBinary {
		t.Fatalf("exact 64 KiB binary misclassified: reason=%s err=%v", reason, err)
	}
	if _, reason, err := validateSecretRead(&proto.ReadResult{
		Content: exactBinary, ContentB64: true, Size: maxSecretFileBytes + 1, EOF: true,
	}); err == nil || reason != observe.ReasonSecretTruncated {
		t.Fatalf("64 KiB+1 binary misclassified: reason=%s err=%v", reason, err)
	}
}

func TestSlowOldCloseCannotOverwriteNewReadyPublication(t *testing.T) {
	c := newTestClient()
	for _, host := range []transport.Host{{Name: "dev", Addr: "u@dev"}, {Name: "other", Addr: "u@other"}} {
		if err := c.Hosts.Add(host); err != nil {
			t.Fatal(err)
		}
	}
	closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
	devDials := 0
	c.dial = func(_ context.Context, host transport.Host, _ AgentLookup) (remoteConnection, error) {
		if host.Name != "dev" {
			return &fakeRemoteConn{host: host}, nil
		}
		devDials++
		conn := &fakeRemoteConn{host: host}
		if devDials == 1 {
			conn.closeFn = func() {
				close(closeEntered)
				<-releaseClose
			}
		}
		return conn, nil
	}
	if _, err := c.conn(t.Context(), "dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.conn(t.Context(), "other"); err != nil {
		t.Fatal(err)
	}
	disconnected := make(chan bool, 1)
	go func() { disconnected <- c.Disconnect("dev") }()
	<-closeEntered

	if _, err := c.conn(t.Context(), "dev"); err != nil {
		t.Fatal(err)
	}
	if !c.IsConnected("dev") || c.ConnectionSecurity("dev").State != observe.SecurityReady {
		t.Fatalf("new publication is not ready: connected=%v status=%+v", c.IsConnected("dev"), c.ConnectionSecurity("dev"))
	}
	otherDone := make(chan error, 1)
	go func() { _, err := c.conn(context.Background(), "other"); otherDone <- err }()
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow Close on dev blocked unrelated pooled connection")
	}

	close(releaseClose)
	if ok := <-disconnected; !ok {
		t.Fatal("old connection was not detached")
	}
	if !c.IsConnected("dev") || c.ConnectionSecurity("dev").State != observe.SecurityReady {
		t.Fatalf("old Close overwrote new publication: connected=%v status=%+v", c.IsConnected("dev"), c.ConnectionSecurity("dev"))
	}
}

func TestSlowDetachedCloseCannotRestoreGenerationBeforeHostInvalidation(t *testing.T) {
	c := newTestClient()
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@old"}); err != nil {
		t.Fatal(err)
	}
	closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
	c.dial = func(_ context.Context, host transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: host, closeFn: func() {
			close(closeEntered)
			<-releaseClose
		}}, nil
	}
	if _, err := c.conn(t.Context(), "dev"); err != nil {
		t.Fatal(err)
	}
	disconnected := make(chan bool, 1)
	go func() { disconnected <- c.Disconnect("dev") }()
	<-closeEntered

	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@new"}); err != nil {
		t.Fatal(err)
	}
	resolved, err := c.Hosts.Resolve("dev")
	if err != nil {
		t.Fatal(err)
	}
	status := c.ConnectionSecurity("dev")
	if status.State != observe.SecurityCold || status.Generation != resolved.Generation {
		t.Fatalf("host invalidation state = %+v, generation=%d", status, resolved.Generation)
	}

	close(releaseClose)
	if ok := <-disconnected; !ok {
		t.Fatal("old connection was not detached")
	}
	status = c.ConnectionSecurity("dev")
	if status.State != observe.SecurityCold || status.Generation != resolved.Generation {
		t.Fatalf("old Close restored stale generation: status=%+v generation=%d", status, resolved.Generation)
	}
}

func TestRetrySlowCloseCannotOverwriteReplacementReadyPublication(t *testing.T) {
	c := newTestClient()
	for _, host := range []transport.Host{{Name: "dev", Addr: "u@dev"}, {Name: "other", Addr: "u@other"}} {
		if err := c.Hosts.Add(host); err != nil {
			t.Fatal(err)
		}
	}
	baseline := c.Hosts.SecuritySnapshot().ConnectionSecurityTransitions
	closeEntered, releaseClose := make(chan struct{}), make(chan struct{})
	devDials := 0
	c.dial = func(_ context.Context, host transport.Host, _ AgentLookup) (remoteConnection, error) {
		conn := &fakeRemoteConn{host: host}
		if host.Name == "dev" {
			devDials++
			if devDials == 1 {
				conn.handler = func(*proto.Request) (*proto.Response, error) {
					return nil, errors.New("injected broken transport")
				}
				conn.closeFn = func() {
					close(closeEntered)
					<-releaseClose
				}
			}
		}
		return conn, nil
	}

	retryDone := make(chan error, 1)
	go func() {
		_, err := c.ReadFile(context.Background(), "dev", "status", 0, 1)
		retryDone <- err
	}()
	<-closeEntered

	// A second caller can publish a replacement while the retry path is blocked
	// closing the detached transport.
	if _, err := c.conn(t.Context(), "dev"); err != nil {
		t.Fatal(err)
	}
	if !c.IsConnected("dev") || c.ConnectionSecurity("dev").State != observe.SecurityReady {
		t.Fatalf("replacement is not ready: connected=%v status=%+v", c.IsConnected("dev"), c.ConnectionSecurity("dev"))
	}
	if got := c.Hosts.SecuritySnapshot().ConnectionSecurityTransitions[string(observe.SecurityCold)]; got != baseline[string(observe.SecurityCold)] {
		t.Fatalf("stale retry teardown published cold before Close returned: before=%d after=%d", baseline[string(observe.SecurityCold)], got)
	}

	otherDone := make(chan error, 1)
	go func() { _, err := c.conn(context.Background(), "other"); otherDone <- err }()
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry Close on dev blocked unrelated host")
	}

	close(releaseClose)
	if err := <-retryDone; err != nil {
		t.Fatal(err)
	}
	if !c.IsConnected("dev") || c.ConnectionSecurity("dev").State != observe.SecurityReady {
		t.Fatalf("stale retry Close overwrote replacement: connected=%v status=%+v", c.IsConnected("dev"), c.ConnectionSecurity("dev"))
	}
	metrics := c.Hosts.SecuritySnapshot().ConnectionSecurityTransitions
	if metrics[string(observe.SecurityCold)] != baseline[string(observe.SecurityCold)] ||
		metrics[string(observe.SecurityInitializing)]-baseline[string(observe.SecurityInitializing)] != 3 ||
		metrics[string(observe.SecurityReady)]-baseline[string(observe.SecurityReady)] != 3 ||
		metrics[string(observe.SecurityFailed)] != baseline[string(observe.SecurityFailed)] {
		t.Fatalf("connection security transition metrics are inconsistent: %v", metrics)
	}
}

func TestUntrustedBase64FlagCannotBypassResponseRedaction(t *testing.T) {
	c := newTestClient()
	const token = "coincidental-base64-text"
	if err := c.Secrets.Set(secrets.OutputKey("tok"), token); err != nil {
		t.Fatal(err)
	}
	resp := &proto.Response{Read: &proto.ReadResult{Content: token, ContentB64: true, EOF: true}}
	got := c.redactResponse(resp)
	if got.Read.Content != "<redacted:tok>" || got.Read.ContentB64 {
		t.Fatalf("content_b64 bypassed redaction: %+v", got.Read)
	}
}

func TestBinaryResponseRedactsBeforeBase64Projection(t *testing.T) {
	c := newTestClient()
	const token = "binary-response-secret"
	if err := c.Secrets.Set(secrets.OutputKey("tok"), token); err != nil {
		t.Fatal(err)
	}
	raw := append([]byte{0xff, 0x00}, []byte(token)...)
	encoded := base64.StdEncoding.EncodeToString(raw)
	got := c.redactResponse(&proto.Response{
		Read: &proto.ReadResult{Content: encoded, ContentB64: true},
		Exec: &proto.ExecResult{
			Stdout: encoded, StdoutB64: true,
			Stderr: encoded, StderrB64: true,
		},
	})
	for name, payload := range map[string]string{
		"read": got.Read.Content, "stdout": got.Exec.Stdout, "stderr": got.Exec.Stderr,
	} {
		decoded, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			t.Fatalf("%s lost binary projection: %v", name, err)
		}
		if bytes.Contains(decoded, []byte(token)) || !bytes.Contains(decoded, []byte("<redacted:tok>")) {
			t.Fatalf("%s binary redaction = %q", name, decoded)
		}
	}
	if !got.Read.ContentB64 || !got.Exec.StdoutB64 || !got.Exec.StderrB64 {
		t.Fatalf("binary flags were not preserved: read=%v stdout=%v stderr=%v", got.Read.ContentB64, got.Exec.StdoutB64, got.Exec.StderrB64)
	}
}

func TestFixedRequestRetryRefusesRedefinedAlias(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@a"})
	requestsOnB := 0
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		if h.Addr == "u@a" {
			return &fakeRemoteConn{
				host: h,
				handler: func(*proto.Request) (*proto.Response, error) {
					return nil, errors.New("broken transport")
				},
				closeFn: func() {
					if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@b"}); err != nil {
						t.Errorf("redefine host: %v", err)
					}
				},
			}, nil
		}
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op == proto.OpWriteFile {
				requestsOnB++
			}
			return &proto.Response{OK: true, Cat: &proto.WriteResult{}}, nil
		}}, nil
	}
	_, err := c.WriteFile(t.Context(), WriteFileOptions{Host: "dev", Path: "/tmp/x", Content: "host-a-only-data"})
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeAmbiguousOutcome || envelope.ExecutionState != proto.StatePossiblyExecuted {
		t.Fatalf("cross-identity mutation error = %#v, want ambiguous_outcome", err)
	}
	if requestsOnB != 0 {
		t.Fatalf("host A request was replayed to host B %d times", requestsOnB)
	}
}

func TestResponseLossRetrySemanticsByOperationClass(t *testing.T) {
	t.Run("read-only reconnects with stable operation identity", func(t *testing.T) {
		c := newTestClient()
		if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
			t.Fatal(err)
		}
		type seenRequest struct {
			operationID string
			clientID    string
			replay      bool
		}
		var seen []seenRequest
		dials := 0
		c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
			dials++
			thisDial := dials
			return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
				seen = append(seen, seenRequest{req.OperationID, req.ClientID, req.Replay})
				if thisDial == 1 {
					// Model a completed read whose final response was lost with the
					// transport. Repeating a read is explicitly safe.
					return nil, errors.New("injected response loss")
				}
				return &proto.Response{OK: true, Read: &proto.ReadResult{Content: "ok", EOF: true}}, nil
			}}, nil
		}

		res, err := c.ReadFile(t.Context(), "dev", "synthetic", 0, 16)
		if err != nil {
			t.Fatal(err)
		}
		if res.Content != "ok" || dials != 2 || len(seen) != 2 {
			t.Fatalf("result=%+v dials=%d seen=%+v", res, dials, seen)
		}
		if seen[0].operationID == "" || seen[0].operationID != seen[1].operationID {
			t.Fatalf("operation IDs changed across retry: %+v", seen)
		}
		if seen[0].clientID == "" || seen[0].clientID != seen[1].clientID || seen[0].clientID != c.callerID {
			t.Fatalf("caller IDs changed across retry: %+v caller=%q", seen, c.callerID)
		}
		if seen[0].replay || !seen[1].replay {
			t.Fatalf("replay markers = %+v, want false then true", seen)
		}
	})

	t.Run("idempotent reconnects with stable operation identity", func(t *testing.T) {
		c := newTestClient()
		if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
			t.Fatal(err)
		}
		var operationIDs, clientIDs []string
		var replays []bool
		dials := 0
		c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
			dials++
			thisDial := dials
			return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
				operationIDs = append(operationIDs, req.OperationID)
				clientIDs = append(clientIDs, req.ClientID)
				replays = append(replays, req.Replay)
				if thisDial == 1 {
					return nil, errors.New("injected response loss")
				}
				return &proto.Response{OK: true}, nil
			}}, nil
		}

		_, err := c.do(t.Context(), "dev", &proto.Request{
			Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: "op_synthetic_target"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if dials != 2 || len(operationIDs) != 2 || operationIDs[0] == "" || operationIDs[0] != operationIDs[1] {
			t.Fatalf("dials=%d operation IDs=%v", dials, operationIDs)
		}
		if clientIDs[0] == "" || clientIDs[0] != clientIDs[1] || replays[0] || !replays[1] {
			t.Fatalf("client IDs=%v replay=%v", clientIDs, replays)
		}
	})

	t.Run("mutation is not replayed without dedupe proof", func(t *testing.T) {
		c := newTestClient()
		if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
			t.Fatal(err)
		}
		executions := 0
		var operationID, clientID string
		c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
			return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
				executions++
				operationID, clientID = req.OperationID, req.ClientID
				// The mutation completed, but its final response was lost.
				return nil, errors.New("injected response loss")
			}}, nil
		}

		_, err := c.WriteFile(t.Context(), WriteFileOptions{Host: "dev", Path: "synthetic", Content: "value"})
		var envelope *proto.ErrorEnvelope
		if !errors.As(err, &envelope) {
			t.Fatalf("error = %#v, want structured envelope", err)
		}
		if envelope.Code != proto.CodeAmbiguousOutcome || envelope.ExecutionState != proto.StatePossiblyExecuted || envelope.OperationID != operationID {
			t.Fatalf("envelope = %+v operationID=%q", envelope, operationID)
		}
		if executions != 1 || operationID == "" || clientID != c.callerID {
			t.Fatalf("executions=%d operationID=%q clientID=%q caller=%q", executions, operationID, clientID, c.callerID)
		}
	})
}

func TestUnknownOperationFailsClosedBeforeDial(t *testing.T) {
	c := newTestClient()
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
		t.Fatal(err)
	}
	dials := 0
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		dials++
		return &fakeRemoteConn{host: h}, nil
	}

	_, err := c.do(t.Context(), "dev", &proto.Request{Op: "synthetic_unknown"})
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeUnknownOperation || envelope.ExecutionState != proto.StateNotSent {
		t.Fatalf("error = %#v, want unknown-operation envelope", err)
	}
	if dials != 0 {
		t.Fatalf("unknown operation dialed %d times", dials)
	}
}

func TestForceAgentUploadReconnectsWithoutPurgingScopedSecret(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	if err := c.SetSecret("dev", "tok", "scoped-secret-value"); err != nil {
		t.Fatal(err)
	}
	dials := 0
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		dials++
		return &fakeRemoteConn{host: h}, nil
	}
	if _, err := c.conn(t.Context(), "dev"); err != nil {
		t.Fatal(err)
	}
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h", ForceAgentUpload: true}); err != nil {
		t.Fatal(err)
	}
	if c.IsConnected("dev") || c.ConnectionSecurity("dev").State != observe.SecurityCold {
		t.Fatalf("force-upload change left stale connection/status: connected=%v status=%+v", c.IsConnected("dev"), c.ConnectionSecurity("dev"))
	}
	if n, ok, err := c.SecretLength("dev", "tok"); err != nil || !ok || n != len("scoped-secret-value") {
		t.Fatalf("force-upload change purged scoped secret: n=%d ok=%v err=%v", n, ok, err)
	}
	if _, err := c.conn(t.Context(), "dev"); err != nil {
		t.Fatal(err)
	}
	if dials != 2 {
		t.Fatalf("force-upload change dial count = %d, want 2", dials)
	}
}

func TestDeclarativeSecretRefreshesOnReconnect(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(st *session.State) {
		st.Secrets = map[string]string{"tok": "~/token"}
	})
	dials := 0
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		dials++
		value := fmt.Sprintf("declared-value-%d", dials)
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			switch req.Op {
			case proto.OpReadFile:
				return &proto.Response{OK: true, Read: &proto.ReadResult{Content: value, Size: int64(len(value)), EOF: true}}, nil
			case proto.OpExec:
				return &proto.Response{OK: true, Exec: &proto.ExecResult{}}, nil
			default:
				return &proto.Response{OK: true}, nil
			}
		}}, nil
	}
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	c.Disconnect("dev")
	if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	value, ok := c.Secrets.Get(testHostSecretKey(c, "dev", "tok"))
	if !ok || value != "declared-value-2" {
		t.Fatalf("declarative value was not refreshed: %q", value)
	}
}

func TestOutputOnlyDeleteCannotDefeatInFlightRedactionSnapshot(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	const token = "output-only-inflight-token"
	if err := c.SetSecret("", "tok", token); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op == proto.OpExec {
				close(entered)
				<-release
				return &proto.Response{OK: true, Exec: &proto.ExecResult{Stdout: token}}, nil
			}
			return &proto.Response{OK: true}, nil
		}}, nil
	}
	type execResult struct {
		res *ExecResult
		err error
	}
	execDone := make(chan execResult, 1)
	go func() {
		res, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}})
		execDone <- execResult{res: res, err: err}
	}()
	<-entered
	deleteDone := make(chan struct{})
	go func() {
		_, _ = c.DeleteSecret("", "tok")
		close(deleteDone)
	}()
	select {
	case <-deleteDone:
	case <-time.After(time.Second):
		t.Fatal("output-only delete was globally blocked by an unrelated host operation")
	}
	close(release)
	got := <-execDone
	if got.err != nil || got.res == nil || strings.Contains(got.res.Stdout, token) {
		t.Fatalf("in-flight response was not redacted: res=%+v err=%v", got.res, got.err)
	}
}

func TestOutputOnlyDeleteDuringColdDialCannotDefeatErrorSnapshot(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	const token = "output-only-cold-dial-token"
	if err := c.SetSecret("", "tok", token); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		close(entered)
		<-release
		return nil, errors.New("dial failed with " + token)
	}
	errDone := make(chan error, 1)
	go func() {
		_, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}})
		errDone <- err
	}()
	<-entered
	if changed, err := c.DeleteSecret("", "tok"); err != nil || !changed {
		t.Fatalf("delete output secret: changed=%v err=%v", changed, err)
	}
	close(release)
	err := <-errDone
	if err == nil || strings.Contains(err.Error(), token) || !strings.Contains(err.Error(), "redacted:tok") {
		t.Fatalf("cold dial error escaped snapshot redaction: %v", err)
	}
}

func TestDeclaredSecretDialFailureDoesNotExposeUnknownValue(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	const token = "declared\"secret\\line\n雪界"
	c.Hosts.Update("dev", func(st *session.State) {
		st.Secrets = map[string]string{"tok": "~/token"}
	})
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return nil, fmt.Errorf("bootstrap stderr=%q", token)
	}
	_, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}})
	if err == nil || strings.Contains(err.Error(), "bootstrap stderr") || strings.Contains(err.Error(), "雪界") || strings.Contains(err.Error(), `secret\\line`) {
		t.Fatalf("unsafe dial failure = %v", err)
	}
	status := c.ConnectionSecurity("dev")
	if status.State != observe.SecurityFailed || status.Reason != observe.ReasonSecretReadFailed {
		t.Fatalf("dial failure security state = %+v", status)
	}
}

func TestInvalidSecretDeclarationFailsBeforeDial(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(st *session.State) {
		st.Secrets = map[string]string{"tok": ""}
	})
	dials := 0
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		dials++
		return nil, errors.New("must not dial")
	}
	_, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}})
	if err == nil || !strings.Contains(err.Error(), "invalid secret declaration") {
		t.Fatalf("invalid declaration error = %v", err)
	}
	if dials != 0 {
		t.Fatalf("invalid declaration performed %d remote dials", dials)
	}
}

func TestRemoteSecretReadErrorDoesNotExposeProspectiveValue(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	const token = "prospective\"secret\\line\n雪界"
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op == proto.OpReadFile {
				return nil, fmt.Errorf("remote stderr=%q", token)
			}
			return &proto.Response{OK: true}, nil
		}}, nil
	}
	err := c.SetSecretFromRemoteFile(t.Context(), "dev", "tok", "~/token")
	if err == nil || strings.Contains(err.Error(), "prospective") || strings.Contains(err.Error(), "雪界") {
		t.Fatalf("unsafe remote secret read error = %v", err)
	}
}

func TestRemoteSecretReadUsesNegotiatedV3Identity(t *testing.T) {
	c := newTestClient()
	host := transport.Host{Name: "dev", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	seen := 0
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op != proto.OpReadFile || proto.ValidateOperationID(req.OperationID) != nil ||
				proto.ValidateOperationID(req.ClientID) != nil {
				t.Fatalf("raw v3 secret request = %+v", req)
			}
			seen++
			return &proto.Response{
				OperationID: req.OperationID, Type: proto.EventFinal, Seq: 1,
				Terminal: true, Execution: proto.StateCompleted, OK: true,
				Read: &proto.ReadResult{Content: "registered-secret-value", Size: 23, EOF: true},
			}, nil
		}}, nil
	}
	if err := c.SetSecretFromRemoteFile(t.Context(), "dev", "tok", "~/token"); err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("raw secret request count = %d", seen)
	}
}

func TestResponseIsRedactedBeforeIdentityUpdatePurgesOldSecret(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@old"})
	const token = "old-host-response-secret"
	if err := c.SetSecret("dev", "tok", token); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op == proto.OpExec {
				close(entered)
				<-release
				return &proto.Response{OK: true, Exec: &proto.ExecResult{Stdout: token, Stderr: "err " + token}}, nil
			}
			return &proto.Response{OK: true}, nil
		}}, nil
	}
	result := make(chan *ExecResult, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := c.Exec(context.Background(), ExecOptions{Host: "dev", Argv: []string{"true"}})
		result <- res
		errCh <- err
	}()
	<-entered
	updated := make(chan error, 1)
	go func() { updated <- c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@new"}) }()
	select {
	case err := <-updated:
		t.Fatalf("identity changed before response redaction: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	res := <-result
	if strings.Contains(res.Stdout+res.Stderr, token) {
		t.Fatalf("old response escaped redaction: %+v", res)
	}
	if err := <-updated; err != nil {
		t.Fatal(err)
	}
	for _, descriptor := range c.Secrets.Descriptors() {
		if descriptor.Host.Alias == "dev" {
			t.Fatalf("old host secret survived identity update: %+v", descriptor)
		}
	}
}

func TestSecretRotationWaitsForInflightResponseRedaction(t *testing.T) {
	c := newTestClient()
	_ = c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	const oldValue = "old-rotation-secret"
	if err := c.SetSecret("dev", "tok", oldValue); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op == proto.OpExec {
				once.Do(func() { close(entered) })
				<-release
				return &proto.Response{OK: true, Exec: &proto.ExecResult{Stdout: oldValue}}, nil
			}
			return &proto.Response{OK: true}, nil
		}}, nil
	}
	result := make(chan *ExecResult, 1)
	go func() {
		res, _ := c.Exec(context.Background(), ExecOptions{Host: "dev", Argv: []string{"true"}})
		result <- res
	}()
	<-entered
	rotated := make(chan error, 1)
	go func() { rotated <- c.SetSecret("dev", "tok", "new-rotation-secret") }()
	select {
	case err := <-rotated:
		t.Fatalf("rotation bypassed operation lease: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if res := <-result; strings.Contains(res.Stdout, oldValue) {
		t.Fatalf("old value escaped during rotation: %q", res.Stdout)
	}
	if err := <-rotated; err != nil {
		t.Fatal(err)
	}
}
