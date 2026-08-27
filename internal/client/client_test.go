package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
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
}

func (f *fakeRemoteConn) Host() transport.Host { return f.host }
func (f *fakeRemoteConn) SSHArgs() []string    { return []string{"-p", fmt.Sprint(f.host.Port)} }
func (f *fakeRemoteConn) Close() error         { f.mu.Lock(); f.closed = true; f.mu.Unlock(); return nil }
func (f *fakeRemoteConn) Do(_ context.Context, req *proto.Request) (*proto.Response, error) {
	if f.entered != nil {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	f.mu.Lock()
	f.ops = append(f.ops, req.Op)
	f.mu.Unlock()
	r := &proto.Response{OK: true}
	switch req.Op {
	case proto.OpExec:
		r.Exec = &proto.ExecResult{}
	case proto.OpReadFile:
		r.Read = &proto.ReadResult{}
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
	params, err := c.buildExecParams("dev", []string{"pwd"}, "", map[string]string{"EXTRA": "2"}, nil)
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
	params, err := c.buildExecParams("dev", []string{"pwd"}, "/tmp", nil, &no)
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
	c.Secrets.Set("tok", "realvalue123")

	params, err := c.buildExecParams("dev", []string{"env"}, "", map[string]string{"T": "secret:tok"}, nil)
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

	_, err := c.buildExecParams("dev", []string{"env"}, "", map[string]string{"T": "secret:missing"}, nil)
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
	// The host is unresolvable here, so either failure is acceptable; what
	// matters is that it does not silently default to a direction.
	if !strings.Contains(err.Error(), "sideways") && !strings.Contains(err.Error(), "unknown host") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRedactErrScrubsSecrets(t *testing.T) {
	c := newTestClient()
	c.Secrets.Set("tok", "82d9b49359b262b40bdbbfa844891b5e")

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
	if err := c.Secrets.SetFromFile("tok", keyPath); err != nil {
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
	var warnings []string
	c := New(func(a, b string) (*transport.AgentBinary, error) { return nil, nil })
	c.warn = func(format string, args ...any) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(s *session.State) {
		s.Secrets = map[string]string{"tok": "/nonexistent/path"}
	})
	if err := c.Secrets.Set("tok", "explicitly-set-value"); err != nil {
		t.Fatal(err)
	}

	// Would try to read the bogus path if it did not skip already-registered names.
	c.loadHostSecrets(context.Background(), "dev", nil)

	if v, _ := c.Secrets.Get("tok"); v != "explicitly-set-value" {
		t.Errorf("value = %q, want the explicit registration preserved", v)
	}
	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none: the configured path must not be read at all", warnings)
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
	if err := c.Secrets.Set("tok", tok); err != nil {
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
	if err := c.Secrets.Set("tok", tok); err != nil {
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
	if err := c.Secrets.Set("tok", tok); err != nil {
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
	c.loadHostSecrets(context.Background(), "dev", nil)
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
	if err := c.Secrets.Set("tok", token); err != nil {
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

	var warnings []string
	c := newTestClient()
	c.warn = func(format string, args ...any) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.Update("dev", func(s *session.State) {
		s.Secrets = map[string]string{"tok": keyPath}
	})
	if err := c.Secrets.SetFromFile("tok", keyPath); err != nil {
		t.Fatal(err)
	}

	// Second pass, as a reconnect would trigger. Reaching the fetch would need a
	// live agent, so it would fail and warn -- silence is the proof it was skipped.
	c.loadHostSecrets(context.Background(), "dev", nil)

	if len(warnings) != 0 {
		t.Errorf("warnings = %v, want none: an already-registered name must not be refetched", warnings)
	}
	if names := c.Secrets.Names(); len(names) != 1 {
		t.Errorf("names = %v, want exactly one registration", names)
	}
	if v, _ := c.Secrets.Get("tok"); v != token {
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

func TestConcurrentLookupCannotPublishSupersededDial(t *testing.T) {
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
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@new"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
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
				c.rsync = func(context.Context, []string) (string, string, error) { close(entered); <-release; return "", "", nil }
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
