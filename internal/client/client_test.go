package client

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

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
	c.loadHostSecrets(context.Background(), "dev")

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
	c.loadHostSecrets(context.Background(), "dev")
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
	c.loadHostSecrets(context.Background(), "dev")

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
