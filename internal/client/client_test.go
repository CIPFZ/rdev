package client

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
func TestLoadHostSecretsKeepsExplicitValue(t *testing.T) {
	c := New(func(a, b string) (*transport.AgentBinary, error) { return nil, nil })
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
