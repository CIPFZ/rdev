package mcpsrv

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func newTestClient() *client.Client {
	// The lookup is never exercised: these tests never dial a host, and any test
	// that tried would fail on ssh rather than on a missing agent build.
	return client.New(func(goos, goarch string) (*transport.AgentBinary, error) {
		return &transport.AgentBinary{Data: []byte("stub")}, nil
	})
}

// connect runs the server over an in-memory transport and returns a session.
//
// Driving the real MCP protocol rather than calling handlers directly is what
// makes these tests meaningful: a tool that is defined but never registered, or
// whose input schema rejects valid arguments, fails here the same way it would in
// Claude Code.
func connect(t *testing.T, c *client.Client) *mcp.ClientSession {
	t.Helper()
	return connectServer(t, New(c))
}

func connectServer(t *testing.T, srv *mcp.Server) *mcp.ClientSession {
	t.Helper()

	serverT, clientT := mcp.NewInMemoryTransports()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	ss, err := srv.Connect(ctx, serverT, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { ss.Close() })

	cli := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := cli.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

// callTool invokes a tool and decodes its structured result.
//
// Returns the tool's error text rather than a Go error when the handler failed,
// since MCP reports handler failures as a result with IsError set.
func callTool(t *testing.T, cs *mcp.ClientSession, name string, args any, out any) (isErr bool, errText string) {
	t.Helper()

	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	if err != nil {
		t.Fatalf("CallTool(%s) transport error: %v", name, err)
	}
	if res.IsError {
		text := ""
		for _, content := range res.Content {
			if tc, ok := content.(*mcp.TextContent); ok {
				text += tc.Text
			}
		}
		return true, text
	}
	if out != nil && res.StructuredContent != nil {
		b, err := json.Marshal(res.StructuredContent)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("decode %s result: %v", name, err)
		}
	}
	return false, ""
}

// Every tool must be registered. One that exists in the codebase but is not
// wired here is invisible to Claude Code -- the same "implemented but not routed"
// failure this project already hit with job_wait.
func TestAllToolsAreRegistered(t *testing.T) {
	cs := connect(t, newTestClient())

	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, tool := range res.Tools {
		got[tool.Name] = true
	}

	want := []string{
		"rdev_exec",
		"rdev_job_start", "rdev_job_list", "rdev_job_status",
		"rdev_job_logs", "rdev_job_stop", "rdev_job_wait", "rdev_job_rm",
		"rdev_read", "rdev_write", "rdev_list",
		"rdev_sync", "rdev_session", "rdev_secrets",
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("tool %q is not registered", name)
		}
	}
	if len(got) != len(want) {
		t.Errorf("registered %d tools, want %d: got %v", len(got), len(want), got)
	}
}

// A description is the only thing telling the model when to reach for one tool
// over another, so an empty one is a real defect.
func TestEveryToolHasADescription(t *testing.T) {
	cs := connect(t, newTestClient())
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		if tool.Description == "" {
			t.Errorf("tool %q has no description", tool.Name)
		}
	}
}

// argv must be an array in the schema. If it were a string, the model could pass
// a shell command and the no-shell guarantee would be gone at the API boundary.
func TestExecArgvIsAnArrayInSchema(t *testing.T) {
	cs := connect(t, newTestClient())
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		if tool.Name != "rdev_exec" {
			continue
		}
		b, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]struct {
				// JSON Schema allows either a single type or a list of them.
				Type json.RawMessage `json:"type"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(b, &schema); err != nil {
			t.Fatal(err)
		}
		got := string(schema.Properties["argv"].Type)
		if !strings.Contains(got, "array") {
			t.Errorf("argv type = %s, want array: a string entry would reintroduce shell parsing", got)
		}
		return
	}
	t.Fatal("rdev_exec not found")
}

// ---------- session scope resolution ----------

// A brand-new host defaults to project scope: a machine registered while working
// in a repo almost always belongs to that repo, and the safer mistake is a host
// that is too narrowly visible rather than one leaking into unrelated projects.
func TestSessionNewHostDefaultsToProjectScope(t *testing.T) {
	cs := connect(t, newTestClient())

	var out SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{Host: "dev", Addr: "u@h"}, &out); isErr {
		t.Fatal(msg)
	}
	if len(out.Hosts) != 1 {
		t.Fatalf("got %d hosts, want 1", len(out.Hosts))
	}
	if out.Hosts[0].Scope != string(session.ScopeProject) {
		t.Errorf("Scope = %q, want project for a new host", out.Hosts[0].Scope)
	}
}

// An existing host keeps its scope when the caller does not name one, so setting
// cwd on a global host does not silently demote it to project-only.
func TestSessionExistingHostKeepsScope(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "prod", Addr: "u@h"})
	c.Hosts.SetScope("prod", session.ScopeGlobal)
	cs := connect(t, c)

	var out SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{Host: "prod", Cwd: "~/app"}, &out); isErr {
		t.Fatal(msg)
	}
	if out.Hosts[0].Scope != string(session.ScopeGlobal) {
		t.Errorf("Scope = %q, want the existing global scope to survive", out.Hosts[0].Scope)
	}
}

func TestSessionExplicitScopeWins(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"})
	c.Hosts.SetScope("dev", session.ScopeProject)
	cs := connect(t, c)

	var out SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{Host: "dev", Scope: "global"}, &out); isErr {
		t.Fatal(msg)
	}
	if out.Hosts[0].Scope != string(session.ScopeGlobal) {
		t.Errorf("Scope = %q, want global", out.Hosts[0].Scope)
	}
}

func TestSessionRejectsUnknownScope(t *testing.T) {
	cs := connect(t, newTestClient())
	isErr, _ := callTool(t, cs, "rdev_session", SessionIn{Host: "dev", Addr: "u@h", Scope: "bogus"}, nil)
	if !isErr {
		t.Error("an unknown scope should be rejected rather than silently defaulted")
	}
}

func TestSessionRejectsUnsafeSSHAndRemoteDirInputs(t *testing.T) {
	cs := connect(t, newTestClient())
	for _, in := range []SessionIn{
		{Host: "dev", Addr: "-oProxyCommand=touch-pwned"},
		{Host: "dev", Addr: "u@h other"},
		{Host: "dev", Addr: "u@h", Port: 65536},
		{Host: "dev", Addr: "u@h", RemoteDir: "~/.cache/$(touch-pwned)"},
		{Host: "dev", Addr: "u@h", RemoteDir: "../outside"},
	} {
		if isErr, _ := callTool(t, cs, "rdev_session", in, nil); !isErr {
			t.Errorf("unsafe session input was accepted: %+v", in)
		}
	}
}

func TestSessionProjectApprovalIsDigestBound(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)
	if err := os.MkdirAll(filepath.Join(home, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(project, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"hosts":[{"name":"dev","addr":"u@project"}]}`)
	if err := os.WriteFile(filepath.Join(project, ".rdev", "hosts.json"), config, 0o600); err != nil {
		t.Fatal(err)
	}
	c := newTestClient()
	var untrusted *session.UntrustedProjectError
	if err := c.Hosts.Load(); !errors.As(err, &untrusted) {
		t.Fatalf("Load error = %v, want untrusted project", err)
	}
	cs := connect(t, c)
	var pending SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{}, &pending); isErr {
		t.Fatal(msg)
	}
	if pending.ProjectTrust.Approved || pending.ProjectTrust.Digest == "" {
		t.Fatalf("project trust = %+v", pending.ProjectTrust)
	}
	if pending.Security.SchemaVersion != 1 || pending.Security.SecurityRejects["project_untrusted"] != 1 {
		t.Fatalf("security snapshot = %+v", pending.Security)
	}
	if isErr, _ := callTool(t, cs, "rdev_session", SessionIn{ApproveProjectDigest: strings.Repeat("0", 64)}, nil); !isErr {
		t.Fatal("wrong project digest was accepted")
	}
	var approved SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{ApproveProjectDigest: pending.ProjectTrust.Digest}, &approved); isErr {
		t.Fatal(msg)
	}
	if !approved.ProjectTrust.Approved || len(approved.Hosts) != 1 || approved.Hosts[0].Addr != "u@project" {
		t.Fatalf("approved output = %+v", approved)
	}
}

func TestSessionCommittedApprovalWarningIsSuccessful(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)
	if err := os.MkdirAll(filepath.Join(home, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(project, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"hosts":[{"name":"dev","addr":"u@project"}]}`)
	if err := os.WriteFile(filepath.Join(project, ".rdev", "hosts.json"), config, 0o600); err != nil {
		t.Fatal(err)
	}
	c := newTestClient()
	var untrusted *session.UntrustedProjectError
	if err := c.Hosts.Load(); !errors.As(err, &untrusted) {
		t.Fatalf("Load error = %v, want untrusted project", err)
	}
	approve := func(digest string) (session.ProjectTrust, error) {
		trust, err := c.Hosts.ApproveProject(digest)
		if err != nil {
			return trust, err
		}
		return trust, &session.ConfigWriteCommittedError{Cause: errors.New("injected backup cleanup failure")}
	}
	cs := connectServer(t, newServer(c, approve))
	var out SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{ApproveProjectDigest: untrusted.Trust.Digest}, &out); isErr {
		t.Fatalf("committed approval projected as MCP failure: %s", msg)
	}
	if !out.ProjectTrust.Approved || out.Warning == "" {
		t.Fatalf("committed approval output=%+v", out)
	}
	if h, err := c.Hosts.Host("dev"); err != nil || h.Addr != "u@project" {
		t.Fatalf("committed live host=%+v err=%v", h, err)
	}
}

// Sticky state must read back: a caller sets cwd once and relies on later calls
// inheriting it.
func TestSessionStoresStickyState(t *testing.T) {
	cs := connect(t, newTestClient())
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{Host: "dev", Addr: "u@h"}, nil); isErr {
		t.Fatal(msg)
	}

	no := false
	var out SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{
		Host:       "dev",
		Cwd:        "~/proj",
		Env:        map[string]string{"PROXY": "http://p:1"},
		LoginShell: &no,
	}, &out); isErr {
		t.Fatal(msg)
	}

	h := out.Hosts[0]
	if h.Cwd != "~/proj" {
		t.Errorf("Cwd = %q, want ~/proj", h.Cwd)
	}
	if h.Env["PROXY"] != "http://p:1" {
		t.Errorf("Env = %v, want PROXY set", h.Env)
	}
	if h.LoginShell {
		t.Error("LoginShell should reflect the explicit false")
	}
}

// Setting state on an unregistered host must error rather than create a
// half-defined host with no address.
func TestSessionUpdateUnknownHostErrors(t *testing.T) {
	cs := connect(t, newTestClient())
	isErr, _ := callTool(t, cs, "rdev_session", SessionIn{Host: "ghost", Cwd: "~/x"}, nil)
	if !isErr {
		t.Error("setting state on an unknown host should error")
	}
}

// RemoteDir decides where the agent binary and job records live, so it must be
// reachable from a tool call rather than only by hand-editing hosts.json.
func TestSessionSetsRemoteDirWithoutLosingAddr(t *testing.T) {
	cs := connect(t, newTestClient())
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{Host: "dev", Addr: "u@h", Port: 36000}, nil); isErr {
		t.Fatal(msg)
	}

	var out SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{Host: "dev", RemoteDir: "~/.cache/custom"}, &out); isErr {
		t.Fatal(msg)
	}
	h := out.Hosts[0]
	if h.RemoteDir != "~/.cache/custom" {
		t.Errorf("RemoteDir = %q, want ~/.cache/custom", h.RemoteDir)
	}
	// A RemoteDir-only update must not drop the destination.
	if h.Addr != "u@h" || h.Port != 36000 {
		t.Errorf("addr = %q:%d, want the existing destination preserved", h.Addr, h.Port)
	}
}

func TestSessionListsAllHostsWhenHostOmitted(t *testing.T) {
	c := newTestClient()
	c.Hosts.Add(transport.Host{Name: "a", Addr: "u@a"})
	c.Hosts.Add(transport.Host{Name: "b", Addr: "u@b"})
	cs := connect(t, c)

	var out SessionOut
	if isErr, msg := callTool(t, cs, "rdev_session", SessionIn{}, &out); isErr {
		t.Fatal(msg)
	}
	if len(out.Hosts) != 2 {
		t.Errorf("got %d hosts, want both", len(out.Hosts))
	}
}

// ---------- secrets ----------

func TestSecretsRejectsUnknownAction(t *testing.T) {
	cs := connect(t, newTestClient())
	if isErr, _ := callTool(t, cs, "rdev_secrets", SecretsIn{Action: "explode"}, nil); !isErr {
		t.Error("an unknown action should be rejected")
	}
}

// The result reports names only. Echoing the plaintext back would defeat the
// point of registering it.
func TestSecretsSetDoesNotEchoValue(t *testing.T) {
	cs := connect(t, newTestClient())

	const plaintext = "super-secret-value"
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "rdev_secrets",
		Arguments: SecretsIn{Action: "set", Name: "tok", Value: plaintext},
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), plaintext) {
		t.Error("the secret value appears in the tool result")
	}

	var out SecretsOut
	if isErr, msg := callTool(t, cs, "rdev_secrets", SecretsIn{Action: "list"}, &out); isErr {
		t.Fatal(msg)
	}
	if len(out.Names) != 1 || out.Names[0] != "tok" {
		t.Errorf("Names = %v, want [tok]", out.Names)
	}
}

func TestSecretsDeleteReportsChange(t *testing.T) {
	cs := connect(t, newTestClient())
	if isErr, msg := callTool(t, cs, "rdev_secrets", SecretsIn{Action: "set", Name: "tok", Value: "abcdefgh"}, nil); isErr {
		t.Fatal(msg)
	}

	var first SecretsOut
	if isErr, msg := callTool(t, cs, "rdev_secrets", SecretsIn{Action: "delete", Name: "tok"}, &first); isErr {
		t.Fatal(msg)
	}
	if !first.Changed {
		t.Error("Changed should be true when a secret was removed")
	}

	// Deleting something absent is not an error, but reports no change. Decoded
	// into a fresh value: Changed is omitempty, so a false result is absent from
	// the JSON and would leave a reused struct's true in place.
	var second SecretsOut
	if isErr, msg := callTool(t, cs, "rdev_secrets", SecretsIn{Action: "delete", Name: "tok"}, &second); isErr {
		t.Fatal(msg)
	}
	if second.Changed {
		t.Error("Changed should be false when nothing was removed")
	}
}

// ---------- input validation ----------

// argv is required, and an empty one must be refused before anything is dialed.
func TestExecRejectsEmptyArgv(t *testing.T) {
	cs := connect(t, newTestClient())
	if isErr, _ := callTool(t, cs, "rdev_exec", ExecIn{Host: "dev", Argv: nil}, nil); !isErr {
		t.Error("an empty argv should be rejected")
	}
}

func TestJobRmRequiresAFilter(t *testing.T) {
	cs := connect(t, newTestClient())
	// No id, no filters: refuse rather than sweep everything.
	if isErr, _ := callTool(t, cs, "rdev_job_rm", JobRmIn{Host: "dev"}, nil); !isErr {
		t.Error("job_rm with no id and no filters should be rejected")
	}
}

func TestSyncRejectsBadDirection(t *testing.T) {
	cs := connect(t, newTestClient())
	isErr, _ := callTool(t, cs, "rdev_sync", SyncIn{
		Host: "dev", Direction: "sideways", Local: "/tmp/a", Remote: "/tmp/b",
	}, nil)
	if !isErr {
		t.Error("an invalid sync direction should be rejected")
	}
}

func TestToJobOutHandlesNil(t *testing.T) {
	if got := toJobOut(nil); got.ID != "" {
		t.Errorf("toJobOut(nil) = %+v, want a zero value rather than a panic", got)
	}
}

func TestVersionIsSet(t *testing.T) {
	if Version == "" {
		t.Error("Version must be reported to MCP clients")
	}
}

// The agent computes orphaned/child_pid for a job whose supervisor died while the
// work kept running. Dropping them here would make an orphaned job look healthy
// over MCP even though its exit code is permanently lost -- and the CLI, which
// prints proto.JobInfo directly, would disagree with this front end.
func TestToJobOutCarriesOrphanState(t *testing.T) {
	out := toJobOut(&proto.JobInfo{
		ID: "j1", State: proto.JobRunning, Orphaned: true, ChildPID: 4242,
	})
	if !out.Orphaned {
		t.Error("Orphaned was dropped: an orphaned job would look healthy over MCP")
	}
	if out.ChildPID != 4242 {
		t.Errorf("ChildPID = %d, want 4242", out.ChildPID)
	}
}

// Waiting on several ids must reach the wait path rather than being rejected as a
// missing id, and each job's outcome comes back separately.
func TestJobWaitAcceptsIDsWithoutSingleID(t *testing.T) {
	cs := connect(t, newTestClient())

	// No host is reachable here, so this fails at dial -- but a validation error
	// would mean ids never got past the argument check.
	isErr, msg := callTool(t, cs, "rdev_job_wait", JobWaitIn{
		Host: "user@unreachable.invalid", IDs: []string{"a", "b"},
	}, nil)
	if !isErr {
		t.Skip("unexpectedly reached a host")
	}
	if strings.Contains(msg, "job id required") {
		t.Errorf("ids was not accepted: %q", msg)
	}
}

func TestJobWaitStillRequiresAnID(t *testing.T) {
	cs := connect(t, newTestClient())
	isErr, msg := callTool(t, cs, "rdev_job_wait", JobWaitIn{Host: "dev"}, nil)
	if !isErr {
		t.Fatal("a wait with neither id nor ids should be rejected")
	}
	if !strings.Contains(msg, "job id required") {
		t.Errorf("err = %q, want it to say an id is required", msg)
	}
}

// The multi-job shape has to survive the schema: ids must be an array and the
// result must carry a per-job list.
func TestJobWaitSchemaExposesIDsArray(t *testing.T) {
	cs := connect(t, newTestClient())
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range res.Tools {
		if tool.Name != "rdev_job_wait" {
			continue
		}
		b, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]struct {
				Type json.RawMessage `json:"type"`
			} `json:"properties"`
		}
		if err := json.Unmarshal(b, &schema); err != nil {
			t.Fatal(err)
		}
		if _, ok := schema.Properties["ids"]; !ok {
			t.Fatal("ids is missing from the rdev_job_wait schema")
		}
		if got := string(schema.Properties["ids"].Type); !strings.Contains(got, "array") {
			t.Errorf("ids type = %s, want array", got)
		}
		if _, ok := schema.Properties["wait_any"]; !ok {
			t.Error("wait_any is missing from the rdev_job_wait schema")
		}
		return
	}
	t.Fatal("rdev_job_wait not found")
}

// The middleware backstop must scrub a secret that no handler remembered to
// redact. This is the whole point of it: per-field scrubbing already failed once
// (SyncResult.Command), and the next omission should be caught by the boundary
// rather than by someone noticing a credential in a transcript.
//
// Driven through a real MCP round trip -- the value has to survive handler,
// serialization, middleware, and transport to prove the interception point is real
// and not just a function that happens to be called.
func TestMiddlewareRedactsUnscrubbedResultField(t *testing.T) {
	c := newTestClient()
	const token = "unscrubbed-credential-value-1234"
	if err := c.Secrets.Set("tok", token); err != nil {
		t.Fatal(err)
	}
	cs := connect(t, c)

	// Cwd is echoed straight back by the session handler with no redaction of its
	// own, which makes it a faithful stand-in for a field someone forgot.
	if isErr, msg := callTool(t, cs, "rdev_session",
		SessionIn{Host: "dev", Addr: "u@h", Cwd: "/data/" + token}, nil); isErr {
		t.Fatal(msg)
	}

	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "rdev_session",
		Arguments: SessionIn{Host: "dev"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check the raw wire payload, not a decoded struct: a client may read either
	// structuredContent or the text fallback, so neither may carry the value.
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), token) {
		t.Errorf("secret reached structuredContent: %s", raw)
	}
	// Without the angle brackets: encoding/json escapes < and > to </>, so
	// matching the literal "<redacted:tok>" fails against a correctly scrubbed
	// payload. The name is the part that has to be there.
	if !strings.Contains(string(raw), "redacted:tok") {
		t.Errorf("want the placeholder in structuredContent, got: %s", raw)
	}
	for _, content := range res.Content {
		tc, ok := content.(*mcp.TextContent)
		if !ok {
			continue
		}
		if strings.Contains(tc.Text, token) {
			t.Errorf("secret reached the text fallback: %s", tc.Text)
		}
	}
}

// Non-tool methods must pass through untouched. Scanning list responses would cost
// bytes on every schema fetch and they carry no remote output.
func TestMiddlewareLeavesToolListIntact(t *testing.T) {
	c := newTestClient()
	if err := c.Secrets.Set("tok", "irrelevant-value-here-0987"); err != nil {
		t.Fatal(err)
	}
	cs := connect(t, c)

	tools, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The exact count is asserted by TestAllToolsAreRegistered; here it only has to
	// come back non-empty and undamaged by the middleware.
	if len(tools.Tools) == 0 {
		t.Error("ListTools returned nothing")
	}
	for _, tool := range tools.Tools {
		if tool.Name == "" || tool.InputSchema == nil {
			t.Errorf("tool %+v lost its name or schema passing through middleware", tool)
		}
	}
}
