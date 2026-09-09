package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/support"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestRemoteSupportAndStateFrontends(t *testing.T) {
	d, namespace, _ := newRemoteRuntime(t)
	owner := broker.Owner{ClientID: "phase6-support", ProjectID: "allowed"}
	denied := broker.Owner{ClientID: owner.ClientID, ProjectID: "denied"}
	p := broker.NewPolicy()
	for _, op := range []string{proto.OpCapabilityProbe, proto.OpStateInspect, proto.OpStateMigrate} {
		if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	cli := os.Getenv("RDEV_TEST_CLI_BINARY")
	if cli == "" {
		cli = filepath.Join(d.dir, "rdev")
		cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
		cmd.Dir = repoRoot(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build CLI: %v %s", err, out)
		}
	}
	trapDir := filepath.Join(d.dir, "trap")
	if err := os.Mkdir(trapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(d.dir, "fallback")
	if err := os.WriteFile(filepath.Join(trapDir, "ssh"), []byte("#!/bin/sh\n: > \"$RDEV_SUPPORT_FALLBACK\"\nexit 99\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	tokens := map[broker.Owner]string{owner: d.token(owner, "5m"), denied: d.token(denied, "5m")}
	command := func(who broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Dir = d.dir
		cmd.Env = append(os.Environ(), "PATH="+trapDir+string(os.PathListSeparator)+os.Getenv("PATH"), "RDEV_SUPPORT_FALLBACK="+marker, "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+who.ClientID, "RDEV_PROJECT_ID="+who.ProjectID, "RDEV_PRINCIPAL_TOKEN="+tokens[who], "RDEV_APPROVAL_TOKEN=")
		return cmd
	}
	// No grant is needed to discover one's own denials, and neither registered
	// nor unknown host discovery may perform remote transport admission.
	for _, host := range []string{"runtime-host", "not-in-registry"} {
		data, err := command(denied, "support", host).Output()
		var out support.Discovery
		if err != nil || json.Unmarshal(data, &out) != nil || out.RuntimeStatus != "permission_denied" || out.Runtime != nil {
			t.Fatalf("denied support failed: %v %s", err, data)
		}
	}
	if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 {
		t.Fatal("denied support connected to SSH")
	}
	for _, who := range []broker.Owner{owner, denied} {
		data, err := command(who, "support", "runtime-host", "--refresh").Output()
		var out support.Discovery
		if err != nil || json.Unmarshal(data, &out) != nil {
			t.Fatalf("CLI support failed: %v %s", err, data)
		}
		if (out.Runtime != nil) != (who == owner) {
			t.Fatal("CLI runtime authorization mismatch")
		}
		if who == owner && (out.Runtime.Platform.OS != "linux" || out.Runtime.Platform.Validation != "runtime_verified") {
			t.Fatalf("actual runtime=%+v", out.Runtime)
		}
		data, err = command(who, "state", "inspect", "runtime-host").Output()
		if who == owner {
			var report proto.StateResult
			if err != nil || json.Unmarshal(data, &report) != nil || report.Root != "state" {
				t.Fatalf("CLI state=%v %s", err, data)
			}
		} else if err == nil || len(data) != 0 {
			t.Fatal("state inspect bypassed separate grant")
		}
		if data, err = command(who, "state", "migrate", "runtime-host", "-dry-run").Output(); err == nil || len(data) != 0 {
			t.Fatal("state migration preview bypassed approval")
		}
		mc := mcp.NewClient(&mcp.Implementation{Name: "phase6-proof", Version: "1"}, nil)
		session, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command(who, "serve")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_support", Arguments: map[string]any{"host": "runtime-host"}})
		if err != nil || result.IsError {
			t.Fatalf("SDK support failed: %v %+v", err, result)
		}
		data, _ = json.Marshal(result.StructuredContent)
		var sdk support.Discovery
		if json.Unmarshal(data, &sdk) != nil || (sdk.Runtime != nil) != (who == owner) {
			t.Fatal("SDK support authorization differs from CLI")
		}
		result, err = session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_state", Arguments: map[string]any{"host": "runtime-host", "action": "inspect"}})
		if err != nil || result.IsError != (who == denied) {
			t.Fatal("SDK state admin grant mismatch")
		}
		_ = session.Close()
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("shared discovery or state used private SSH fallback")
	}
	// Standalone MCP uses an in-memory host definition; no registry is written.
	cmd := exec.CommandContext(t.Context(), cli, "serve")
	cmd.Dir = d.dir
	cmd.Env = append(d.env, "RDEV_BROKER_SOCKET=")
	mc := mcp.NewClient(&mcp.Implementation{Name: "phase6-standalone", Version: "1"}, nil)
	session, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	remote := os.Getenv("RDEV_TEST_REMOTE")
	if remote == "" {
		remote = "service-deploy"
	}
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_session", Arguments: map[string]any{"host": "runtime-host", "addr": remote, "remote_dir": namespace, "login_shell": false}})
	if err != nil || result.IsError {
		t.Fatalf("standalone host registration failed: %v", err)
	}
	for _, tool := range []struct {
		name string
		args map[string]any
	}{{"rdev_support", map[string]any{"host": "runtime-host"}}, {"rdev_state", map[string]any{"host": "runtime-host", "action": "inspect"}}} {
		result, err = session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool.name, Arguments: tool.args})
		if err != nil || result.IsError {
			t.Fatalf("standalone SDK %s failed: %v %+v", tool.name, err, result)
		}
	}
	t.Log("actual CLI and official SDK: support probes only granted target; denied/unknown targets disclose only own denials without SSH; state inspection requires host-wide admin grant and migration preview retains approval; standalone SDK probe/state pass")
}
