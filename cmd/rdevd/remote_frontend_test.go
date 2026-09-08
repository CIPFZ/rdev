package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestRemoteBrokerFrontendBoundary(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "frontend-shared", ProjectID: "pressure"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "reader"}
	denied := broker.Owner{ClientID: a.ClientID, ProjectID: "denied"}
	p := broker.NewPolicy()
	for _, owner := range []broker.Owner{a, b} {
		if err := p.Grant(owner.Key(), "status"); err != nil {
			t.Fatal(err)
		}
		if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(proto.OpList), proto.OpList); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	tokens := map[broker.Owner]string{}
	for _, owner := range []broker.Owner{a, b, denied} {
		tokens[owner] = d.token(owner, "5m")
	}
	for range 8 {
		d.dial(a, tokens[a], true)
	}
	cli := os.Getenv("RDEV_TEST_CLI_BINARY")
	if cli == "" {
		cli = filepath.Join(d.dir, "rdev-frontend")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build frontend: %v %s", err, out)
		}
	}

	// Only the CLI gets this trap. The actual daemon uses real OpenSSH. A local
	// fallback would touch the marker instead of entering the broker socket.
	trapDir := filepath.Join(d.dir, "frontend-tools")
	if err := os.Mkdir(trapDir, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(d.dir, "direct-transport-used")
	for _, tool := range []string{"ssh", "rsync"} {
		if err := os.WriteFile(filepath.Join(trapDir, tool), []byte("#!/bin/sh\n: > \"$RDEV_FRONTEND_FALLBACK_MARKER\"\nexit 99\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	project := filepath.Join(d.dir, "frontend-project")
	if err := os.Mkdir(project, 0700); err != nil {
		t.Fatal(err)
	}
	command := func(owner broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Dir = project
		cmd.Env = append(os.Environ(), "PATH="+trapDir+string(os.PathListSeparator)+os.Getenv("PATH"), "RDEV_FRONTEND_FALLBACK_MARKER="+marker, "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PROJECT_ID="+owner.ProjectID, "RDEV_PRINCIPAL_TOKEN="+tokens[owner])
		return cmd
	}
	for _, owner := range []broker.Owner{a, b, denied} {
		out, err := command(owner, "broker", "status").Output()
		var status broker.StatusSnapshot
		if owner == denied {
			if err == nil || len(out) != 0 {
				t.Fatal("CLI status bypassed default deny")
			}
			continue
		}
		want := 1
		if owner == a {
			want = 9
		}
		if err != nil || json.Unmarshal(out, &status) != nil || status.Ingress.Connections != want || len(status.PolicyDigest) != 64 || status.Scheduler.Active != 0 || status.Scheduler.Queued != 0 || status.Scheduler.Limits.PerOwner <= 0 {
			t.Fatal("CLI status lost scoped usage or quota projection")
		}
		if strings.Contains(string(out), a.ClientID) || strings.Contains(string(out), a.ProjectID) {
			t.Fatal("CLI status exposed owner identifiers")
		}
	}
	out, err := command(b, "ls", "runtime-host", namespace, "-limit", "1").Output()
	var list proto.ListResult
	if err != nil || json.Unmarshal(out, &list) != nil || len(list.Entries) != 1 || !list.Terminal || list.OperationID == "" {
		t.Fatal("CLI listing did not use actual broker agent")
	}
	agentPID := func() int {
		out, err := sshRun(remoteAgentPIDScript)
		var pids []int
		if err != nil || json.Unmarshal(out, &pids) != nil || len(pids) != 1 {
			t.Fatalf("base agent count: %v %s", err, out)
		}
		return pids[0]
	}
	before := agentPID()
	for _, args := range [][]string{
		{"sync", "runtime-host", "push", "local", "remote"},
		{"secrets", "list"},
		{"hosts", "add", "forbidden", "localhost", "-save"},
		{"state", "inspect", "runtime-host"},
		{"env", "inspect", "runtime-host"},
		{"job", "unsupported", "runtime-host"},
	} {
		out, err := command(b, args...).Output()
		if err == nil || len(out) != 0 {
			t.Fatalf("unsupported shared command %s fell through to standalone behavior", args[0])
		}
	}
	for _, args := range [][]string{{"ls", "ungranted-alias", "."}, {"ls", "runtime-host", namespace}} {
		owner := b
		if args[1] == "runtime-host" {
			owner = denied
		}
		if out, err := command(owner, args...).Output(); err == nil || len(out) != 0 {
			t.Fatal("CLI listing bypassed exact-host/principal policy")
		}
	}
	if _, err := os.Stat(filepath.Join(project, ".rdev")); !os.IsNotExist(err) {
		t.Fatal("unsupported hosts command changed local registry")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("broker mode opened a direct SSH/rsync process")
	}
	for _, owner := range []broker.Owner{b, denied} {
		client := mcp.NewClient(&mcp.Implementation{Name: "frontend-proof", Version: "1"}, nil)
		session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: command(owner, "serve")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range []struct {
			name string
			args map[string]any
		}{{"rdev_broker_status", map[string]any{}}, {"rdev_list", map[string]any{"host": "runtime-host", "path": namespace, "limit": 1}}} {
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: tool.name, Arguments: tool.args})
			if err != nil || result == nil || result.IsError != (owner == denied) {
				t.Fatalf("MCP %s lost policy boundary", tool.name)
			}
			if owner == denied {
				continue
			}
			data, _ := json.Marshal(result.StructuredContent)
			if tool.name == "rdev_broker_status" {
				var status broker.StatusSnapshot
				if json.Unmarshal(data, &status) != nil || status.Ingress.Connections != 1 || len(status.PolicyDigest) != 64 || status.Scheduler.Limits.PerOwner <= 0 {
					t.Fatal("MCP returned unscoped/incomplete broker status")
				}
			} else {
				var list proto.ListResult
				if json.Unmarshal(data, &list) != nil || len(list.Entries) != 1 || !list.Terminal {
					t.Fatal("MCP list projection incomplete")
				}
			}
		}
		session.Close()
	}
	if after := agentPID(); after != before {
		t.Fatalf("frontend routes replaced shared remote agent: %d -> %d", before, after)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("MCP broker mode spawned a direct transport")
	}
	t.Logf("actual CLI/MCP broker status: owner A has 8 held sockets, B sees only itself, default-denied project gets no result; remote ls uses policy and shared agent PID %d; unsupported sync/secret/host/state/env/job commands neither spawn SSH/rsync nor mutate local registry; ungranted host and principal denied", before)
}
