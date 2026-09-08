package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

type remoteProcessResult struct {
	PID      int       `json:"pid"`
	Client   string    `json:"client"`
	Calls    int       `json:"calls"`
	PingMS   []float64 `json:"ping_ms"`
	ExecMS   []float64 `json:"exec_ms"`
	CallerID string    `json:"caller_id"`
	AgentPID int       `json:"agent_pid"`
}

func TestRemoteBrokerProcesses(t *testing.T) {
	if os.Getenv("RDEV_REMOTE_PROCESS_HELPER") == "1" {
		runRemoteProcessClient(t)
		return
	}
	d, _, sshRun := newRemoteRuntime(t)
	const clients, rounds = 20, 25
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	owners := make([]broker.Owner, clients)
	for i := range owners {
		owners[i] = broker.Owner{ClientID: fmt.Sprintf("remote-client-%02d", i%10), ProjectID: fmt.Sprintf("phase5-%d", i/10)}
		policy.Grant(owners[i].Key(), "ping")
		policy.Grant(owners[i].Key(), "exec")
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	type child struct {
		cmd  *exec.Cmd
		log  *bytes.Buffer
		done chan error
		id   string
	}
	children := make([]child, 0, clients)
	for i, owner := range owners {
		instance := fmt.Sprintf("process-%02d", i)
		approvals := make([]string, rounds)
		for round := 0; round < rounds; round++ {
			wire := remoteBenchmarkRequest(round)
			if broker.RequiresApproval(broker.Request{Operation: wire.Op, Wire: wire}) {
				approvals[round] = d.approve(owner, wire)
			}
		}
		approvalData, _ := json.Marshal(approvals)
		if err := os.WriteFile(filepath.Join(d.dir, instance+".approvals"), approvalData, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestRemoteBrokerProcesses$", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "RDEV_REMOTE_PROCESS_HELPER=1", "RDEV_REMOTE_INSTANCE_ID="+instance, "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PROJECT_ID="+owner.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(owner, "5m"), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_REMOTE_PROCESS_DIR="+d.dir, "RDEV_REMOTE_PROCESS_ROUNDS="+strconv.Itoa(rounds))
		log := new(bytes.Buffer)
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		children = append(children, child{cmd: cmd, log: log, done: done, id: instance})
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		ready := 0
		for _, child := range children {
			if _, err := os.Stat(filepath.Join(d.dir, child.id+".ready")); err == nil {
				ready++
			}
		}
		if ready == clients {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("20 separate clients did not all attach before the start barrier")
		}
		time.Sleep(10 * time.Millisecond)
	}
	started := time.Now()
	if err := os.WriteFile(filepath.Join(d.dir, "start"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	completed := make(map[int]bool)
	maxAgent, maxSSH, samples := 0, 0, 0
	agentPIDs := make(map[int]bool)
	deadline = time.Now().Add(90 * time.Second)
	for len(completed) < clients {
		for i, child := range children {
			if completed[i] {
				continue
			}
			select {
			case err := <-child.done:
				completed[i] = true
				if err != nil {
					t.Fatalf("client %s failed: %v\n%s", child.id, err, child.log.String())
				}
			default:
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("remote process workload stalled: %d/%d finished", len(completed), clients)
		}
		out, err := sshRun(remoteAgentPIDScript)
		if err != nil {
			t.Fatalf("inspect remote agent processes: %v %s", err, out)
		}
		var pids []int
		if err := json.Unmarshal(out, &pids); err != nil {
			t.Fatalf("remote PID evidence: %v %s", err, out)
		}
		if len(pids) > maxAgent {
			maxAgent = len(pids)
		}
		for _, pid := range pids {
			agentPIDs[pid] = true
		}
		sshCount := countDaemonSSHChildren(t, d.cmd.Process.Pid)
		if sshCount > maxSSH {
			maxSSH = sshCount
		}
		samples++
		time.Sleep(25 * time.Millisecond)
	}
	if maxAgent != 1 || len(agentPIDs) != 1 {
		t.Fatalf("shared base agent evidence failed: concurrent=%d distinct=%d", maxAgent, len(agentPIDs))
	}
	if maxSSH != 1 {
		t.Fatalf("daemon owned %d simultaneous SSH children, want one", maxSSH)
	}
	var allPing, allExec []float64
	processPIDs := make(map[int]bool)
	principalIDs := make(map[string]bool)
	for _, child := range children {
		data, err := os.ReadFile(filepath.Join(d.dir, child.id+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var result remoteProcessResult
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result.Calls != rounds || result.Client != child.id {
			t.Fatalf("incomplete client result: %+v", result)
		}
		processPIDs[result.PID] = true
		if result.CallerID == "" || !agentPIDs[result.AgentPID] {
			t.Fatal("remote agent did not report the observed shared PID and principal")
		}
		principalIDs[result.CallerID] = true
		allPing = append(allPing, result.PingMS...)
		allExec = append(allExec, result.ExecMS...)
	}
	if len(processPIDs) != clients {
		t.Fatalf("only %d independent client PIDs", len(processPIDs))
	}
	if len(principalIDs) != clients {
		t.Fatalf("remote agent saw only %d distinct principals for %d client/project pairs", len(principalIDs), clients)
	}
	metrics := map[string]any{"clients": clients, "calls": clients * rounds, "elapsed_ms": float64(time.Since(started).Microseconds()) / 1000, "remote_agent_max": maxAgent, "remote_agent_distinct": len(agentPIDs), "remote_principals": len(principalIDs), "daemon_ssh_max": maxSSH, "process_samples": samples, "ping_p50_ms": percentile(allPing, .50), "ping_p95_ms": percentile(allPing, .95), "ping_p99_ms": percentile(allPing, .99), "exec_p95_ms": percentile(allExec, .95)}
	report, _ := json.Marshal(metrics)
	t.Logf("real SSH multi-process benchmark: %s", report)
}

// newRemoteRuntime uses isolated local and remote namespaces and real OpenSSH.
func newRemoteRuntime(t *testing.T) (*runtimeDaemon, string, func(string) ([]byte, error)) {
	t.Helper()
	if os.Getenv("RDEV_RUN_REMOTE") != "1" {
		t.Skip("set RDEV_RUN_REMOTE=1 for the real SSH multi-process benchmark")
	}
	if runtime.GOOS != "linux" {
		t.Skip("local /proc process-count evidence currently requires Linux")
	}
	remote := os.Getenv("RDEV_TEST_REMOTE")
	if remote == "" {
		remote = "service-deploy"
	}
	sshConfig := os.Getenv("RDEV_TEST_SSH_CONFIG")
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		t.Fatal(err)
	}
	bin := os.Getenv("RDEV_TEST_DAEMON_BINARY")
	if bin == "" {
		bin = filepath.Join(t.TempDir(), "rdevd")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", bin, ".")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build daemon: %v %s", err, out)
		}
	}
	d := newRuntimeDaemon(t, bin)
	namespace := ".cache/rdev-phase5-" + filepath.Base(d.dir)
	sshRun := func(script string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		args := []string{}
		if sshConfig != "" {
			args = append(args, "-F", sshConfig)
		}
		args = append(args, remote, "python3 - '"+namespace+"'")
		cmd := exec.CommandContext(ctx, sshPath, args...)
		cmd.Stdin = strings.NewReader(script)
		return cmd.CombinedOutput()
	}
	t.Cleanup(func() {
		d.stop(syscall.SIGTERM)
		out, err := sshRun("import os,sys,shutil\np=os.path.expanduser('~/'+sys.argv[1])\nassert '/.cache/rdev-phase5-' in p\nshutil.rmtree(p,ignore_errors=True)\n")
		if err != nil {
			t.Errorf("remote test namespace cleanup failed: %v %s", err, out)
		}
	})
	// The wrapper only supplies the user's selected SSH configuration. It execs
	// real OpenSSH unchanged; transport, agent and daemon code are not replaced.
	wrapDir := filepath.Join(d.dir, "tools")
	if err := os.Mkdir(wrapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	wrap := "#!/bin/sh\nif [ -n \"$RDEV_TEST_SSH_CONFIG\" ]; then exec \"$RDEV_TEST_REAL_SSH\" -F \"$RDEV_TEST_SSH_CONFIG\" \"$@\"; fi\nexec \"$RDEV_TEST_REAL_SSH\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(wrapDir, "ssh"), []byte(wrap), 0o700); err != nil {
		t.Fatal(err)
	}
	d.env = append(os.Environ(), "PATH="+wrapDir+string(os.PathListSeparator)+os.Getenv("PATH"), "RDEV_TEST_REAL_SSH="+sshPath, "RDEV_TEST_SSH_CONFIG="+sshConfig)
	hosts := map[string]any{"hosts": []map[string]any{{"name": "runtime-host", "addr": remote, "remote_dir": namespace, "login_shell": false}}}
	data, _ := json.Marshal(hosts)
	hostsPath := filepath.Join(d.dir, "hosts.json")
	if err := os.WriteFile(hostsPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	d.extraArgs = []string{"-hosts-file", hostsPath, "-agent-dir", filepath.Join(repoRoot(t), "cmd", "rdev", "agents")}
	return d, namespace, sshRun
}

const remoteAgentPIDScript = `import os,sys,json
binary=os.path.expanduser('~/'+sys.argv[1]+'/rdev-agent')
pids=[]
for name in os.listdir('/proc'):
 if not name.isdigit(): continue
 try:
  args=open('/proc/'+name+'/cmdline','rb').read().split(b'\0')
  if args and os.fsdecode(args[0])==binary and b'-supervise' not in args: pids.append(int(name))
 except (FileNotFoundError,ProcessLookupError,PermissionError): pass
print(json.dumps(sorted(pids)))
`

func countDaemonSSHChildren(t *testing.T, daemonPID int) int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "status"))
		if err != nil {
			continue
		}
		parent, ssh := false, false
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "PPid:") {
				parent = strings.TrimSpace(strings.TrimPrefix(line, "PPid:")) == strconv.Itoa(daemonPID)
			}
			if strings.HasPrefix(line, "Name:") {
				ssh = strings.TrimSpace(strings.TrimPrefix(line, "Name:")) == "ssh"
			}
		}
		if parent && ssh {
			count++
		}
	}
	return count
}

func runRemoteProcessClient(t *testing.T) {
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dir := os.Getenv("RDEV_REMOTE_PROCESS_DIR")
	instance := os.Getenv("RDEV_REMOTE_INSTANCE_ID")
	if err := os.WriteFile(filepath.Join(dir, instance+".ready"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "start")); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(time.Millisecond)
	}
	rounds, err := strconv.Atoi(os.Getenv("RDEV_REMOTE_PROCESS_ROUNDS"))
	if err != nil {
		t.Fatal(err)
	}
	result := remoteProcessResult{PID: os.Getpid(), Client: instance}
	data, err := os.ReadFile(filepath.Join(dir, instance+".approvals"))
	if err != nil {
		t.Fatal(err)
	}
	var approvals []string
	if json.Unmarshal(data, &approvals) != nil || len(approvals) != rounds {
		t.Fatal("missing administrator approvals")
	}
	for i := 0; i < rounds; i++ {
		wire := remoteBenchmarkRequest(i)
		started := time.Now()
		response, err := c.DoContext(ctx, broker.Request{Approval: approvals[i], Operation: wire.Op, Host: "runtime-host", Wire: wire})
		elapsed := float64(time.Since(started).Microseconds()) / 1000
		if err != nil || !response.OK || response.Wire == nil || !response.Wire.OK {
			t.Fatalf("remote %s failed: %v %+v", wire.Op, err, response)
		}
		if wire.Op == proto.OpPing {
			if response.Wire.Ping == nil || response.Wire.Ping.CallerID == "" {
				t.Fatal("remote ping has no principal evidence")
			}
			if result.CallerID != "" && (result.CallerID != response.Wire.Ping.CallerID || result.AgentPID != response.Wire.Ping.PID) {
				t.Fatal("remote principal or agent changed during the workload")
			}
			result.CallerID = response.Wire.Ping.CallerID
			result.AgentPID = response.Wire.Ping.PID
			result.PingMS = append(result.PingMS, elapsed)
		} else {
			if response.Wire.Exec == nil || response.Wire.Exec.ExitCode != 0 || response.Wire.Exec.Stdout != "broker-ok" {
				t.Fatalf("incorrect remote exec result: %+v", response.Wire.Exec)
			}
			result.ExecMS = append(result.ExecMS, elapsed)
		}
		result.Calls++
	}
	data, err = json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, instance+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return values[int(float64(len(values)-1)*p)]
}

func remoteBenchmarkRequest(round int) *proto.Request {
	if round%2 == 0 {
		return &proto.Request{Op: proto.OpPing}
	}
	return &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "sleep .02; printf broker-ok"}, TimeoutSec: 10, MaxOutputBytes: 256}}
}
