package main

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// A transparent real-SSH relay records only frame byte counts, fixed lane names
// and protocol principal hashes. No argv, paths, payloads or credentials are saved.
const trafficSSHWrapper = `#!/usr/bin/env python3
import os,sys,json,subprocess,threading
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'):args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
p=subprocess.Popen(args+sys.argv[1:],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=sys.stderr.buffer)
streams={};operations={};cut=set()
def record(owner,lane,direction,n):
 row={'owner':owner,'lane':lane,direction:n}
 fd=os.open(os.environ['RDEV_TRAFFIC_LOG'],os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
 try:os.write(fd,(json.dumps(row)+'\n').encode())
 finally:os.close(fd)
def inbound():
 try:
  for line in sys.stdin.buffer:
   binding=None
   try:
    r=json.loads(line);owner=r.get('client_id','');op=r.get('op','')
    if owner:
     lane='exec' if op in ('exec','job_start','job_wait') else 'bulk' if op in ('read_file','write_file') else 'control'
     if op=='cancel':lane=operations.get((r.get('cancel') or {}).get('operation_id'),lane)
     binding=(owner,lane);streams[r['id']]=binding;operations[r.get('operation_id')]=lane
     if op=='read_file' and os.path.exists(os.environ['RDEV_TRAFFIC_CUT']):cut.add(r['id'])
   except (ValueError,KeyError,AttributeError):pass
   p.stdin.write(line);p.stdin.flush()
   if binding:record(*binding,'sent_bytes',len(line))
 except (BrokenPipeError,OSError):pass
 finally:
  try:p.stdin.close()
  except (BrokenPipeError,OSError):pass
threading.Thread(target=inbound,daemon=True).start()
client_closed=False
try:
 for line in p.stdout:
  binding=None
  try:
   r=json.loads(line);rid=r.get('id');binding=streams.get(rid)
   if rid in cut and r.get('terminal') and os.path.exists(os.environ['RDEV_TRAFFIC_CUT']):
    os.unlink(os.environ['RDEV_TRAFFIC_CUT']);p.terminate();break
  except (ValueError,AttributeError):pass
  sys.stdout.buffer.write(line);sys.stdout.buffer.flush()
  if binding:record(*binding,'received_bytes',len(line))
except BrokenPipeError:client_closed=True
finally:
 if client_closed and p.poll() is None:p.terminate()
 try:p.wait(timeout=5)
 except subprocess.TimeoutExpired:
  p.terminate()
  try:p.wait(timeout=2)
  except subprocess.TimeoutExpired:p.kill();p.wait()
sys.exit(p.returncode)
`

func TestRemoteBrokerLaneTraffic(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "lane-same-client", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	denied := broker.Owner{ClientID: a.ClientID, ProjectID: "denied"}
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"status", proto.OpPing, proto.OpExec, proto.OpReadFile, proto.OpWriteFile} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	logPath, cutPath := filepath.Join(d.dir, "traffic.jsonl"), filepath.Join(d.dir, "cut-read-once")
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(trafficSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_TRAFFIC_LOG="+logPath, "RDEV_TRAFFIC_CUT="+cutPath)
	if out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]);os.makedirs(p,exist_ok=True)\nopen(p+'/binary','wb').write(bytes(range(256))*1024)\nopen(p+'/text','w').write('z'*262144)\n"); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	d.start()
	cli := os.Getenv("RDEV_TEST_CLI_BINARY")
	if cli == "" {
		cli = filepath.Join(d.dir, "rdev-traffic")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build CLI: %v %s", err, out)
		}
	}
	tokens := map[broker.Owner]string{}
	for _, owner := range []broker.Owner{a, b, denied} {
		tokens[owner] = d.token(owner, "5m")
	}
	command := func(owner broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PROJECT_ID="+owner.ProjectID, "RDEV_PRINCIPAL_TOKEN="+tokens[owner])
		return cmd
	}
	status := func(owner broker.Owner) broker.SchedulerSnapshot {
		t.Helper()
		out, err := command(owner, "broker", "status").Output()
		var snapshot broker.StatusSnapshot
		if err != nil || json.Unmarshal(out, &snapshot) != nil {
			t.Fatal("CLI traffic projection failed")
		}
		if len(snapshot.Scheduler.Traffic) != 3 {
			t.Fatal("missing lane traffic")
		}
		return snapshot.Scheduler
	}
	for _, v := range status(b).Traffic {
		if v != (observe.TrafficSnapshot{}) {
			t.Fatal("idle project inherited traffic")
		}
	}
	if out, err := command(denied, "broker", "status").Output(); err == nil || len(out) != 0 {
		t.Fatal("ungranted project read traffic")
	}
	if err := command(a, "ping", "runtime-host").Run(); err != nil {
		t.Fatal(err)
	}
	argv := []string{"python3", "-c", "import sys;sys.stdout.write('x'*65536);sys.stderr.write('y'*16384)"}
	wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: argv, LoginShell: false}}
	cmd := command(a, append([]string{"exec", "runtime-host", "-no-login", "--"}, argv...)...)
	cmd.Env = append(cmd.Env, "RDEV_APPROVAL_TOKEN="+d.approve(a, wire))
	if err := cmd.Run(); err != nil {
		t.Fatal("real CLI exec failed")
	}
	// Cut one real bulk terminal before delivery. The read-only retry must be
	// charged to the same owner/lane, including the first attempt's accepted frame.
	if err := os.WriteFile(cutPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := command(a, "read", "runtime-host", "~/"+namespace+"/text", "-limit", "262144").CombinedOutput(); err != nil {
		t.Fatalf("real text read/retry failed: %v %s", err, out)
	}
	if _, err := os.Stat(cutPath); !os.IsNotExist(err) {
		t.Fatal("retry cut was not exercised")
	}
	for _, v := range status(b).Traffic {
		if v != (observe.TrafficSnapshot{}) {
			t.Fatal("project B inherited A counters")
		}
	}
	if err := command(b, "ping", "runtime-host").Run(); err != nil {
		t.Fatal(err)
	}
	// Owner B performs bulk work through an actual MCP process.
	mc := mcp.NewClient(&mcp.Implementation{Name: "lane-proof", Version: "1"}, nil)
	session, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command(b, "serve")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_read", Arguments: map[string]any{"host": "runtime-host", "path": "~/" + namespace + "/binary", "limit": 1024}})
	if err != nil || result == nil || result.IsError {
		t.Fatal("MCP read failed")
	}
	// Kill only A's actual frontend during a streaming exec; late protocol data
	// and the automatically generated cancellation retain A's exec meter.
	argv = []string{"python3", "-c", "import time;print('ready',flush=True);time.sleep(30)"}
	wire = &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: argv, LoginShell: false}}
	before := status(a).Traffic[broker.LaneExec]
	running := command(a, append([]string{"exec", "runtime-host", "-no-login", "--"}, argv...)...)
	running.Env = append(running.Env, "RDEV_APPROVAL_TOKEN="+d.approve(a, wire))
	running.Stdout, running.Stderr = io.Discard, io.Discard
	if err := running.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = running.Process.Kill() })
	awaitRuntime(t, 5*time.Second, "actual streaming exec started", func() bool { return status(a).Traffic[broker.LaneExec].ReceivedBytes > before.ReceivedBytes })
	_ = running.Process.Kill()
	_ = running.Wait()
	awaitRuntime(t, 5*time.Second, "canceled request releases lane", func() bool { return status(a).Active == 0 })
	// Compare CLI and MCP projections with a separate transparent SSH recorder.
	expected := func() map[string]map[broker.Lane]observe.TrafficSnapshot {
		t.Helper()
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		totals := map[string]map[broker.Lane]observe.TrafficSnapshot{}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			var row struct {
				Owner string      `json:"owner"`
				Lane  broker.Lane `json:"lane"`
				observe.TrafficSnapshot
			}
			if json.Unmarshal([]byte(line), &row) != nil {
				t.Fatal("invalid private byte ledger")
			}
			if totals[row.Owner] == nil {
				totals[row.Owner] = map[broker.Lane]observe.TrafficSnapshot{}
			}
			value := totals[row.Owner][row.Lane]
			value.SentBytes += row.SentBytes
			value.ReceivedBytes += row.ReceivedBytes
			totals[row.Owner][row.Lane] = value
		}
		return totals
	}
	awaitRuntime(t, 5*time.Second, "late frame counter convergence", func() bool {
		want := expected()
		for _, owner := range []broker.Owner{a, b} {
			for lane, got := range status(owner).Traffic {
				if got != want[proto.PrincipalID(owner.ClientID, owner.ProjectID)][lane] {
					return false
				}
			}
		}
		return true
	})
	for _, owner := range []broker.Owner{a, b} {
		got := status(owner)
		if got.Traffic[broker.LaneControl].SentBytes == 0 || got.Traffic[broker.LaneBulk].ReceivedBytes == 0 {
			t.Fatal("real lanes not measured")
		}
		if owner == a && got.Traffic[broker.LaneExec].ReceivedBytes < 81920 {
			t.Fatal("streamed exec output missing")
		}
		if owner == b && got.Traffic[broker.LaneExec] != (observe.TrafficSnapshot{}) {
			t.Fatal("A late exec/cancel traffic charged to B")
		}
		t.Logf("project %s independent SSH byte ledger matched CLI traffic: %+v", owner.ProjectID, got.Traffic)
	}
	result, err = session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_broker_status", Arguments: map[string]any{}})
	if err != nil || result == nil || result.IsError {
		t.Fatal("MCP traffic status failed")
	}
	data, _ := json.Marshal(result.StructuredContent)
	var mcpStatus broker.StatusSnapshot
	if json.Unmarshal(data, &mcpStatus) != nil {
		t.Fatal("MCP traffic status decode")
	}
	for lane, value := range status(b).Traffic {
		if mcpStatus.Scheduler.Traffic[lane] != value {
			t.Fatal("MCP/CLI counters differ")
		}
	}
	session.Close()
	t.Log("real daemon/CLI/MCP/SSH: three lanes, binary/base64 and streaming bytes, dropped-terminal retry, frontend SIGKILL and late cancel frames matched independent byte-only recorder; exact project isolation and default denial passed")
}
