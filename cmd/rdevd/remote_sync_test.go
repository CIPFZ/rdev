package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Only an actual rsync server response is held. Normal base/bulk agent SSH
// processes exec OpenSSH directly and remain independent of the canceled group.
const syncPreviewSSHWrapper = `#!/usr/bin/env python3
import os,sys,subprocess,time,json
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'):args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
args+=sys.argv[1:]
gate=os.environ['RDEV_SYNC_GATE']
if 'rsync' not in sys.argv or not os.path.exists(gate):os.execv(args[0],args)
p=subprocess.Popen(args,stdin=sys.stdin.buffer,stdout=subprocess.PIPE,stderr=sys.stderr.buffer)
first=p.stdout.read(1)
with open(gate+'.entered','w') as f:json.dump([os.getpid(),p.pid],f)
while os.path.exists(gate):time.sleep(.01)
sys.stdout.buffer.write(first);sys.stdout.buffer.flush()
while True:
 b=p.stdout.read(32768)
 if not b:break
 sys.stdout.buffer.write(b);sys.stdout.buffer.flush()
sys.exit(p.wait())
`

func TestRemoteBrokerSyncPreview(t *testing.T) {
	d, namespace, ssh := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "sync-shared", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	denied := broker.Owner{ClientID: a.ClientID, ProjectID: "denied"}
	p := broker.NewPolicy()
	if err := p.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	if err := p.GrantHost(a.Key(), "runtime-host", "secret", "secret.set"); err != nil {
		t.Fatal(err)
	}
	for _, o := range []broker.Owner{a, b} {
		for _, op := range []string{"sync.push", "sync.pull", proto.OpPing} {
			if err := p.GrantHost(o.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
		for _, op := range []string{"status", "audit_query"} {
			if err := p.Grant(o.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.GrantHost(a.Key(), "runtime-host", "sync", "sync.delete"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(d.dir, "sync-held")
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(syncPreviewSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_SYNC_GATE="+gate)
	local := t.TempDir()
	for name, value := range map[string]string{"new-file": "local-new", "excluded-file": "excluded"} {
		if err := os.WriteFile(filepath.Join(local, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	remote := "~/" + namespace + "/sync-target/"
	if out, err := ssh("import os,sys,shutil\nassert shutil.which('rsync')\np=os.path.expanduser('~/'+sys.argv[1]+'/sync-target');os.makedirs(p,exist_ok=True)\nopen(p+'/remote-only','w').write('remote-old')\n"); err != nil {
		t.Fatalf("sync fixture: %v %s", err, out)
	}
	d.start()
	wires := map[broker.Owner]*runtimeWire{}
	connect := func() {
		for _, o := range []broker.Owner{a, b, denied} {
			wires[o] = d.dial(o, d.token(o, "5m"), true)
		}
	}
	connect()
	call := func(o broker.Owner, r broker.Request) broker.Response {
		r.Owner = o
		return policyRuntimeRequest(t, wires[o], r)
	}
	request := func(direction string) broker.Request {
		return broker.Request{Operation: "sync." + direction, Host: "runtime-host", Sync: &client.SyncOptions{Direction: direction, Local: local + "/", Remote: remote, DryRun: true, Exclude: []string{"excluded-file"}}}
	}
	var rejectionRefs []string
	for _, modify := range []func(*broker.Request){
		func(r *broker.Request) { r.Sync.DryRun = false; r.Risk = false },
		func(r *broker.Request) { r.Sync.Local = "relative" },
		func(r *broker.Request) { r.Sync.Direction = "pull" },
		func(r *broker.Request) { r.Sync.Host = "other-host" },
		func(r *broker.Request) { r.Wire = &proto.Request{Op: proto.OpExec} },
		func(r *broker.Request) { r.Sync.MaxOutputBytes = -1 },
		func(r *broker.Request) { r.Sync.Remote = "dst;touch-invalid" },
		func(r *broker.Request) { r.Capability = "file.read" },
	} {
		r := request("push")
		modify(&r)
		response := call(a, r)
		if response.OK || response.Sync != nil || response.Mutation != nil || response.RequestRef == "" || response.PolicyDigest == "" {
			t.Fatal("invalid sync route acquired authority or returned data")
		}
		rejectionRefs = append(rejectionRefs, response.RequestRef)
	}
	for _, r := range []broker.Request{request("push"), request("pull")} {
		if got := call(denied, r); got.OK || got.Error != "denied by default" {
			t.Fatal("another project inherited sync authority")
		}
		r.Host = "other-host"
		if got := call(a, r); got.OK || got.Error != "denied by default" {
			t.Fatal("sync grant expanded to another host")
		}
	}
	deleteReq := request("push")
	deleteReq.Sync.Delete = true
	if r := call(b, deleteReq); r.OK || r.Error != "denied by default" {
		t.Fatal("delete preview bypassed its additional capability")
	}
	if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 {
		t.Fatal("sync denial dialed a remote connection")
	}
	const privateName = "sync-preview-private-value"
	secret := &broker.SecretParams{Name: "sync-test", Value: privateName}
	admin := runtimeApprovalAdmin()
	adminWire := d.dial(admin, d.token(admin, "5m"), true)
	approval := policyRuntimeRequest(t, adminWire, broker.Request{Owner: admin, Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: a, Host: "runtime-host", Operation: "secret.set", Secret: secret}})
	if !approval.OK || approval.Approval == nil {
		t.Fatal("sync redaction fixture approval failed")
	}
	if r := call(a, broker.Request{Operation: "secret.set", Host: "runtime-host", Secret: secret, Approval: approval.Approval.Token}); !r.OK {
		t.Fatal("sync redaction fixture registration failed")
	}
	if err := os.WriteFile(filepath.Join(local, privateName), []byte("name-redaction"), 0600); err != nil {
		t.Fatal(err)
	}
	preview := func(o broker.Owner, req broker.Request) *client.SyncResult {
		t.Helper()
		r := call(o, req)
		if !r.OK || r.Sync == nil || !r.Sync.DryRun || r.Sync.ExitCode != 0 {
			t.Fatalf("real rsync preview failed: %s %+v", r.Error, r.Sync)
		}
		return r.Sync
	}
	push := preview(a, request("push"))
	if !strings.Contains(push.Stdout, "new-file") || strings.Contains(push.Stdout, "excluded-file") || strings.Contains(push.Stdout, privateName) || !strings.Contains(push.Stdout, "redacted") || !push.ManifestComplete || push.ManifestDigest == "" {
		t.Fatal("push preview lost its source manifest or exclusion semantics")
	}
	if !strings.Contains(preview(a, deleteReq).Stdout, "remote-only") {
		t.Fatal("delete preview omitted destination-only files")
	}
	if !strings.Contains(preview(b, request("pull")).Stdout, "remote-only") {
		t.Fatal("pull preview omitted its remote source")
	}
	ping := func() int {
		t.Helper()
		r := call(b, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
		if !r.OK || r.Wire == nil || r.Wire.Ping == nil {
			t.Fatal("other owner lost its base connection")
		}
		return r.Wire.Ping.PID
	}
	basePID := ping()
	cli := os.Getenv("RDEV_TEST_CLI_BINARY")
	if cli == "" {
		cli = filepath.Join(t.TempDir(), "rdev")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build sync frontend: %v %s", err, out)
		}
	}
	trapDir := filepath.Join(d.dir, "frontend-tools")
	if err := os.Mkdir(trapDir, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(d.dir, "frontend-fallback")
	for _, tool := range []string{"ssh", "rsync"} {
		if err := os.WriteFile(filepath.Join(trapDir, tool), []byte("#!/bin/sh\n: > \"$RDEV_SYNC_FALLBACK\"\nexit 99\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	command := func(o broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Dir = local
		cmd.Env = append(os.Environ(), "PATH="+trapDir+string(os.PathListSeparator)+os.Getenv("PATH"), "RDEV_SYNC_FALLBACK="+marker, "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+o.ClientID, "RDEV_PROJECT_ID="+o.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(o, "5m"))
		return cmd
	}
	if out, err := command(a, "sync", "runtime-host", "push", "./", remote, "-dry-run", "-exclude", "excluded-file").Output(); err != nil || !strings.Contains(string(out), "new-file") || strings.Contains(string(out), "excluded-file") {
		t.Fatalf("CLI shared preview failed: %v", err)
	}
	mc := mcp.NewClient(&mcp.Implementation{Name: "sync-preview-proof", Version: "1"}, nil)
	ms, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command(b, "serve")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	result, err := ms.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_sync", Arguments: map[string]any{"host": "runtime-host", "direction": "pull", "local": local + "/", "remote": remote, "dry_run": true}})
	data, _ := json.Marshal(result)
	if err != nil || result == nil || result.IsError || !strings.Contains(string(data), "remote-only") {
		t.Fatal("MCP shared pull preview failed")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("sync frontend used a private SSH/rsync fallback")
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	pending := d.dial(a, d.token(a, "5m"), true)
	r := request("push")
	r.Owner = a
	if err := pending.enc.Encode(r); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "real rsync server response held", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
	state := call(a, broker.Request{Operation: "status"})
	other := call(b, broker.Request{Operation: "status"})
	if state.Scheduler == nil || state.Scheduler.Lanes[broker.LaneBulk].Active != 1 || state.Ingress.ObservationBytes != broker.SyncPreviewBudget(r) || state.Ingress.Bytes < 2*broker.SyncPreviewBudget(r) || other.Ingress.ObservationBytes != 0 || other.Scheduler.Active != 0 {
		t.Fatal("sync worker/capture reservation missing or crossed projects")
	}
	if ping() != basePID {
		t.Fatal("held sync replaced the other owner's base agent")
	}
	pending.Close()
	awaitRuntime(t, 5*time.Second, "canceled sync process and capture release", func() bool {
		s := call(a, broker.Request{Operation: "status"})
		return s.OK && s.Scheduler.Active == 0 && s.Ingress.ObservationBytes == 0
	})
	if ping() != basePID {
		t.Fatal("canceling sync replaced the shared base agent")
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	var descendants []int
	raw, err := os.ReadFile(gate + ".entered")
	if err != nil || json.Unmarshal(raw, &descendants) != nil || len(descendants) != 2 {
		t.Fatal("held SSH process identities missing")
	}
	for _, pid := range descendants {
		if raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
			fields := strings.Fields(string(raw)[strings.LastIndex(string(raw), ")")+1:])
			if len(fields) == 0 || fields[0] != "Z" {
				t.Fatal("canceled rsync left its SSH child running")
			}
		}
	}
	for i := range 250 {
		if err := os.WriteFile(filepath.Join(local, fmt.Sprintf("bounded-output-%03d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	r = request("push")
	r.Sync.MaxOutputBytes = 1024
	bounded := preview(a, r)
	if !bounded.Truncated || bounded.StdoutTruncation.RetainedBytes > 1024 || bounded.StdoutTruncation.DroppedBytes == 0 {
		t.Fatal("sync output was not bounded with an exact truncation ledger")
	}
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/sync-target')\nassert os.listdir(p)==['remote-only']\nassert open(p+'/remote-only').read()=='remote-old'\n"); err != nil {
		t.Fatalf("preview mutated remote tree: %v %s", err, out)
	}
	if _, err := os.Stat(filepath.Join(local, "remote-only")); !os.IsNotExist(err) {
		t.Fatal("pull preview wrote the local destination")
	}
	audit := call(a, broker.Request{Operation: "audit_query"})
	seen := map[string]bool{}
	for _, event := range audit.Audit {
		if event.Owner != broker.AuditOwnerID(a.Key()) {
			t.Fatal("sync audit crossed projects")
		}
		for _, ref := range rejectionRefs {
			if event.RequestRef == ref && event.PolicyDigest != "" && (event.Result == "route_rejected" || event.Result == "denied") {
				seen[ref] = true
			}
		}
	}
	data, _ = json.Marshal(audit.Audit)
	if !audit.OK || len(seen) != len(rejectionRefs) || strings.Contains(string(data), local) || strings.Contains(string(data), remote) || strings.Contains(string(data), "new-file") || strings.Contains(string(data), privateName) {
		t.Fatal("sync audit lost request decisions or recorded source/destination/output")
	}
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	preview(b, request("pull"))
	if got := call(b, deleteReq); got.OK || got.Error != "denied by default" {
		t.Fatal("restart broadened sync delete authority")
	}
	t.Log("actual daemon/CLI/MCP/rsync/SSH: push/pull/delete previews preserve both trees; exact host/project/default/capability and malformed-route denial before dial; separate delete grant; no frontend fallback; bounded output and independently held owner capture budget; held real server response cancellation terminates SSH descendants and preserves the other project's base PID; request/outcome audit excludes paths/output; same authority after SIGKILL; manifest-bound shared execution remains pending")
}
