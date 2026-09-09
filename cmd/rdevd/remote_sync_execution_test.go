package main

import (
	"encoding/json"
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

func TestRemoteBrokerPreparedSync(t *testing.T) {
	d, namespace, ssh := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "prepared-sync", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	admin := runtimeApprovalAdmin()
	policy := broker.NewPolicy()
	if err := policy.Grant(admin.Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"sync.push", "sync.pull", proto.OpPing} {
			if err := policy.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
		for _, op := range []string{"status", "mutation.status", "audit_query"} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := policy.GrantHost(a.Key(), "runtime-host", "sync", "sync.delete"); err != nil {
		t.Fatal(err)
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	source := t.TempDir()
	payload := "retained-original-" + strings.Repeat("binary\x00", 30000)
	if err := os.WriteFile(filepath.Join(source, "file"), []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "excluded"), []byte("source excluded"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("file", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	remote := "~/" + namespace + "/target/"
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/target');os.makedirs(p)\nopen(p+'/obsolete','w').write('old')\nopen(p+'/excluded','w').write('keep excluded')\n"); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	gate := filepath.Join(d.dir, "sync-commit-gate")
	t.Cleanup(func() { _ = os.Remove(gate) })
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(mutationSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_MUTATION_GATE="+gate)
	d.start()
	wires := map[broker.Owner]*runtimeWire{}
	connect := func() {
		for _, owner := range []broker.Owner{a, b, admin} {
			wires[owner] = d.dial(owner, d.token(owner, "10m"), true)
		}
	}
	connect()
	call := func(owner broker.Owner, r broker.Request) broker.Response {
		r.Owner = owner
		return policyRuntimeRequest(t, wires[owner], r)
	}
	prepare := func(owner broker.Owner, opts client.SyncOptions) broker.Request {
		t.Helper()
		opts.Prepare = true
		opts.DryRun = true
		r := broker.Request{Operation: "sync." + opts.Direction, Host: "runtime-host", Sync: &opts}
		result := call(owner, r)
		if !result.OK || result.Sync == nil || result.Sync.PlanID == "" || !result.Sync.ManifestComplete {
			t.Fatalf("prepare: %s %+v", result.Error, result.Sync)
		}
		opts.Prepare = false
		opts.DryRun = false
		opts.PlanID = result.Sync.PlanID
		opts.ConfirmDelete = opts.Delete
		r.Sync = &opts
		return r
	}
	approve := func(owner broker.Owner, r broker.Request) broker.Request {
		t.Helper()
		result := call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: owner, Operation: r.Operation, Host: r.Host, Sync: r.Sync}})
		if !result.OK || result.Approval == nil {
			t.Fatalf("approval: %s", result.Error)
		}
		r.Approval = result.Approval.Token
		r.OperationID, _ = proto.NewOperationID()
		return r
	}
	ping := func() int {
		r := call(b, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
		if !r.OK || r.Wire == nil || r.Wire.Ping == nil {
			t.Fatal("peer ping failed", r.Error)
		}
		return r.Wire.Ping.PID
	}
	base := ping()
	// Root spelling is part of transfer layout, then normalized for filesystem
	// identity checks. Exercise real absolute remote paths (not expandHome).
	remoteBaseRaw, err := ssh("import os,sys\nprint(os.path.expanduser('~/'+sys.argv[1]))\n")
	if err != nil {
		t.Fatal(err)
	}
	remoteBase := strings.TrimSpace(string(remoteBaseRaw))
	for _, existing := range []bool{false, true} {
		name := "missing-slash"
		if existing {
			name = "existing-slash"
			if out, err := ssh("import os,sys\nos.mkdir(os.path.expanduser('~/'+sys.argv[1]+'/existing-slash'))\n"); err != nil {
				t.Fatal(err, string(out))
			}
		}
		upload := approve(a, prepare(a, client.SyncOptions{Direction: "push", Local: filepath.Join(source, "file"), Remote: remoteBase + "/" + name + "/"}))
		if r := call(a, upload); !r.OK || r.Mutation == nil || !r.Mutation.RemoteOK {
			t.Fatal("single-file push to directory operand", existing, r.Error)
		}
		destination := filepath.Join(t.TempDir(), "download")
		if existing {
			if err := os.Mkdir(destination, 0700); err != nil {
				t.Fatal(err)
			}
		}
		download := approve(a, prepare(a, client.SyncOptions{Direction: "pull", Local: destination + "/", Remote: remoteBase + "/" + name + "/file"}))
		if r := call(a, download); !r.OK || r.Mutation == nil || !r.Mutation.RemoteOK {
			t.Fatal("single-file pull to directory operand", existing, r.Error)
		}
		if data, err := os.ReadFile(filepath.Join(destination, "file")); err != nil || string(data) != payload {
			t.Fatal("directory operand changed transfer layout", err)
		}
	}
	// A replacement cannot consume protected descendants, including without
	// --delete. Rejection happens while preparing, before an approval is issued.
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/protected-replacement/file');os.makedirs(p);open(p+'/protected','w').write('keep')\n"); err != nil {
		t.Fatal(err, string(out))
	}
	for _, deletion := range []bool{false, true} {
		options := &client.SyncOptions{Direction: "push", Local: filepath.Join(source, "file"), Remote: remoteBase + "/protected-replacement/", Exclude: []string{"protected"}, Delete: deletion, Prepare: true, DryRun: true}
		if r := call(a, broker.Request{Operation: "sync.push", Host: "runtime-host", Sync: options}); r.OK {
			t.Fatal("approved replacement could delete excluded contents")
		}
	}
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/protected-replacement/file/protected');assert open(p).read()=='keep'\n"); err != nil {
		t.Fatal(err, string(out))
	}
	opts := client.SyncOptions{Direction: "push", Local: source + "/", Remote: remote, Delete: true, Exclude: []string{"excluded"}}
	req := prepare(a, opts)
	if r := call(a, req); r.OK {
		t.Fatal("execution without approval accepted")
	}
	if r := call(b, req); r.OK {
		t.Fatal("foreign owner obtained plan/delete authority")
	}
	req = approve(a, req)
	for _, change := range []func(*broker.Request){
		func(r *broker.Request) { r.Sync.Remote += "other" },
		func(r *broker.Request) { r.Sync.Delete = false },
		func(r *broker.Request) { r.Sync.Exclude = nil },
		func(r *broker.Request) { r.Sync.PlanID = strings.Repeat("a", 64) },
		func(r *broker.Request) { r.Host = "other-host" },
	} {
		changed := req
		copy := *req.Sync
		changed.Sync = &copy
		change(&changed)
		if r := call(a, changed); r.OK {
			t.Fatal("substitution accepted")
		}
	}
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("edited after preparation"), 0600); err != nil {
		t.Fatal(err)
	}
	executed := call(a, req)
	if !executed.OK || executed.Mutation == nil || executed.Mutation.State != "completed" || !executed.Mutation.RemoteOK {
		t.Fatalf("execute: %s %+v", executed.Error, executed.Mutation)
	}
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/target')\nassert open(p+'/file','rb').read()==b'retained-original-'+b'binary\\x00'*30000\nassert not os.path.exists(p+'/obsolete')\nassert open(p+'/excluded').read()=='keep excluded'\nassert os.readlink(p+'/link')=='file'\n"); err != nil {
		t.Fatalf("retained push/delete: %v %s", err, out)
	}
	if call(a, req).OK {
		t.Fatal("approval/plan replay accepted")
	}
	if r := call(b, broker.Request{Operation: "mutation.status", MutationID: req.OperationID}); r.OK || r.Mutation != nil {
		t.Fatal("foreign outcome exposed")
	}
	if ping() != base {
		t.Fatal("sync replaced peer base agent")
	}
	// Retain a remote pull, then edit that source before approving local writes.
	local := filepath.Join(t.TempDir(), "pull")
	if err := os.Mkdir(local, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"obsolete", "excluded"} {
		if err := os.WriteFile(filepath.Join(local, name), []byte("local retained"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	pull := prepare(a, client.SyncOptions{Direction: "pull", Local: local, Remote: remote, Delete: true, Exclude: []string{"excluded"}})
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/target/file');open(p,'w').write('remote edited after prepare')\n"); err != nil {
		t.Fatal(err, string(out))
	}
	pull = approve(a, pull)
	if r := call(a, pull); !r.OK || r.Mutation == nil || !r.Mutation.RemoteOK {
		t.Fatal("pull execute", r.Error)
	}
	if raw, err := os.ReadFile(filepath.Join(local, "file")); err != nil || string(raw) != payload {
		t.Fatal("pull lost retained bytes", err)
	}
	if _, err := os.Stat(filepath.Join(local, "obsolete")); !os.IsNotExist(err) {
		t.Fatal("pull deletion not applied")
	}
	if raw, err := os.ReadFile(filepath.Join(local, "excluded")); err != nil || string(raw) != "local retained" {
		t.Fatal("pull deletion lost excluded target", err)
	}
	// A destination edit after preparation invalidates the full snapshot before
	// any approved source changes/deletions can run.
	if err := os.WriteFile(filepath.Join(source, "new"), []byte("must not be written"), 0600); err != nil {
		t.Fatal(err)
	}
	drift := approve(a, prepare(a, opts))
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/target');open(p+'/concurrent','w').write('keep')\n"); err != nil {
		t.Fatal(err, string(out))
	}
	if r := call(a, drift); r.OK || r.Mutation == nil || r.Mutation.RemoteOK {
		t.Fatalf("target drift accepted: %+v", r.Mutation)
	}
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/target');assert not os.path.exists(p+'/new');assert open(p+'/concurrent').read()=='keep'\n"); err != nil {
		t.Fatal(err, string(out))
	}
	// Hold the actual remote terminal after files and its outcome are durable,
	// then kill the daemon before it can record success. Recovery only queries.
	crashReq := approve(a, prepare(a, client.SyncOptions{Direction: "push", Local: source + "/", Remote: "~/" + namespace + "/crash/"}))
	gateBytes, _ := json.Marshal(map[string]string{"operation_id": crashReq.OperationID})
	if err := os.WriteFile(gate, gateBytes, 0600); err != nil {
		t.Fatal(err)
	}
	pending := d.dial(a, d.token(a, "10m"), true)
	crashReq.Owner = a
	if err := pending.enc.Encode(crashReq); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 10*time.Second, "sync commit response held", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
	if r := call(a, broker.Request{Operation: "mutation.status", MutationID: crashReq.OperationID}); !r.OK || r.Mutation == nil || r.Mutation.State != "dispatched" {
		t.Fatal("pre-ACK sync intent missing", r.Error)
	}
	if ping() != base {
		t.Fatal("held sync commit blocked peer base agent")
	}
	// Restart forgets disposable plans/approvals, preserving owner-only outcomes.

	d.stop(syscall.SIGKILL)
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	d.start()
	connect()
	if r := call(a, broker.Request{Operation: "mutation.status", MutationID: req.OperationID}); !r.OK || r.Mutation == nil || !r.Mutation.RemoteOK {
		t.Fatal("restart lost outcome", r.Error)
	}
	if r := call(a, broker.Request{Operation: "mutation.status", MutationID: crashReq.OperationID}); !r.OK || r.Mutation == nil || r.Mutation.State != "completed" || !r.Mutation.RemoteOK {
		t.Fatalf("remote sync ledger recovery failed: %s %+v", r.Error, r.Mutation)
	}
	if r := call(b, broker.Request{Operation: "mutation.status", MutationID: crashReq.OperationID}); r.OK || r.Mutation != nil {
		t.Fatal("recovered sync outcome crossed owner")
	}
	if call(a, crashReq).OK {
		t.Fatal("crashed sync plan replayed")
	}
	// A newly reviewed plan cannot reuse an old operation identity either.
	reused := approve(a, prepare(a, client.SyncOptions{Direction: "push", Local: source + "/", Remote: "~/" + namespace + "/id-conflict/"}))
	reused.OperationID = crashReq.OperationID
	if call(a, reused).OK {
		t.Fatal("stable operation identity executed twice")
	}
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]);assert not os.path.exists(p+'/id-conflict');assert open(p+'/crash/file').read()=='edited after preparation'\n"); err != nil {
		t.Fatal(err, string(out))
	}
	if call(a, drift).OK {
		t.Fatal("restart revived a consumed plan")
	}
	cli := os.Getenv("RDEV_TEST_CLI_BINARY")
	if cli == "" {
		cli = filepath.Join(t.TempDir(), "rdev")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatal(err, string(out))
		}
	}
	command := func(args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+a.ClientID, "RDEV_PROJECT_ID="+a.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(a, "10m"))
		return cmd
	}
	// A real CLI prepares; a separate MCP process consumes the approved plan.
	target2 := "~/" + namespace + "/frontend/"
	out, err := command("sync", "runtime-host", "push", source+"/", target2, "-prepare").Output()
	if err != nil {
		t.Fatalf("CLI prepare: %v %s", err, out)
	}
	var prepared client.SyncResult
	if json.Unmarshal(out, &prepared) != nil || prepared.PlanID == "" {
		t.Fatal("CLI omitted prepared plan")
	}
	frontend := approve(a, broker.Request{Operation: "sync.push", Host: "runtime-host", Sync: &client.SyncOptions{Direction: "push", Local: source + "/", Remote: target2, PlanID: prepared.PlanID}})
	mc := mcp.NewClient(&mcp.Implementation{Name: "prepared-sync-proof", Version: "1"}, nil)
	ms, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command("serve")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()
	result, err := ms.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_sync", Arguments: map[string]any{"host": "runtime-host", "direction": "push", "local": source + "/", "remote": target2, "plan_id": prepared.PlanID, "approval_token": frontend.Approval, "operation_id": frontend.OperationID}})
	if err != nil || result == nil || result.IsError {
		t.Fatalf("MCP execute: %v %+v", err, result)
	}
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/frontend');assert open(p+'/file').read()=='edited after preparation'\n"); err != nil {
		t.Fatal(err, string(out))
	}
	// SIGKILL of an actual CLI releases only its request; the already executed
	// operation stays queryable after the held final is released.
	base = ping()
	cancelReq := approve(a, prepare(a, client.SyncOptions{Direction: "push", Local: source + "/", Remote: "~/" + namespace + "/canceled/"}))
	_ = os.Remove(gate + ".entered")
	gateBytes, _ = json.Marshal(map[string]string{"operation_id": cancelReq.OperationID})
	if err := os.WriteFile(gate, gateBytes, 0600); err != nil {
		t.Fatal(err)
	}
	frontendProcess := command("sync", "runtime-host", "push", source+"/", cancelReq.Sync.Remote, "-plan", cancelReq.Sync.PlanID)
	frontendProcess.Env = append(frontendProcess.Env, "RDEV_APPROVAL_TOKEN="+cancelReq.Approval, "RDEV_OPERATION_ID="+cancelReq.OperationID)
	if err := frontendProcess.Start(); err != nil {
		t.Fatal(err)
	}
	frontendDone := make(chan error, 1)
	go func() { frontendDone <- frontendProcess.Wait() }()
	t.Cleanup(func() { _ = frontendProcess.Process.Kill() })
	awaitRuntime(t, 10*time.Second, "CLI sync final held", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
	if err := frontendProcess.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-frontendDone:
	case <-time.After(5 * time.Second):
		t.Fatal("killed CLI did not exit")
	}
	awaitRuntime(t, 5*time.Second, "canceled sync released worker and reservations", func() bool {
		r := call(a, broker.Request{Operation: "status"})
		return r.OK && r.Scheduler != nil && r.Scheduler.Active == 0 && r.Ingress.ObservationBytes == 0
	})
	if ping() != base {
		t.Fatal("killed sync frontend replaced peer base agent")
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	if r := call(a, broker.Request{Operation: "mutation.status", MutationID: cancelReq.OperationID}); !r.OK || r.Mutation == nil || !r.Mutation.RemoteOK {
		t.Fatalf("canceled sync lost durable result: %s %+v", r.Error, r.Mutation)
	}
	status := call(a, broker.Request{Operation: "status"})
	if !status.OK || status.Scheduler == nil {
		t.Fatal("owner status missing")
	}
	audit := call(a, broker.Request{Operation: "audit_query"})
	raw, _ := json.Marshal(audit.Audit)
	if !audit.OK || strings.Contains(string(raw), source) || strings.Contains(string(raw), req.Approval) || strings.Contains(string(raw), "retained-original") {
		t.Fatal("sync audit leaked paths/content/token")
	}
	seen := false
	for _, e := range audit.Audit {
		if e.OperationRef == broker.OperationReference(req) && e.Result == "completed" && e.RequestDigest != "" && e.ApprovalID != "" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("sync approval/outcome audit correlation missing")
	}
	t.Log("actual daemon + CLI/MCP + SSH: retained push/pull, exact delete preserving excludes, source edits after preparation, target-drift rejection before writes, owner/host/options/plan substitution and replay denial, pre-ACK SIGKILL recovery from remote ledger without replay, durable owner-only outcomes, actual CLI SIGKILL releases worker/budget and preserves peer base PID, private approval/outcome audit")
}
