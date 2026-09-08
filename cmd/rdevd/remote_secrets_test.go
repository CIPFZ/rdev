package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestRemoteBrokerSecrets(t *testing.T) {
	d, namespace, ssh := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "secret-same-client", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	noUse := broker.Owner{ClientID: a.ClientID, ProjectID: "no-use"}
	admin := runtimeApprovalAdmin()
	p := broker.NewPolicy()
	for _, op := range []string{"approval.create", "policy.grant"} {
		if err := p.Grant(admin.Key(), op); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range []broker.Owner{a, b, noUse} {
		for _, op := range []string{"secret.set", "secret.delete", "secret.list", proto.OpExec, proto.OpJobStart, proto.OpJobStatus, proto.OpJobWait, proto.OpJobLogs, proto.OpJobRm, "mutation.status"} {
			if err := p.GrantHost(o.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
		if o != noUse {
			if err := p.GrantHost(o.Key(), "runtime-host", "secret", "secret.use"); err != nil {
				t.Fatal(err)
			}
		}
		for _, op := range []string{"audit_query", "status"} {
			if err := p.Grant(o.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	wires := map[broker.Owner]*runtimeWire{}
	connect := func() {
		for _, o := range []broker.Owner{a, b, noUse, admin} {
			wires[o] = d.dial(o, d.token(o, "10m"), true)
		}
	}
	connect()
	call := func(o broker.Owner, req broker.Request) broker.Response {
		req.Owner = o
		return policyRuntimeRequest(t, wires[o], req)
	}
	issue := func(o broker.Owner, op string, secret *broker.SecretParams, wire *proto.Request) broker.Response {
		return call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: o, Operation: op, Host: "runtime-host", Secret: secret, Wire: wire, TTL: 5 * time.Minute}})
	}
	token := func(o broker.Owner, op string, secret *broker.SecretParams, wire *proto.Request) string {
		t.Helper()
		r := issue(o, op, secret, wire)
		if !r.OK || r.Approval == nil {
			t.Fatal("exact approval issuance failed")
		}
		return r.Approval.Token
	}
	mutate := func(o broker.Owner, op string, secret *broker.SecretParams) broker.Response {
		t.Helper()
		id, _ := proto.NewOperationID()
		r := call(o, broker.Request{Operation: op, Host: "runtime-host", Secret: secret, OperationID: id, Approval: token(o, op, secret, nil)})
		if !r.OK || r.Mutation == nil || r.Mutation.State != "completed" {
			t.Fatal("secret mutation failed")
		}
		return r
	}
	list := func(o broker.Owner) []broker.SecretDescriptor {
		t.Helper()
		r := call(o, broker.Request{Operation: "secret.list", Host: "runtime-host", Secret: &broker.SecretParams{}})
		if !r.OK {
			t.Fatal("secret list failed")
		}
		return r.Secrets
	}
	const alpha = "alpha-credential-\"quoted\"-雪\nline"
	const beta = "beta-distinct-credential"
	const rotated = "alpha-rotated-credential"
	// Exercise the actual CLI and MCP processes for registration and listing.
	cli := filepath.Join(t.TempDir(), "rdev")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v %s", err, out)
	}
	command := func(o broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+o.ClientID, "RDEV_PROJECT_ID="+o.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(o, "10m"))
		return cmd
	}
	secretA := &broker.SecretParams{Name: "token", Value: alpha}
	idA, _ := proto.NewOperationID()
	set := command(a, "secret", "set", "runtime-host", "token")
	set.Stdin = strings.NewReader(alpha)
	set.Env = append(set.Env, "RDEV_APPROVAL_TOKEN="+token(a, "secret.set", secretA, nil), "RDEV_OPERATION_ID="+idA)
	if out, err := set.Output(); err != nil || strings.Contains(string(out), alpha) {
		t.Fatal("CLI secret registration failed or returned plaintext")
	}
	mc := mcp.NewClient(&mcp.Implementation{Name: "secret-proof", Version: "1"}, nil)
	session, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command(b, "serve")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	secretB := &broker.SecretParams{Name: "token", Value: beta}
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_secrets", Arguments: map[string]any{"action": "set", "host": "runtime-host", "name": "token", "value": beta, "approval_token": token(b, "secret.set", secretB, nil)}})
	if err != nil || result == nil || result.IsError {
		t.Fatal("MCP secret registration failed")
	}
	if got := list(noUse); len(got) != 0 {
		t.Fatal("new project inherited another project's secret")
	}
	mutate(noUse, "secret.set", &broker.SecretParams{Name: "token", Value: "unusable-without-grant"})
	if len(list(a)) != 1 || len(list(b)) != 1 || list(a)[0].Version == list(b)[0].Version {
		t.Fatal("same-name credentials not independently owned")
	}
	if r := call(a, broker.Request{Operation: "secret.list", Host: "other-host", Secret: &broker.SecretParams{}}); r.OK {
		t.Fatal("host grant widened secret list")
	}
	// Knowing another project's name cannot inject it or consume its approval.
	execWire := func() *proto.Request {
		return &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"python3", "-c", "import os,hashlib;v=os.environ['TOKEN'];print(hashlib.sha256(v.encode()).hexdigest());print(v)"}, Env: map[string]string{"TOKEN": "secret:token"}}}
	}
	if r := issue(noUse, proto.OpExec, nil, execWire()); r.OK {
		t.Fatal("exec authority implicitly granted secret use")
	}
	verifyExec := func(o broker.Owner, value string) {
		t.Helper()
		w := execWire()
		r := call(o, broker.Request{Operation: w.Op, Host: "runtime-host", Wire: w, Approval: token(o, w.Op, nil, w)})
		sum := sha256.Sum256([]byte(value))
		if !r.OK || r.Wire == nil || r.Wire.Exec == nil || !strings.HasPrefix(r.Wire.Exec.Stdout, hex.EncodeToString(sum[:])) || strings.Contains(r.Wire.Exec.Stdout, value) || !strings.Contains(r.Wire.Exec.Stdout, "<redacted:broker_") {
			t.Fatal("real SSH injection or output redaction failed")
		}
	}
	verifyExec(a, alpha)
	verifyExec(b, beta)

	// Expanded credentials consume ingress bytes; cancellation releases the charge
	// while another project continues real SSH work on the shared transport.
	large := strings.Repeat("v", 65536)
	mutate(a, "secret.set", &broker.SecretParams{Name: "token", Value: large})
	heldWire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"python3", "-c", "import os,time;open(os.path.expanduser('~/" + namespace + "/secret-exec-started'),'w').close();time.sleep(30)"}, Env: map[string]string{"TOKEN": "secret:token"}}}
	held := d.dial(a, d.token(a, "5m"), true)
	if err := held.enc.Encode(broker.Request{Owner: a, Operation: heldWire.Op, Host: "runtime-host", Wire: heldWire, Approval: token(a, heldWire.Op, nil, heldWire)}); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "large secret exec started", func() bool {
		out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/secret-exec-started');print(os.path.exists(p))\n")
		return err == nil && strings.TrimSpace(string(out)) == "True"
	})
	charged := call(a, broker.Request{Operation: "status"})
	if !charged.OK || charged.Ingress == nil || charged.Ingress.Bytes < 65536 {
		t.Fatal("expanded secret payload bypassed ingress accounting")
	}
	verifyExec(b, beta)
	held.Close()
	awaitRuntime(t, 5*time.Second, "secret exec cancellation releases expanded bytes", func() bool {
		r := call(a, broker.Request{Operation: "status"})
		return r.OK && r.Ingress != nil && r.Ingress.Bytes < 4096 && r.Scheduler.Active == 0
	})
	mutate(a, "secret.set", secretA)
	staleWire := execWire()
	staleToken := token(a, proto.OpExec, nil, staleWire)
	// Start a detached job with the old value, then rotate/delete before allowing
	// output. The remote supervisor and its redaction must survive daemon SIGKILL.
	jobScript := "import os,time;d=os.path.expanduser('~/" + namespace + "');open(d+'/secret-job-started','w').close();\nwhile not os.path.exists(d+'/secret-job-release'):time.sleep(.02)\nprint(os.environ['TOKEN'])"
	job := &proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"python3", "-c", jobScript}, Env: map[string]string{"TOKEN": "secret:token"}}}}
	jr := call(a, broker.Request{Operation: job.Op, Host: "runtime-host", Wire: job, Approval: token(a, job.Op, nil, job)})
	if !jr.OK || jr.Wire == nil || jr.Wire.Job == nil || jr.Wire.Job.Info == nil {
		t.Fatal("secret-bearing job start failed")
	}
	jobID := jr.Wire.Job.Info.ID
	pid := jr.Wire.Job.Info.PID
	awaitRuntime(t, 5*time.Second, "secret-bearing detached job started", func() bool {
		out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/secret-job-started');print(os.path.exists(p))\n")
		return err == nil && strings.TrimSpace(string(out)) == "True"
	})
	mutate(a, "secret.set", &broker.SecretParams{Name: "token", Value: rotated})
	if r := call(a, broker.Request{Operation: proto.OpExec, Host: "runtime-host", Wire: staleWire, Approval: staleToken}); r.OK {
		t.Fatal("approval survived credential rotation")
	}
	verifyExec(a, rotated)
	verifyExec(b, beta)
	mutate(a, "secret.delete", &broker.SecretParams{Name: "token"})
	if len(list(a)) != 0 || len(list(b)) != 1 {
		t.Fatal("delete crossed owner")
	}
	if r := issue(a, proto.OpExec, nil, execWire()); r.OK {
		t.Fatal("deleted secret fell back to another principal")
	}
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	if len(list(a)) != 0 || len(list(b)) != 1 {
		t.Fatal("crash lost secret lifecycle state")
	}
	// Reuse of the original ID must not reactivate the deleted credential.
	replay := call(a, broker.Request{Operation: "secret.set", Host: "runtime-host", Secret: secretA, OperationID: idA, Approval: token(a, "secret.set", secretA, nil)})
	if replay.OK || replay.Mutation == nil || len(list(a)) != 0 {
		t.Fatal("original mutation ID re-registered a retired credential")
	}
	if out, err := ssh("import os,sys\nopen(os.path.expanduser('~/'+sys.argv[1]+'/secret-job-release'),'w').close()\n"); err != nil {
		t.Fatalf("release remote job: %v %s", err, out)
	}
	wait := &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobID, WaitTimeoutSec: 8, TailOnExit: 10}}
	waited := call(a, broker.Request{Operation: wait.Op, Host: "runtime-host", Wire: wait})
	raw, _ := json.Marshal(waited)
	if !waited.OK || waited.Wire == nil || !waited.Wire.OK || strings.Contains(string(raw), "alpha-credential") || !strings.Contains(string(raw), "redacted:broker_") {
		t.Fatal("old detached output lost protection after delete/crash")
	}
	status := call(a, broker.Request{Operation: proto.OpJobStatus, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: jobID}}})
	if !status.OK || status.Wire == nil || status.Wire.Job == nil || status.Wire.Job.Info.PID != pid {
		t.Fatal("crash replaced original supervisor")
	}
	if r := call(b, broker.Request{Operation: proto.OpJobLogs, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobLogs, Job: &proto.JobParams{ID: jobID}}}); r.OK {
		t.Fatal("secret-bearing job visible to another project")
	}
	// Actual storage obstruction must fail closed and preserve the prior durable
	// version. Restore only the test's own path, then recover in a fresh daemon.
	replacement := &broker.SecretParams{Name: "token", Value: "must-not-be-acknowledged"}
	approval := token(b, "secret.set", replacement, nil)
	statePath := d.socket + ".secrets"
	if err := os.Rename(statePath, statePath+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(statePath, 0700); err != nil {
		t.Fatal(err)
	}
	failed := call(b, broker.Request{Operation: "secret.set", Host: "runtime-host", Secret: replacement, Approval: approval})
	if failed.OK || failed.Mutation == nil || failed.Mutation.State != "ambiguous" {
		t.Fatal("secret storage failure acknowledged")
	}
	if r := call(b, broker.Request{Operation: "secret.list", Host: "runtime-host", Secret: &broker.SecretParams{}}); r.OK {
		t.Fatal("failed secret store still served state")
	}
	if err := os.Remove(statePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(statePath+".saved", statePath); err != nil {
		t.Fatal(err)
	}
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	verifyExec(b, beta)
	for _, o := range []broker.Owner{a, b, noUse} {
		r := call(o, broker.Request{Operation: "audit_query"})
		if !r.OK {
			t.Fatal("audit query failed")
		}
	}
	for _, suffix := range []string{".audit", ".mutations", ".events"} {
		data, err := os.ReadFile(d.socket + suffix)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{alpha, beta, rotated, replacement.Value} {
			if strings.Contains(string(data), secret) {
				t.Fatal("credential entered low-sensitivity durable metadata")
			}
		}
	}
	info, err := os.Stat(statePath)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("secret state permissions incorrect")
	}
	out, err := command(b, "secret", "list", "runtime-host").Output()
	if err != nil || strings.Contains(string(out), beta) {
		t.Fatal("CLI list failed or returned credential")
	}
	// Clean up the completed supervised record with its exact original owner.
	rm := &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: jobID}}
	if r := call(a, broker.Request{Operation: rm.Op, Host: "runtime-host", Wire: rm, Approval: token(a, rm.Op, nil, rm)}); !r.OK {
		t.Fatal("owned job cleanup failed")
	}

	// Missing credential state must block READY once a durable secret mutation
	// exists; otherwise startup could return historical job output unredacted.
	d.stop(syscall.SIGTERM)
	if err := os.Rename(statePath, statePath+".saved"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	missing := exec.CommandContext(ctx, d.bin, d.args()...)
	missing.Dir = d.dir
	missing.Env = d.env
	if err := missing.Run(); err == nil || ctx.Err() != nil {
		cancel()
		t.Fatal("missing prior secret state did not fail startup promptly")
	}
	cancel()
	if _, err := os.Stat(d.ready); !os.IsNotExist(err) {
		t.Fatal("missing secret state published READY")
	}
	if err := os.Rename(statePath+".saved", statePath); err != nil {
		t.Fatal(err)
	}
	// Changing the configured target retires injection authority durably. A
	// subsequent restart with the original definition cannot reactivate it.
	hostPath := filepath.Join(d.dir, "hosts.json")
	hosts, err := os.ReadFile(hostPath)
	if err != nil {
		t.Fatal(err)
	}
	var changed map[string]any
	if json.Unmarshal(hosts, &changed) != nil {
		t.Fatal("host fixture decode")
	}
	entries := changed["hosts"].([]any)
	entries[0].(map[string]any)["login_shell"] = true
	modified, _ := json.Marshal(changed)
	if err := os.WriteFile(hostPath, modified, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	connect()
	if len(list(b)) != 0 {
		t.Fatal("changed host inherited prior credential")
	}
	d.stop(syscall.SIGTERM)
	if err := os.WriteFile(hostPath, hosts, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	connect()
	if len(list(b)) != 0 {
		t.Fatal("returning host reactivated retired credential")
	}
	t.Log("actual daemon/CLI/MCP/SSH: same-name exact-project credentials; explicit secret.use; host/default denial; exact value/version approvals; rotation and deletion isolation; detached old-value output redacted after SIGKILL with original supervisor; repeated mutation ID cannot reactivate; private durable state, missing-state startup denial, host replacement/return retirement, real rename failure and restart recovery; low-sensitivity audit/mutation/event files exclude credentials")
}
