package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// This wraps real OpenSSH and holds only a selected terminal response. The
// remote command and agent are unchanged. It makes the execution-before-local-
// acknowledgement crash window deterministic without a daemon test hook.
const mutationSSHWrapper = `#!/usr/bin/env python3
import os,sys,json,subprocess,time
gate=os.environ['RDEV_MUTATION_GATE'];unavailable=gate+'.unavailable'
if os.path.exists(unavailable):sys.exit(255)
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'):args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
p=subprocess.Popen(args+sys.argv[1:],stdin=sys.stdin.buffer,stdout=subprocess.PIPE,stderr=sys.stderr.buffer)
client_closed=False
try:
 while True:
  line=p.stdout.readline()
  if not line:break
  try:
   response=json.loads(line)
   with open(gate) as f:expected=json.load(f)['operation_id']
   if response.get('terminal') and response.get('operation_id')==expected:
    info=(response.get('job') or {}).get('info') or {}
    proof={'operation_id':expected,'job_id':info.get('id',''),'pid':info.get('pid',0)}
    fd=os.open(gate+'.entered',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w') as f:json.dump(proof,f)
    while os.path.exists(gate):time.sleep(.01)
  except (ValueError,FileNotFoundError,KeyError):pass
  sys.stdout.buffer.write(line);sys.stdout.buffer.flush()
except BrokenPipeError:client_closed=True
finally:
 # stdout EOF can precede a successful ssh process exit. Killing at EOF
 # creates a false transport failure, especially after native mux fallback.
 if client_closed and p.poll() is None:p.terminate()
 try:p.wait(timeout=5)
 except subprocess.TimeoutExpired:
  p.terminate()
  try:p.wait(timeout=2)
  except subprocess.TimeoutExpired:p.kill();p.wait()
sys.exit(p.returncode)
`

func TestRemoteBrokerMutationCrashRecovery(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	cleanupRemoteJobSupervisors(t, sshRun)
	a := broker.Owner{ClientID: "mutation-shared", ProjectID: "phase5-lifecycle"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "other-project"}
	p := broker.NewPolicy()
	if err := p.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		if err := p.Grant(owner.Key(), "mutation.status"); err != nil {
			t.Fatal(err)
		}
		if err := p.Grant(owner.Key(), "audit_query"); err != nil {
			t.Fatal(err)
		}
		for _, op := range []string{proto.OpJobStart, proto.OpJobStatus, proto.OpJobList, proto.OpJobStop, proto.OpJobRm, proto.OpWriteFile} {
			if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(d.dir, "mutation-response-gate")
	t.Cleanup(func() { _ = os.Remove(gate); _ = os.Remove(gate + ".unavailable") })
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(mutationSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_MUTATION_GATE="+gate)
	d.start()
	wires := map[broker.Owner]*runtimeWire{}
	connect := func() {
		for _, owner := range []broker.Owner{a, b} {
			wires[owner] = d.dial(owner, d.token(owner, "5m"), true)
		}
	}
	connect()
	call := func(owner broker.Owner, wire *proto.Request) broker.Response {
		r := broker.Request{Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire}
		if broker.RequiresApproval(r) {
			r.Approval = d.approve(owner, wire)
		}
		return policyRuntimeRequest(t, wires[owner], r)
	}
	query := func(owner broker.Owner, id string) broker.Response {
		return policyRuntimeRequest(t, wires[owner], broker.Request{Owner: owner, Operation: "mutation.status", MutationID: id})
	}
	require := func(owner broker.Owner, wire *proto.Request) *proto.JobResult {
		r := call(owner, wire)
		if !r.OK || r.Wire == nil || !r.Wire.OK || r.Wire.Job == nil {
			t.Fatalf("job call failed: %s", r.Error)
		}
		return r.Wire.Job
	}
	hold := func(id string) {
		_ = os.Remove(gate + ".entered")
		data, _ := json.Marshal(map[string]string{"operation_id": id})
		if err := os.WriteFile(gate, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	awaitHeld := func(id string, processes ...*lifecycleProcess) {
		awaitRuntime(t, 10*time.Second, "remote mutation "+id+" final held before broker ACK", func() bool {
			for _, process := range processes {
				log, _ := os.ReadFile(process.logPath)
				if strings.Contains(string(log), "remote request failed:") {
					if len(log) > 8192 {
						log = log[:8192]
					}
					t.Fatalf("mutation %s failed before remote response barrier: %s", id, log)
				}
			}
			data, err := os.ReadFile(gate + ".entered")
			var got map[string]any
			return err == nil && json.Unmarshal(data, &got) == nil && got["operation_id"] == id
		})
	}
	crash := func(front *lifecycleProcess) {
		d.stop(syscall.SIGKILL)
		_ = os.Remove(gate)
		select {
		case <-front.done:
		case <-time.After(5 * time.Second):
			t.Fatal("crashed daemon left frontend blocked")
		}
	}
	jobOp := "op_runtime_preack_job_1"
	start := &proto.Request{Op: proto.OpJobStart, OperationID: jobOp, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf once >> "$1"; exec sleep 300`, "mutation-proof", namespace + "/start-proof"}}}}
	hold(jobOp)
	front := startLifecycleProcess(t, d, a, start, false)
	awaitHeld(jobOp, front)
	jobID, _ := proto.JobIDForOperation(proto.PrincipalID(a.ClientID, a.ProjectID), jobOp)
	if r := query(a, jobOp); !r.OK || r.Mutation == nil || r.Mutation.State != "dispatched" || r.Mutation.JobID != jobID {
		t.Fatal("pre-ACK durable intent missing")
	}
	if r := query(b, jobOp); r.OK || r.Mutation != nil {
		t.Fatal("other project read mutation intent")
	}
	// Pending ownership cannot be removed while the start can still complete.
	if r := call(a, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: jobID}}); r.OK {
		t.Fatal("in-flight start ownership removed")
	}
	if out, err := sshRun("import os,sys,time\np=os.path.expanduser('~/'+sys.argv[1]+'/start-proof')\nfor _ in range(100):\n if os.path.exists(p):break\n time.sleep(.01)\nassert open(p).read()=='once'\n"); err != nil {
		t.Fatalf("remote execution barrier: %v %s", err, out)
	}
	crash(front)
	if err := os.WriteFile(gate+".unavailable", nil, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	connect()
	if r := query(a, jobOp); !r.OK || r.Mutation.State != "ambiguous" {
		t.Fatal("unavailable restart forgot uncertain mutation")
	}
	if err := os.Remove(gate + ".unavailable"); err != nil {
		t.Fatal(err)
	}
	// Several independent clients can discover the same ambiguous start at
	// once. Hold the first real remote status response until all are submitted.
	recoverOp := "op_runtime_concurrent_status"
	hold(recoverOp)
	var observers []*lifecycleProcess
	pids := make(map[int]bool)
	for range 20 {
		observer := startLifecycleProcess(t, d, a, &proto.Request{Op: proto.OpJobStatus, OperationID: recoverOp, Job: &proto.JobParams{ID: jobID}}, false)
		observers = append(observers, observer)
		pids[observer.cmd.Process.Pid] = true
	}
	awaitHeld(recoverOp, observers...)
	if len(pids) != 20 {
		t.Fatal("recovery clients were not independent processes")
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	for _, observer := range observers {
		result := observer.result(t)
		if result.Job == nil || result.Job.Info == nil || result.Job.Info.ID != jobID || result.Job.Info.StartOperationID != jobOp {
			t.Fatal("concurrent recovery returned another job")
		}
	}
	info := require(a, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: jobID}}).Info
	if info == nil || info.ID != jobID || info.StartOperationID != jobOp || info.State != proto.JobRunning {
		t.Fatal("unacknowledged job not rediscovered")
	}
	if r := query(a, jobOp); !r.OK || r.Mutation.State != "completed" {
		t.Fatal("matching remote identity did not resolve mutation")
	}
	if r := call(a, start); r.OK || r.Mutation == nil || r.Mutation.State != "completed" {
		t.Fatal("recorded start was replayed")
	}
	changed := *start
	params := *start.Job
	params.Spec = &proto.ExecParams{Argv: []string{"false"}}
	changed.Job = &params
	if r := call(a, &changed); r.OK || !strings.Contains(r.Error, "conflict") {
		t.Fatal("same mutation ID changed command")
	}
	// A fresh remote agent must also use durable identity after losing cache.
	remoteDurableReplay(t, sshRun, start, a, jobID, true)
	require(a, &proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: jobID, Signal: "TERM", GraceSec: 1}})
	require(a, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: jobID}})
	remoteDurableReplay(t, sshRun, start, a, jobID, false)
	// Generic mutations also keep a durable ambiguous outcome and reject replay.
	writeOp := "op_runtime_preack_append_1"
	write := &proto.Request{Op: proto.OpWriteFile, OperationID: writeOp, Cat: &proto.WriteParams{Path: namespace + "/append-proof", Content: "once", Append: true}}
	hold(writeOp)
	front = startLifecycleProcess(t, d, a, write, false)
	awaitHeld(writeOp, front)
	crash(front)
	d.start()
	connect()
	if r := query(a, writeOp); !r.OK || r.Mutation == nil || r.Mutation.State != "ambiguous" {
		t.Fatal("generic mutation uncertainty lost")
	}
	if r := call(a, write); r.OK || r.Mutation == nil || r.Mutation.State != "ambiguous" {
		t.Fatal("uncertain append replayed after crash")
	}
	// A real pre-dispatch rename failure must never reach the remote command.
	path := d.socket + ".mutations"
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	failed := &proto.Request{Op: proto.OpWriteFile, OperationID: "op_runtime_intent_disk_fail", Cat: &proto.WriteParams{Path: namespace + "/must-not-exist", Content: "forbidden"}}
	if r := call(a, failed); r.OK {
		t.Fatal("intent write failure acknowledged mutation")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "forbidden") || strings.Contains(string(data), "printf once") || strings.Contains(string(data), "exec sleep") {
		t.Fatal("request body leaked into intent registry")
	}
	if out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nassert open(p+'/start-proof').read()=='once'\nassert open(p+'/append-proof').read()=='once'\nassert not os.path.exists(p+'/must-not-exist')\n"); err != nil {
		t.Fatalf("mutation replay/failure proof: %v %s", err, out)
	}
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	if r := query(a, jobOp); !r.OK || r.Mutation.State != "completed" {
		t.Fatal("completed start intent lost after job removal/restart")
	}
	if list := require(a, &proto.Request{Op: proto.OpJobList, Job: &proto.JobParams{}}); len(list.List) != 0 {
		t.Fatal("completed intent resurrected removed job")
	}
	// A real terminal rejection before remote admission permits removal of the
	// local ownership reservation. It must not become permanently ambiguous.
	rejected := &proto.Request{Op: proto.OpJobStart, OperationID: "op_runtime_rejected_start", StreamWindowBytes: proto.AbsoluteStreamWindowBytes + 1, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"true"}}}}
	r := call(a, rejected)
	if r.OK || r.Mutation == nil || r.Mutation.State != "not_sent" {
		t.Fatalf("definitive remote rejection unresolved: %s", r.Error)
	}
	rejectedID := r.Mutation.JobID
	if r := call(b, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: rejectedID}}); r.OK {
		t.Fatal("other owner removed rejected start reservation")
	}
	require(a, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: rejectedID}})
	if r := call(a, rejected); r.OK || r.Mutation == nil || r.Mutation.State != "not_sent" {
		t.Fatal("removed rejection identity dispatched again")
	}
	failedStart := &proto.Request{Op: proto.OpJobStart, OperationID: "op_runtime_failed_start", Job: &proto.JobParams{Spec: &proto.ExecParams{}}}
	r = call(a, failedStart)
	if r.OK || r.Mutation == nil || r.Mutation.State != "completed" || r.Mutation.RemoteOK {
		t.Fatalf("terminal remote failure unresolved: %s", r.Error)
	}
	require(a, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: r.Mutation.JobID}})
	// SIGTERM must bound a transport stalled after mutation execution and
	// retain the outcome across restart, just like the SIGKILL window above.
	shutdownWrite := &proto.Request{Op: proto.OpWriteFile, OperationID: "op_runtime_shutdown_append", Cat: &proto.WriteParams{Path: namespace + "/shutdown-proof", Content: "once", Append: true}}
	hold(shutdownWrite.OperationID)
	front = startLifecycleProcess(t, d, a, shutdownWrite, false)
	awaitHeld(shutdownWrite.OperationID, front)
	shutdownAt := time.Now()
	d.stop(syscall.SIGTERM)
	shutdownElapsed := time.Since(shutdownAt)
	_ = os.Remove(gate)
	select {
	case <-front.done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown left mutation frontend blocked")
	}
	d.start()
	connect()
	if r := query(a, shutdownWrite.OperationID); !r.OK || r.Mutation == nil || r.Mutation.State != "ambiguous" {
		t.Fatal("shutdown forgot unacknowledged mutation")
	}
	if r := call(a, shutdownWrite); r.OK || r.Mutation == nil || r.Mutation.State != "ambiguous" {
		t.Fatal("shutdown mutation replayed")
	}
	verifyMutationFrontends(t, d, a, b, namespace, jobOp)
	for _, owner := range []broker.Owner{a, b} {
		audit := policyRuntimeRequest(t, wires[owner], broker.Request{Owner: owner, Operation: "audit_query"})
		// Earlier SIGKILL windows make the former asynchronous tail uncertain.
		// Retained completed-operation correlation must still be exact below.
		if !audit.OK || !audit.AuditIncomplete {
			t.Fatal("mutation audit unavailable or crash-tail uncertainty hidden")
		}
		found := false
		ref := broker.OperationReference(broker.Request{Wire: &proto.Request{OperationID: "op_runtime_mcp_append"}})
		for _, event := range audit.Audit {
			if event.Owner != broker.AuditOwnerID(owner.Key()) {
				t.Fatal("mutation audit crossed owners")
			}
			found = found || event.OperationRef == ref && event.Result == "completed"
		}
		if found != (owner == a) {
			t.Fatal("operation reference missing or exposed to another project")
		}
	}
	auditData, err := os.ReadFile(d.socket + ".audit")
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{jobOp, writeOp, "op_runtime_mcp_append", "printf once", "exec sleep", "forbidden"} {
		if strings.Contains(string(auditData), raw) {
			t.Fatal("raw mutation ID or payload leaked into audit")
		}
	}
	if out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nassert open(p+'/cli-proof').read()=='once'\nassert open(p+'/mcp-proof').read()=='once'\nassert open(p+'/shutdown-proof').read()=='once'\n"); err != nil {
		t.Fatalf("frontend duplicate protection: %v %s", err, out)
	}
	t.Logf("real pre-ACK crash recovery: job command once; pending job ownership retained through SSH-unavailable restart; recovered supervisor=%d; original owner resolves durable start; other project denied; stable ID substitution/replay denied; new remote agent replay recovers metadata and rejects deleted tombstone; append once after crash; pre-dispatch intent rename failure sends no mutation; no payloads in intent snapshot", info.PID)
	t.Log("real CLI and MCP processes: explicit operation IDs, owner-scoped outcome queries and duplicate append prevention passed; definitive remote rejection reservation cleaned by original owner only")
	t.Logf("SIGTERM with remote mutation response held: bounded shutdown=%s; restart preserves ambiguity and refuses duplicate append", shutdownElapsed)
	t.Log("20 independent status clients concurrently resolved the same durable job start without spurious transition failures")
}

func verifyMutationFrontends(t *testing.T, d *runtimeDaemon, owner, other broker.Owner, namespace, recoveredID string) {
	t.Helper()
	cli := filepath.Join(d.dir, "rdev")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build frontend: %v %s", err, out)
	}
	command := func(principal broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+principal.ClientID, "RDEV_PROJECT_ID="+principal.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(principal, "5m"))
		return cmd
	}
	for _, principal := range []broker.Owner{owner, other} {
		out, err := command(principal, "mutation", "status", recoveredID).Output()
		var got broker.MutationIntent
		if principal == owner {
			if err != nil || json.Unmarshal(out, &got) != nil || got.OperationID != recoveredID || got.State != "completed" {
				t.Fatal("CLI could not query recovered mutation")
			}
		} else if err == nil || len(out) != 0 {
			t.Fatal("CLI exposed other project's mutation")
		}
	}
	cliWrite := &proto.Request{Op: proto.OpWriteFile, OperationID: "op_runtime_cli_append", Cat: &proto.WriteParams{Path: namespace + "/cli-proof", Content: "once", Append: true}}
	for attempt := 0; attempt < 2; attempt++ {
		cmd := command(owner, "write", "runtime-host", cliWrite.Cat.Path, "-append")
		cmd.Stdin = strings.NewReader(cliWrite.Cat.Content)
		cmd.Env = append(cmd.Env, "RDEV_OPERATION_ID="+cliWrite.OperationID, "RDEV_APPROVAL_TOKEN="+d.approve(owner, cliWrite))
		out, err := cmd.CombinedOutput()
		if attempt == 0 && err != nil || attempt == 1 && (err == nil || !strings.Contains(string(out), cliWrite.OperationID)) {
			// Only this test's synthetic write output is included; never emit cmd.Env,
			// which carries the principal and approval credentials.
			if len(out) > 4096 {
				out = out[:4096]
			}
			t.Fatalf("CLI stable-ID attempt %d: %v; output: %s", attempt, err, out)
		}
	}
	for _, principal := range []broker.Owner{owner, other} {
		client := mcp.NewClient(&mcp.Implementation{Name: "phase5-runtime", Version: "1"}, nil)
		session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: command(principal, "serve")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		status, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_mutation_status", Arguments: map[string]any{"operation_id": recoveredID}})
		if err != nil || status == nil || status.IsError != (principal == other) {
			t.Fatal("MCP mutation query owner isolation failed")
		}
		if principal == other {
			continue
		}
		data, _ := json.Marshal(status.StructuredContent)
		var got broker.MutationIntent
		if json.Unmarshal(data, &got) != nil || got.OperationID != recoveredID || got.State != "completed" {
			t.Fatal("MCP recovered mutation projection failed")
		}
		wire := &proto.Request{Op: proto.OpWriteFile, OperationID: "op_runtime_mcp_append", Cat: &proto.WriteParams{Path: namespace + "/mcp-proof", Content: "once", Append: true}}
		for attempt := 0; attempt < 2; attempt++ {
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_write", Arguments: map[string]any{"host": "runtime-host", "path": wire.Cat.Path, "content": "once", "append": true, "operation_id": wire.OperationID, "approval_token": d.approve(owner, wire)}})
			if err != nil || result == nil || result.IsError != (attempt == 1) {
				t.Fatalf("MCP stable-ID attempt %d failed", attempt)
			}
		}
		status, err = session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_mutation_status", Arguments: map[string]any{"operation_id": wire.OperationID}})
		if err != nil || status == nil || status.IsError {
			t.Fatal("MCP supplied operation ID was not retained")
		}
	}
}

func remoteDurableReplay(t *testing.T, sshRun func(string) ([]byte, error), request *proto.Request, owner broker.Owner, id string, wantFound bool) {
	t.Helper()
	wire := *request
	params := *request.Job
	params.DurableStart = true
	wire.Job = &params
	wire.ClientID = proto.PrincipalID(owner.ClientID, owner.ProjectID)
	wire.ProjectID = owner.ProjectID
	wire.Replay = true
	wire.ID = "durable-replay"
	// Replay the effective envelope the broker sent, including bounded defaults.
	// An omitted resource envelope is a different durable operation digest.
	normalized, err := proto.NormalizeTimeouts(&wire)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(normalized)
	script := fmt.Sprintf(`import os,sys,json,subprocess,base64
root=os.path.expanduser('~/'+sys.argv[1])
p=subprocess.Popen([root+'/rdev-agent','-state',root],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
def send(request):
 p.stdin.write(json.dumps(request)+'\n');p.stdin.flush()
 while True:
  line=p.stdout.readline();assert line,'agent closed'
  result=json.loads(line)
  if result.get('terminal') or request['id']=='hello':return result
try:
 hello=send({'id':'hello','op':'ping','hello':{'min_version':3,'max_version':3,'features':['operation_id','deduplication','durable_job_start','job_resource_envelope']}})
 assert hello.get('ok')
 request=json.loads(base64.b64decode('%s'))
 result=send(request)
 if %s:assert result.get('ok') and result.get('job',{}).get('info',{}).get('id')=='%s', ('durable replay rejected',result.get('error',{}).get('code'),result.get('execution_state'))
 else:assert not result.get('ok') and result.get('error',{}).get('code')=='transport.ambiguous_outcome'
finally:
 p.stdin.close()
 try:p.wait(timeout=5)
 except subprocess.TimeoutExpired:p.kill();p.wait()
`, base64.StdEncoding.EncodeToString(data), map[bool]string{true: "True", false: "False"}[wantFound], id)
	if out, err := sshRun(script); err != nil {
		t.Fatalf("remote durable replay: %v %s", err, out)
	}
}

// Pipe EOF and process exit are separate events. The injection wrapper must
// preserve the real child's outcome instead of manufacturing a transport fault.
func TestMutationWrapperWaitsForProcessAfterStdoutEOF(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("Python required by the SSH injection wrapper")
	}
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			dir := t.TempDir()
			wrapper := filepath.Join(dir, "wrapper")
			child := filepath.Join(dir, "ssh-child")
			if err := os.WriteFile(wrapper, []byte(mutationSSHWrapper), 0700); err != nil {
				t.Fatal(err)
			}
			script := `#!/usr/bin/env python3
import os,sys,time
os.write(1,b'probe-output\n')
os.close(1)
time.sleep(.2)
sys.exit(int(os.environ['RDEV_WRAPPER_EXIT']))
`
			if err := os.WriteFile(child, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, wrapper)
			cmd.Env = append(os.Environ(), "RDEV_MUTATION_GATE="+filepath.Join(dir, "unused-gate"), "RDEV_TEST_REAL_SSH="+child, "RDEV_TEST_SSH_CONFIG=", "RDEV_WRAPPER_EXIT="+fmt.Sprint(code))
			out, err := cmd.CombinedOutput()
			if string(out) != "probe-output\n" {
				t.Fatalf("wrapper altered stdout: %q", out)
			}
			got := 0
			if err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				got = exit.ExitCode()
			}
			if got != code {
				t.Fatalf("stdout EOF replaced real exit code: got %d, want %d", got, code)
			}
		})
	}
}
