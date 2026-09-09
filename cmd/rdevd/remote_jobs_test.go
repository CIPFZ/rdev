package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

// Real acknowledged jobs survive broker SIGKILL, unavailable SSH on startup,
// SIGHUP and deletion-persistence failure. The separate pre-acknowledgement
// job-start intent/replay window is deliberately not claimed by this test.
func TestRemoteBrokerJobRecovery(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "jobs-shared-client", ProjectID: "phase5-lifecycle"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "jobs-other-project"}
	p := broker.NewPolicy()
	if err := p.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{proto.OpJobStart, proto.OpJobList, proto.OpJobStatus, proto.OpJobLogs, proto.OpJobStop, proto.OpJobWait, proto.OpJobRm} {
			if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(d.dir, "ssh-unavailable")
	wrapper := "#!/bin/sh\nif [ -f \"$RDEV_JOB_SSH_GATE\" ]; then exit 255; fi\nif [ -n \"$RDEV_TEST_SSH_CONFIG\" ]; then exec \"$RDEV_TEST_REAL_SSH\" -F \"$RDEV_TEST_SSH_CONFIG\" \"$@\"; fi\nexec \"$RDEV_TEST_REAL_SSH\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_JOB_SSH_GATE="+gate)
	t.Cleanup(func() { _ = os.Remove(gate) })
	cleanupRemoteJobSupervisors(t, sshRun)
	d.start()
	wires := map[broker.Owner]*runtimeWire{}
	connect := func() {
		for _, owner := range []broker.Owner{a, b} {
			wires[owner] = d.dial(owner, d.token(owner, "5m"), true)
		}
	}
	connect()
	call := func(owner broker.Owner, wire *proto.Request) broker.Response {
		req := broker.Request{Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire}
		if broker.RequiresApproval(req) {
			req.Approval = d.approve(owner, wire)
		}
		return policyRuntimeRequest(t, wires[owner], req)
	}
	require := func(owner broker.Owner, wire *proto.Request) *proto.JobResult {
		response := call(owner, wire)
		if !response.OK || response.Wire == nil || !response.Wire.OK || response.Wire.Job == nil {
			t.Fatalf("job %s failed: %s", wire.Op, response.Error)
		}
		return response.Wire.Job
	}
	startSpec := func(name string) *proto.Request {
		return &proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf once >> "$1"; exec sleep 120`, "job-proof", namespace + "/proof-" + name}}}}
	}
	first := startLifecycleProcess(t, d, a, startSpec("a"), false).result(t)
	if first.Job == nil || first.Job.Info == nil {
		t.Fatal("separate frontend did not create a detached job")
	}
	infoA := first.Job.Info
	infoB := require(b, startSpec("b")).Info
	if infoB == nil || infoB.ID == infoA.ID {
		t.Fatal("distinct job identity missing")
	}
	ids := map[broker.Owner]string{a: infoA.ID, b: infoB.ID}
	assertOwned := func() {
		for _, owner := range []broker.Owner{a, b} {
			list := require(owner, &proto.Request{Op: proto.OpJobList, Job: &proto.JobParams{Limit: 1}})
			if len(list.List) != 1 || list.List[0].ID != ids[owner] || list.Total != 1 || list.Truncated {
				t.Fatalf("owner list/page leaked or hid jobs: count=%d total=%d truncated=%v", len(list.List), list.Total, list.Truncated)
			}
			status := require(owner, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: ids[owner]}}).Info
			if status == nil || status.State != proto.JobRunning {
				t.Fatal("detached job not running")
			}
		}
	}
	assertOwned()
	if list := require(a, &proto.Request{Op: proto.OpJobList}); len(list.List) != 1 || list.List[0].ID != infoA.ID || list.Total != 1 {
		t.Fatal("omitted list parameters escaped owner scope")
	}
	for _, op := range []string{proto.OpJobStatus, proto.OpJobLogs, proto.OpJobWait, proto.OpJobStop, proto.OpJobRm} {
		if response := call(b, &proto.Request{Op: op, Job: &proto.JobParams{ID: infoA.ID}}); response.OK {
			t.Fatalf("granted other project accessed job using %s", op)
		}
	}
	// The ownership ACK must survive immediate broker SIGKILL. Re-probing while
	// its SSH executable refuses every connection must not erase either owner.
	d.stop(syscall.SIGKILL)
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	connect()
	disk := broker.NewJobRegistry()
	if err := disk.Load(d.socket + ".jobs"); err != nil {
		t.Fatal(err)
	}
	for owner, id := range ids {
		if ref, ok := disk.GetHost("runtime-host", id); !ok || ref.Owner != owner.Key() {
			t.Fatal("unavailable SSH recovery erased persisted ownership")
		}
	}
	if response := call(a, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: infoA.ID}}); response.OK && response.Wire != nil && response.Wire.OK {
		t.Fatal("SSH failure injection did not reach runtime")
	}
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	assertOwned()
	if status := require(a, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: infoA.ID}}).Info; status.PID != infoA.PID {
		t.Fatal("broker crash replaced the detached supervisor")
	}
	if err := d.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitDaemonLog(t, d, "configuration reloaded")
	assertOwned()
	// A real rename failure after remote job_rm must not publish deletion in the
	// active ownership map, nor remove another owner's reference on shutdown.
	require(a, &proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: infoA.ID, Signal: "TERM", GraceSec: 1}})
	path := d.socket + ".jobs"
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	const removalOperationID = "op_job_recovery_delete_failure"
	response := call(a, &proto.Request{Op: proto.OpJobRm, OperationID: removalOperationID, Job: &proto.JobParams{ID: infoA.ID}})
	if response.OK || response.Mutation == nil || response.Mutation.State != "ambiguous" || response.Mutation.OperationID != removalOperationID || response.ErrorEnvelope == nil || response.ErrorEnvelope.Validate() != nil || response.ErrorEnvelope.Code != proto.CodeAmbiguousOutcome || response.ErrorEnvelope.ExecutionState != proto.StatePossiblyExecuted || response.ErrorEnvelope.OperationID != removalOperationID {
		t.Fatal("remote deletion durability failure was acknowledged")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	// The retained owner can authorize an explicit idempotent cleanup of the
	// already-missing remote record. An unowned ID cannot pass this boundary.
	missing := require(a, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: infoA.ID}})
	if len(missing.Missing) != 1 || missing.Missing[0] != infoA.ID {
		t.Fatal("owned missing-record cleanup did not execute")
	}
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	if err := disk.Load(path); err != nil {
		t.Fatal(err)
	}
	if _, ok := disk.GetHost("runtime-host", infoA.ID); ok {
		t.Fatal("acknowledged cleanup lost on SIGKILL")
	}
	if ref, ok := disk.GetHost("runtime-host", infoB.ID); !ok || ref.Owner != b.Key() {
		t.Fatal("other project ownership lost during deletion recovery")
	}
	if status := require(b, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: infoB.ID}}).Info; status == nil || status.PID != infoB.PID || status.State != proto.JobRunning {
		t.Fatal("other owner's detached job did not survive recovery")
	}
	require(b, &proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: infoB.ID, Signal: "TERM", GraceSec: 1}})
	require(b, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: infoB.ID}})
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	if err := disk.Load(path); err != nil || len(disk.Snapshot()) != 0 {
		t.Fatal("final job removal was not durable")
	}
	out, err := sshRun("import os,sys,json\np=os.path.expanduser('~/'+sys.argv[1])\nprint(json.dumps([open(p+'/proof-'+n).read() for n in ['a','b']]))\n")
	var proofs []string
	if err != nil || json.Unmarshal(out, &proofs) != nil || fmt.Sprint(proofs) != "[once once]" {
		t.Fatal("detached job executed more than once")
	}
	t.Log("real detached jobs: separate frontend ACK survives SIGKILL; SSH-unavailable startup retains both project owners; same supervisors survive reconnect/SIGHUP; scoped Limit=1 listing precedes remote pagination; granted other project denied status/logs/wait/stop/rm; real deletion rename failure rolls back active ownership; owned Missing cleanup persists across SIGKILL; each job command executed once")
	t.Logf("remote supervisor PIDs retained across recovery: project A=%d project B=%d; final proof file values=%v", infoA.PID, infoB.PID, proofs)
}

func cleanupRemoteJobSupervisors(t *testing.T, sshRun func(string) ([]byte, error)) {
	t.Helper()
	// Cleanup only the exact test namespace and verified child process groups.
	t.Cleanup(func() {
		if out, err := sshRun(`import os,sys,signal,json
root=os.path.expanduser('~/'+sys.argv[1]);binary=root+'/rdev-agent'
for entry in os.listdir('/proc'):
 if not entry.isdigit(): continue
 try:
  pid=int(entry);args=open('/proc/'+entry+'/cmdline','rb').read().split(b'\0')
  if len(args)<3 or os.fsdecode(args[0])!=binary or args[1]!=b'-supervise': continue
  directory=os.fsdecode(args[2])
  if not directory.startswith(root+'/jobs/'): continue
  try:
   child=json.load(open(directory+'/child.json'))['child_pid']
   stat=open('/proc/'+str(child)+'/stat').read().rsplit(')',1)[1].split()
   if int(stat[1])==pid and os.getpgid(child)==child: os.killpg(child,signal.SIGKILL)
  except (FileNotFoundError,ProcessLookupError,KeyError): pass
  if os.getpgid(pid)==pid: os.killpg(pid,signal.SIGKILL)
 except (FileNotFoundError,ProcessLookupError): pass
`); err != nil {
			t.Errorf("job cleanup: %v %s", err, out)
		}
	})
}
