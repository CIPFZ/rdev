package main

import (
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestRemoteBrokerSharedWait(t *testing.T) {
	d, _, sshRun := newRemoteRuntime(t)
	cleanupRemoteJobSupervisors(t, sshRun)
	a := broker.Owner{ClientID: "wait-shared", ProjectID: "phase5-lifecycle"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "other-project"}
	p := broker.NewPolicy()
	if err := p.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		if err := p.Grant(owner.Key(), "status"); err != nil {
			t.Fatal(err)
		}
		for _, op := range []string{proto.OpJobStart, proto.OpJobStatus, proto.OpJobWait, proto.OpJobStop, proto.OpJobRm} {
			if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	w := d.dial(a, d.token(a, "5m"), true)
	other := d.dial(b, d.token(b, "5m"), true)
	call := func(req *proto.Request) *proto.JobResult {
		r := broker.Request{Owner: a, Operation: req.Op, Host: "runtime-host", Wire: req}
		if broker.RequiresApproval(r) {
			r.Approval = d.approve(a, req)
		}
		resp := policyRuntimeRequest(t, w, r)
		if !resp.OK || resp.Wire == nil || !resp.Wire.OK || resp.Wire.Job == nil {
			t.Fatalf("job request failed: %s", resp.Error)
		}
		return resp.Wire.Job
	}
	status := func(w *runtimeWire, owner broker.Owner) broker.SharedWaitStatus {
		r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "status"})
		if !r.OK || r.SharedWaits == nil {
			t.Fatal("shared observation status missing")
		}
		return *r.SharedWaits
	}
	start := func() *proto.JobInfo {
		return call(&proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "printf 'first\\nready'; exec sleep 300"}}}}).Info
	}
	job := start()
	if job == nil {
		t.Fatal("job missing")
	}
	wait := &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: job.ID, WaitTimeoutSec: 40, TailOnExit: 1}}
	first := startLifecycleProcess(t, d, a, wait, false)
	awaitRuntime(t, 5*time.Second, "one initial observation", func() bool { return status(w, a) == (broker.SharedWaitStatus{Observers: 1, Subscribers: 1}) })
	if err := first.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-first.done
	awaitRuntime(t, 5*time.Second, "initiator lease released independently", func() bool { return status(w, a) == (broker.SharedWaitStatus{Observers: 1, Subscribers: 0}) })
	clients := make([]*lifecycleProcess, 20)
	pids := make(map[int]bool)
	for i := range clients {
		variant := &proto.Request{Op: proto.OpJobWait, DeadlineUnixMilli: time.Now().Add(time.Duration(50+i) * time.Second).UnixMilli(), Job: &proto.JobParams{ID: job.ID, WaitTimeoutSec: 40 + i, TailOnExit: 1 + i%2}}
		clients[i] = startLifecycleProcess(t, d, a, variant, false)
		pids[clients[i].cmd.Process.Pid] = true
	}
	if len(pids) != 20 {
		t.Fatal("not 20 independent frontend processes")
	}
	awaitRuntime(t, 5*time.Second, "20 subscribers one remote wait", func() bool { return status(w, a) == (broker.SharedWaitStatus{Observers: 1, Subscribers: 20}) })
	if status(other, b) != (broker.SharedWaitStatus{}) {
		t.Fatal("other project saw wait counters")
	}
	if r := policyRuntimeRequest(t, other, broker.Request{Owner: b, Operation: proto.OpJobWait, Host: "runtime-host", Wire: wait}); r.OK {
		t.Fatal("granted other project joined owned wait")
	}
	for _, c := range clients[:5] {
		if err := c.cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		<-c.done
	}
	awaitRuntime(t, 5*time.Second, "killed subscribers detached", func() bool { return status(w, a) == (broker.SharedWaitStatus{Observers: 1, Subscribers: 15}) })
	if err := d.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitDaemonLog(t, d, "configuration reloaded")
	if got := status(w, a); got != (broker.SharedWaitStatus{Observers: 1, Subscribers: 15}) {
		t.Fatal("reload changed observation", got)
	}
	call(&proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: job.ID, Signal: "TERM", GraceSec: 1}})
	operationID := ""
	for i, c := range clients[5:] {
		r := c.result(t)
		logs := "ready"
		if (i+5)%2 == 1 {
			logs = "first\nready"
		}
		if r.Job == nil || r.Job.Info == nil || r.Job.Info.ID != job.ID || r.Job.Info.State == proto.JobRunning || r.Job.TimedOut || r.Job.Logs != logs {
			t.Fatalf("subscriber lost terminal job result: job=%+v", r.Job)
		}
		if operationID == "" {
			operationID = r.OperationID
		}
		if r.OperationID == "" || r.OperationID != operationID {
			t.Fatal("subscribers performed separate remote waits")
		}
	}
	awaitRuntime(t, 5*time.Second, "terminal observation released", func() bool { return status(w, a) == (broker.SharedWaitStatus{}) })
	call(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: job.ID}})
	// A different running job has a long remote observation during SIGTERM.
	// Shutdown cancels that observation promptly without stopping the supervisor.
	next := start()
	pending := startLifecycleProcess(t, d, a, &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: next.ID, WaitTimeoutSec: 40}}, false)
	awaitRuntime(t, 5*time.Second, "shutdown observation active", func() bool { return status(w, a).Observers == 1 })
	before := time.Now()
	d.stop(syscall.SIGTERM)
	elapsed := time.Since(before)
	if elapsed > 5*time.Second {
		t.Fatalf("observation stranded shutdown for %s", elapsed)
	}
	select {
	case <-pending.done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown stranded frontend")
	}
	d.start()
	w = d.dial(a, d.token(a, "5m"), true)
	info := call(&proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: next.ID}}).Info
	if info == nil || info.State != proto.JobRunning || info.PID != next.PID {
		t.Fatal("canceling observation killed detached job")
	}
	call(&proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: next.ID, Signal: "TERM", GraceSec: 1}})
	call(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: next.ID}})
	t.Logf("real shared wait: 20 independent PIDs with distinct timeout/deadline and tail1/2; initiator SIGKILL leaves 1 observer/0 subscribers; reconnect yields 1/20; five SIGKILLs yield 1/15; SIGHUP preserves fan-out; 15 terminal replies preserve each tail and share one nonempty remote operation ID; other-project wait/status denied; SIGTERM drain=%s; detached PID=%d preserved after restart", elapsed, next.PID)
}
