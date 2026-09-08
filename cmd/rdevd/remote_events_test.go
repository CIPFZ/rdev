package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestRemoteBrokerJobEventHistory(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	cleanupRemoteJobSupervisors(t, sshRun)
	a := broker.Owner{ClientID: "events-shared", ProjectID: "phase5-lifecycle"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "other-project"}
	p := broker.NewPolicy()
	if err := p.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		if err := p.Grant(owner.Key(), "status"); err != nil {
			t.Fatal(err)
		}
		for _, op := range []string{proto.OpJobStart, proto.OpJobStatus, proto.OpJobWait, proto.OpJobRm, "job.events"} {
			if err := p.GrantHost(owner.Key(), "runtime-host", "job", op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	wires := make(map[broker.Owner]*runtimeWire)
	connect := func() {
		for _, owner := range []broker.Owner{a, b} {
			wires[owner] = d.dial(owner, d.token(owner, "5m"), true)
		}
	}
	connect()
	call := func(wire *proto.Request) broker.Response {
		r := broker.Request{Owner: a, Host: "runtime-host", Operation: wire.Op, Wire: wire}
		if broker.RequiresApproval(r) {
			r.Approval = d.approve(a, wire)
		}
		return policyRuntimeRequest(t, wires[a], r)
	}
	query := func(owner broker.Owner, id string, cursor broker.JobEventCursor, limit int) broker.Response {
		return policyRuntimeRequest(t, wires[owner], broker.Request{Owner: owner, Host: "runtime-host", Operation: "job.events", JobEvents: &broker.JobEventQuery{ID: id, Cursor: cursor, Limit: limit}})
	}
	start := func(suffix string) *proto.JobInfo {
		r := call(&proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `while [ ! -f "$1" ]; do sleep .05; done; printf event-history-output-canary`, "event-proof", namespace + "/" + suffix}}}})
		if !r.OK || r.Wire == nil || r.Wire.Job == nil || r.Wire.Job.Info == nil {
			t.Fatalf("start event job: %s", r.Error)
		}
		return r.Wire.Job.Info
	}
	release := func(suffix string) {
		out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nopen(p+'/" + suffix + "','w').close()\n")
		if err != nil {
			t.Fatalf("release real remote job: %v %s", err, out)
		}
	}
	job := start("finish-first")
	first := query(a, job.ID, broker.JobEventCursor{}, 1)
	if !first.OK || first.History == nil || len(first.History.Events) != 1 || first.History.Events[0].State != proto.JobRunning || first.History.Cursor.Sequence != 1 {
		t.Fatal("durable start event missing")
	}
	if r := query(b, job.ID, first.History.Cursor, 1); r.OK || r.History != nil {
		t.Fatal("other project queried event history")
	}
	var clients []*lifecycleProcess
	pids := make(map[int]bool)
	for range 20 {
		c := startLifecycleProcess(t, d, a, &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: job.ID, WaitTimeoutSec: 40, TailOnExit: 100}}, false)
		clients = append(clients, c)
		pids[c.cmd.Process.Pid] = true
	}
	if len(pids) != 20 {
		t.Fatal("wait subscribers were not independent processes")
	}
	waitCounts := func(observers, subscribers int) bool {
		r := policyRuntimeRequest(t, wires[a], broker.Request{Owner: a, Operation: "status"})
		return r.OK && r.Ingress != nil && (r.Ingress.ObservationBytes > 0) == (observers > 0) && r.SharedWaits != nil && r.SharedWaits.Observers == observers && r.SharedWaits.Subscribers == subscribers
	}
	awaitRuntime(t, 5*time.Second, "one shared observation and twenty subscribers", func() bool { return waitCounts(1, 20) })
	for _, c := range clients {
		_ = c.cmd.Process.Kill()
		select {
		case <-c.done:
		case <-time.After(5 * time.Second):
			t.Fatal("killed event subscriber remained alive")
		}
	}
	awaitRuntime(t, 5*time.Second, "zero subscribers preserve observation", func() bool { return waitCounts(1, 0) })
	release("finish-first")
	awaitRuntime(t, 8*time.Second, "terminal event durable with no subscribers", func() bool {
		r := query(a, job.ID, first.History.Cursor, 0)
		return r.OK && r.History != nil && len(r.History.Events) == 1 && r.History.Events[0].State == proto.JobExited && r.History.Cursor.Sequence == 2
	})
	awaitRuntime(t, 3*time.Second, "terminal observer releases detached ingress bytes", func() bool { return waitCounts(0, 0) })
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	after := query(a, job.ID, first.History.Cursor, 1)
	if !after.OK || after.History == nil || len(after.History.Events) != 1 || after.History.Cursor.Sequence != 2 || after.History.Cursor.Stream != first.History.Cursor.Stream || after.History.Truncated || after.History.More {
		t.Fatal("restart lost cursor or duplicated shared terminal event")
	}
	if r := call(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: job.ID}}); !r.OK {
		t.Fatal("remove completed event job", r.Error)
	}
	removed := query(a, job.ID, after.History.Cursor, 0)
	if !removed.OK || removed.History == nil || len(removed.History.Events) != 1 || removed.History.Events[0].State != "removed" || removed.History.Cursor.Sequence != 3 {
		t.Fatal("owned history lost after job removal")
	}
	verifyJobEventFrontends(t, d, a, b, job.ID, first.History.Cursor)
	// A real atomic-rename failure after remote completion must not publish
	// terminal history. Restoring storage and observing status repairs it once.
	second := start("finish-second")
	beforeFailure := query(a, second.ID, broker.JobEventCursor{}, 0)
	if !beforeFailure.OK || beforeFailure.History == nil {
		t.Fatal("second history missing")
	}
	path := d.socket + ".events"
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	release("finish-second")
	if r := call(&proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: second.ID, WaitTimeoutSec: 5}}); r.OK {
		t.Fatal("event persistence failure acknowledged")
	}
	if r := query(a, second.ID, beforeFailure.History.Cursor, 0); !r.OK || len(r.History.Events) != 0 {
		t.Fatal("failed event publication changed visible history")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	if r := call(&proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: second.ID}}); !r.OK {
		t.Fatal("history recovery failed", r.Error)
	}
	if r := query(a, second.ID, beforeFailure.History.Cursor, 0); !r.OK || len(r.History.Events) != 1 || r.History.Events[0].State != proto.JobExited {
		t.Fatal("terminal observation not repaired exactly once")
	}
	data, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(data), "event-history-output-canary") || strings.Contains(string(data), "while [") {
		t.Fatal("job event history retained command/output")
	}
	if r := call(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: second.ID}}); !r.OK {
		t.Fatal("second job cleanup failed", r.Error)
	}
	t.Log("real job history: 20 independent wait clients killed; one zero-subscriber observation retained its ingress charge and persisted exactly one terminal event, then released the charge; SIGKILL restart preserved cursor; removed job history retained for original owner; CLI/MCP cursor replay and other-project denials; real event rename failure retained old snapshot and later status repaired one terminal event; no argv/output in snapshot")
}

func verifyJobEventFrontends(t *testing.T, d *runtimeDaemon, owner, other broker.Owner, jobID string, cursor broker.JobEventCursor) {
	t.Helper()
	cli := filepath.Join(d.dir, "rdev-events")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build event frontend: %v %s", err, out)
	}
	command := func(principal broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+principal.ClientID, "RDEV_PROJECT_ID="+principal.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(principal, "5m"))
		return cmd
	}
	for _, principal := range []broker.Owner{owner, other} {
		out, err := command(principal, "job", "events", "runtime-host", jobID, "-stream", cursor.Stream, "-after", strconv.FormatUint(cursor.Sequence, 10)).Output()
		var page broker.JobEventPage
		if principal == owner {
			if err != nil || json.Unmarshal(out, &page) != nil || len(page.Events) != 2 || page.Events[0].State != proto.JobExited || page.Events[1].State != "removed" || page.Truncated {
				t.Fatal("CLI failed durable event replay")
			}
		} else if err == nil || len(out) != 0 {
			t.Fatal("CLI returned other owner's history")
		}
		client := mcp.NewClient(&mcp.Implementation{Name: "phase5-event-test", Version: "1"}, nil)
		session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: command(principal, "serve")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_job_events", Arguments: map[string]any{"host": "runtime-host", "id": jobID, "cursor": cursor}})
		if err != nil || result == nil || result.IsError != (principal == other) {
			t.Fatal("MCP event replay owner boundary failed")
		}
		if principal == owner {
			data, _ := json.Marshal(result.StructuredContent)
			if json.Unmarshal(data, &page) != nil || len(page.Events) != 2 || page.Cursor.Sequence != 3 || page.Truncated {
				t.Fatal("MCP replay cursor projection failed")
			}
		}
	}
}
