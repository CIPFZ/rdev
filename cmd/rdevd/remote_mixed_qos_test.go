package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mixedInput struct {
	Kind     string
	Requests []broker.Request
}
type mixedResult struct {
	PID      int
	AgentPID int
	Calls    map[string]int
	Latency  map[string][]float64
}

func runMixedClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	dir, id := os.Getenv("RDEV_MIXED_DIR"), os.Getenv("RDEV_MIXED_ID")
	raw, err := os.ReadFile(filepath.Join(dir, id+".input"))
	if err != nil {
		t.Fatal(err)
	}
	var input mixedInput
	if err := json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := mixedResult{PID: os.Getpid(), Calls: map[string]int{}, Latency: map[string][]float64{}}
	publish := func() {
		raw, _ := json.Marshal(result)
		tmp := filepath.Join(dir, id+".tmp")
		if err := os.WriteFile(tmp, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, filepath.Join(dir, id+".result")); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, id+".ready"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	count := 0
	for ctx.Err() == nil {
		phaseBytes, _ := os.ReadFile(filepath.Join(dir, "mixed-phase"))
		phase := string(phaseBytes)
		if phase == "stop" {
			break
		}
		if phase == "" || input.Kind == "sync" && phase != "loaded" && phase != "cancel" {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		index := count % len(input.Requests)
		if input.Kind == "exec" {
			if count >= len(input.Requests) {
				t.Fatal("execution exceeded preapproved bounded workload")
			}
			index = count
		}
		req := input.Requests[index]
		started := time.Now()
		r, err := c.DoContext(ctx, req)
		elapsed := float64(time.Since(started).Nanoseconds()) / 1e6
		if err != nil || !r.OK {
			t.Fatalf("mixed %s failed: %v %s", input.Kind, err, r.Error)
		}
		if input.Kind == "sync" && (r.Mutation == nil || !r.Mutation.RemoteOK || r.Sync == nil) {
			t.Fatal("sync did not complete its durable plan")
		}
		if req.Operation == proto.OpPing {
			if r.Wire == nil || r.Wire.Ping == nil {
				t.Fatal("missing control result")
			}
			if result.AgentPID != 0 && result.AgentPID != r.Wire.Ping.PID {
				t.Fatal("base agent changed")
			}
			result.AgentPID = r.Wire.Ping.PID
		}
		key := phase + "." + req.Operation
		result.Calls[key]++
		result.Latency[key] = append(result.Latency[key], elapsed)
		count++
		publish()
		if input.Kind == "sync" {
			break
		}
		if input.Kind == "control" {
			time.Sleep(10 * time.Millisecond)
		}
	}
	publish()
}

// Keep exec and timed job-wait loops running across both measurement phases.
// Only admitted sync uploads differ between the baseline and loaded phases.
// Seven independent client processes exercise the existing runtime, without a
// scheduler hook, fake delay, relaxed latency floor or replacement transport.
func TestRemoteBrokerMixedWorkload(t *testing.T) {
	if os.Getenv("RDEV_MIXED_HELPER") == "1" {
		runMixedClient(t)
		return
	}
	d, namespace, ssh := newRemoteRuntime(t)
	cleanupRemoteJobSupervisors(t, ssh)
	owner := func(project string) broker.Owner { return broker.Owner{ClientID: "mixed-shared", ProjectID: project} }
	syncOwners := []broker.Owner{owner("sync-a"), owner("sync-b")}
	execOwners := []broker.Owner{owner("exec-a"), owner("exec-b")}
	jobOwner, controlOwner, admin := owner("job"), owner("control"), runtimeApprovalAdmin()
	all := append(append(append([]broker.Owner{}, syncOwners...), execOwners...), jobOwner, controlOwner)
	policy := broker.NewPolicy()
	if err := policy.Grant(admin.Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	if err := policy.Grant(admin.Key(), "pool.health"); err != nil {
		t.Fatal(err)
	}
	for _, o := range all {
		if err := policy.Grant(o.Key(), "status"); err != nil {
			t.Fatal(err)
		}
		if err := policy.GrantHost(o.Key(), "runtime-host", broker.CapabilityForOperation(proto.OpPing), proto.OpPing); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range syncOwners {
		if err := policy.GrantHost(o.Key(), "runtime-host", "sync", "sync.push"); err != nil {
			t.Fatal(err)
		}
	}
	for _, o := range execOwners {
		if err := policy.GrantHost(o.Key(), "runtime-host", "exec", proto.OpExec); err != nil {
			t.Fatal(err)
		}
	}
	for _, op := range []string{proto.OpJobStart, proto.OpJobStatus, proto.OpJobWait, proto.OpJobStop, proto.OpJobRm} {
		if err := policy.GrantHost(jobOwner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
			t.Fatal(err)
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	cfg := broker.Config{MaxHosts: 128, IdleTTL: time.Minute, BulkIdleTTL: time.Second, QoS: broker.QoSConfig{MaxQueued: 64, PerOwnerQueued: 8, BulkBytesPerSecond: 2 << 20}}
	raw, _ := json.Marshal(cfg)
	if err := os.WriteFile(d.socket+".json", raw, 0600); err != nil {
		t.Fatal(err)
	}
	trafficLog := filepath.Join(d.dir, "mixed-traffic.jsonl")
	wrapper := strings.Replace(trafficSSHWrapper, "('read_file','write_file')", "('read_file','write_file','sync_inspect','sync_stage','sync_commit')", 1)
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_TRAFFIC_LOG="+trafficLog, "RDEV_TRAFFIC_CUT="+filepath.Join(d.dir, "never-cut"))
	d.start()
	cli := os.Getenv("RDEV_TEST_CLI_BINARY")
	if cli == "" {
		cli = filepath.Join(d.dir, "mixed-cli")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
		build.Dir = repoRoot(t)
		if raw, err := build.CombinedOutput(); err != nil {
			t.Fatal(err, string(raw))
		}
	}
	frontendStatus := func(o broker.Owner, useMCP bool) broker.StatusSnapshot {
		t.Helper()
		args := []string{"broker", "status"}
		if useMCP {
			args = []string{"serve"}
		}
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+o.ClientID, "RDEV_PROJECT_ID="+o.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(o, "5m"))
		var out []byte
		if useMCP {
			mc := mcp.NewClient(&mcp.Implementation{Name: "mixed-status-proof", Version: "1"}, nil)
			ms, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: cmd}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer ms.Close()
			r, err := ms.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_broker_status", Arguments: map[string]any{}})
			if err != nil || r == nil || r.IsError {
				t.Fatal("MCP sync status", err)
			}
			out, err = json.Marshal(r.StructuredContent)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			var err error
			out, err = cmd.Output()
			if err != nil {
				t.Fatal("CLI sync status", err)
			}
		}
		var projected broker.StatusSnapshot
		if err := json.Unmarshal(out, &projected); err != nil || len(projected.PolicyDigest) != 64 {
			t.Fatal("incomplete frontend status", err)
		}
		return projected
	}
	wires := map[broker.Owner]*runtimeWire{}
	for _, o := range append(all, admin) {
		wires[o] = d.dial(o, d.token(o, "5m"), true)
	}
	call := func(o broker.Owner, r broker.Request) broker.Response {
		r.Owner = o
		return policyRuntimeRequest(t, wires[o], r)
	}
	status := func(o broker.Owner) broker.SchedulerSnapshot {
		t.Helper()
		r := call(o, broker.Request{Operation: "status"})
		if !r.OK || r.Scheduler == nil {
			t.Fatal("status unavailable", r.Error)
		}
		return *r.Scheduler
	}
	resources := func(o broker.Owner) broker.StatusSnapshot {
		t.Helper()
		r, err := broker.ProjectStatus(call(o, broker.Request{Operation: "status"}))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	issue := func(o broker.Owner, r broker.Request) broker.Request {
		t.Helper()
		a := call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: o, Host: r.Host, Operation: r.Operation, Sync: r.Sync, Wire: r.Wire, TTL: 2 * time.Minute}})
		if !a.OK || a.Approval == nil {
			t.Fatal("approval unavailable", a.Error)
		}
		r.Approval = a.Approval.Token
		return r
	}
	var syncRequests []broker.Request
	payload := bytes.Repeat([]byte{0, 1, 127, 255, 13, 10}, 2<<20)
	sum := sha256.Sum256(payload)
	for i, o := range syncOwners {
		source := t.TempDir()
		if err := os.WriteFile(filepath.Join(source, "payload"), payload, 0600); err != nil {
			t.Fatal(err)
		}
		opts := &client.SyncOptions{Direction: "push", Local: source + "/", Remote: fmt.Sprintf("~/%s/mixed-%d/", namespace, i), Prepare: true, DryRun: true, MaxOutputBytes: 64 << 10}
		req := broker.Request{Operation: "sync.push", Host: "runtime-host", Sync: opts}
		r := call(o, req)
		if !r.OK || r.Sync == nil {
			t.Fatal("prepare mixed sync", r.Error)
		}
		opts.Prepare, opts.DryRun, opts.PlanID = false, false, r.Sync.PlanID
		req = issue(o, req)
		req.OperationID, _ = proto.NewOperationID()
		syncRequests = append(syncRequests, req)
	}
	start := broker.Request{Operation: proto.OpJobStart, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "sleep 90; printf job-finished"}}}}}
	startedJob := call(jobOwner, issue(jobOwner, start))
	if !startedJob.OK || startedJob.Wire == nil || startedJob.Wire.Job == nil || startedJob.Wire.Job.Info == nil {
		t.Fatal("job setup", startedJob.Error)
	}
	jobID := startedJob.Wire.Job.Info.ID
	var children []*qosChild
	spawn := func(id, kind string, o broker.Owner, requests []broker.Request) {
		t.Helper()
		raw, _ := json.Marshal(mixedInput{Kind: kind, Requests: requests})
		if err := os.WriteFile(filepath.Join(d.dir, id+".input"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestRemoteBrokerMixedWorkload$", "-test.timeout=110s")
		cmd.Env = append(os.Environ(), "RDEV_MIXED_HELPER=1", "RDEV_MIXED_DIR="+d.dir, "RDEV_MIXED_ID="+id, "RDEV_CLIENT_ID="+o.ClientID, "RDEV_PROJECT_ID="+o.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(o, "5m"), "RDEV_BROKER_SOCKET="+d.socket)
		child := &qosChild{cmd: cmd, done: make(chan error, 1), log: new(bytes.Buffer), id: id}
		cmd.Stdout, cmd.Stderr = child.log, child.log
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		go func() { child.done <- cmd.Wait() }()
		children = append(children, child)
	}
	for i, o := range syncOwners {
		spawn(fmt.Sprintf("sync-%d", i), "sync", o, []broker.Request{syncRequests[i]})
	}
	for i, o := range execOwners {
		var requests []broker.Request
		for range 45 {
			requests = append(requests, issue(o, broker.Request{Operation: proto.OpExec, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "sleep 1; printf exec-progress"}, MaxOutputBytes: 1024, TimeoutSec: 5}}}))
		}
		spawn(fmt.Sprintf("exec-%d", i), "exec", o, requests)
	}
	spawn("wait", "wait", jobOwner, []broker.Request{{Operation: proto.OpJobWait, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobID, WaitTimeoutSec: 1}}}})
	ping := broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}}
	spawn("job-control", "control", jobOwner, []broker.Request{ping, {Operation: proto.OpJobStatus, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: jobID}}}, {Operation: "status"}})
	spawn("control", "control", controlOwner, []broker.Request{ping, {Operation: "status"}})
	awaitRuntime(t, 10*time.Second, "seven independent clients ready", func() bool {
		for _, c := range children {
			if _, err := os.Stat(filepath.Join(d.dir, c.id+".ready")); err != nil {
				return false
			}
		}
		return true
	})
	phase := func(p string) {
		if err := os.WriteFile(filepath.Join(d.dir, "mixed-phase"), []byte(p), 0600); err != nil {
			t.Fatal(err)
		}
	}
	result := func(id string) mixedResult {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(d.dir, id+".result"))
		var r mixedResult
		if err != nil || json.Unmarshal(raw, &r) != nil {
			t.Fatalf("missing %s progress", id)
		}
		return r
	}
	phase("baseline")
	time.Sleep(5 * time.Second)
	basePing := call(controlOwner, ping)
	if !basePing.OK || basePing.Wire == nil || basePing.Wire.Ping == nil {
		t.Fatal("baseline ping failed", basePing.Error)
	}
	basePID := basePing.Wire.Ping.PID
	for _, id := range []string{"exec-0", "exec-1", "wait"} {
		r := result(id)
		total := 0
		for key, n := range r.Calls {
			if strings.HasPrefix(key, "baseline.") {
				total += n
			}
		}
		if total < 3 {
			t.Fatal("baseline workload not sustained", id)
		}
	}
	initial := []uint64{status(syncOwners[0]).BulkPayloadBytes, status(syncOwners[1]).BulkPayloadBytes}
	for _, o := range syncOwners {
		if r := resources(o); r.Ingress.ObservationBytes != 16<<20 || r.Ingress.Bytes > broker.MaxOwnerIngressBytes {
			t.Fatal("retained sync plan reservation", r.Ingress)
		}
	}
	if resources(controlOwner).Ingress.ObservationBytes != 0 {
		t.Fatal("retained sync resources crossed projects")
	}
	for _, useMCP := range []bool{false, true} {
		if frontendStatus(syncOwners[0], useMCP).Ingress.ObservationBytes != 16<<20 || frontendStatus(controlOwner, useMCP).Ingress.ObservationBytes != 0 {
			t.Fatal("frontend retained sync resource isolation")
		}
	}
	phase("loaded")
	loadStart := time.Now()
	lastProgress := []time.Time{loadStart, loadStart}
	previous := append([]uint64(nil), initial...)
	finished := []bool{false, false}
	peakOwnerActive, peakOwnerQueued := 0, 0
	var peakSyncReservation int64
	for !finished[0] || !finished[1] {
		if time.Since(loadStart) > 40*time.Second {
			t.Fatal("bounded mixed sync did not finish")
		}
		time.Sleep(250 * time.Millisecond)
		for _, o := range all {
			r := resources(o)
			s := r.Scheduler
			peakOwnerActive, peakOwnerQueued = max(peakOwnerActive, s.Active), max(peakOwnerQueued, s.Queued)
			if s.Active > 4 || s.Queued > 8 {
				t.Fatal("mixed owner limits exceeded")
			}
			if r.Ingress.Bytes > broker.MaxOwnerIngressBytes {
				t.Fatal("mixed owner ingress exceeded", r.Ingress)
			}
			if o == syncOwners[0] || o == syncOwners[1] {
				peakSyncReservation = max(peakSyncReservation, r.Ingress.ObservationBytes)
			}
		}
		for i, o := range syncOwners {
			s := status(o)
			if s.BulkPayloadBytes > previous[i] {
				lastProgress[i] = time.Now()
				previous[i] = s.BulkPayloadBytes
			}
			if !finished[i] && time.Since(lastProgress[i]) > 3*time.Second {
				t.Fatal("sync owner made no progress for three seconds", i)
			}
			select {
			case err := <-children[i].done:
				if err != nil {
					t.Fatalf("sync child: %v %s", err, children[i].log.String())
				}
				finished[i] = true
				children[i].done = nil
			default:
			}
		}
	}
	loadedDuration := time.Since(loadStart)
	for i, o := range syncOwners {
		if status(o).BulkPayloadBytes-initial[i] != uint64(len(payload)) {
			t.Fatal("sync payload accounting mismatch")
		}
	}
	if peakSyncReservation != 24<<20 {
		t.Fatal("sync execution reservation not observed", peakSyncReservation)
	}
	// Keep the same exec/wait/control clients alive for a separate cancellation
	// phase, so killing one uploader cannot improve the measured bulk SLO.
	phase("cancel")
	cancelInitial := []uint64{status(syncOwners[0]).BulkPayloadBytes, status(syncOwners[1]).BulkPayloadBytes}
	for i, o := range syncOwners {
		opts := *syncRequests[i].Sync
		opts.Remote = fmt.Sprintf("~/%s/cancel-%d/", namespace, i)
		opts.Prepare, opts.DryRun, opts.PlanID = true, true, ""
		req := broker.Request{Operation: "sync.push", Host: "runtime-host", Sync: &opts}
		r := call(o, req)
		if !r.OK || r.Sync == nil {
			t.Fatal("prepare cancellation sync", r.Error)
		}
		opts.Prepare, opts.DryRun, opts.PlanID = false, false, r.Sync.PlanID
		req = issue(o, req)
		req.OperationID, _ = proto.NewOperationID()
		spawn(fmt.Sprintf("cancel-sync-%d", i), "sync", o, []broker.Request{req})
	}
	awaitRuntime(t, 5*time.Second, "both cancellation-phase uploads started", func() bool {
		return status(syncOwners[0]).BulkPayloadBytes > cancelInitial[0] && status(syncOwners[1]).BulkPayloadBytes > cancelInitial[1]
	})
	for _, useMCP := range []bool{false, true} {
		if frontendStatus(syncOwners[0], useMCP).Ingress.ObservationBytes != 24<<20 || frontendStatus(controlOwner, useMCP).Ingress.ObservationBytes != 0 {
			t.Fatal("frontend active sync resource isolation")
		}
	}
	canceled, peer := children[7], children[8]
	peerBeforeKill := status(syncOwners[1]).BulkPayloadBytes
	if status(syncOwners[0]).BulkPayloadBytes-cancelInitial[0] >= uint64(len(payload)) || peerBeforeKill-cancelInitial[1] >= uint64(len(payload)) {
		t.Fatal("upload finished before cancellation")
	}
	if err := canceled.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-canceled.done:
		if err == nil {
			t.Fatal("canceled upload exited successfully")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("killed uploader did not exit")
	}
	awaitRuntime(t, 5*time.Second, "canceled owner released worker and reservations", func() bool {
		r := resources(syncOwners[0])
		return r.Scheduler.Active == 0 && r.Scheduler.Queued == 0 && r.Ingress.ObservationBytes == 0 && r.Ingress.Connections == 1
	})
	select {
	case err := <-peer.done:
		if err != nil {
			t.Fatalf("peer uploader failed after cancellation: %v %s", err, peer.log.String())
		}
	case <-time.After(15 * time.Second):
		t.Fatal("peer uploader stalled after cancellation")
	}
	if n := status(syncOwners[1]).BulkPayloadBytes; n <= peerBeforeKill || n-cancelInitial[1] != uint64(len(payload)) {
		t.Fatal("peer upload did not make complete progress after cancellation")
	}
	phase("stop")
	for _, c := range children[2:7] {
		select {
		case err := <-c.done:
			if err != nil {
				t.Fatalf("mixed child %s: %v %s", c.id, err, c.log.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("mixed frontend did not finish", c.id)
		}
	}
	var report = map[string]any{"measurement_processes": 7, "source_bytes_each": len(payload), "loaded_seconds": loadedDuration.Seconds(), "bulk_bytes_per_second": 2 << 20, "same_base_agent": true, "mixed_sync_sigkill_isolated": true, "peak_owner_active": peakOwnerActive, "peak_owner_queued": peakOwnerQueued, "peak_sync_reservation_bytes": peakSyncReservation}
	for _, id := range []string{"job-control", "control"} {
		r := result(id)
		if r.AgentPID != basePID {
			t.Fatal("control lost base agent")
		}
		for _, op := range []string{proto.OpPing, "status", proto.OpJobStatus} {
			baseline, loaded := r.Latency["baseline."+op], r.Latency["loaded."+op]
			if len(baseline) == 0 {
				continue
			}
			if len(baseline) < 100 || len(loaded) < 100 {
				t.Fatal("insufficient latency samples", id, op, len(baseline), len(loaded))
			}
			b, p := percentile(baseline, .95), percentile(loaded, .95)
			report[id+"."+op] = map[string]any{"baseline_p95_ms": b, "bulk_p95_ms": p, "ratio": p / b, "samples": len(loaded)}
			if p > 2*b {
				t.Errorf("mixed %s/%s p95 %.3f / %.3f = %.2f exceeds 2x", id, op, p, b, p/b)
			}
		}
	}
	for _, id := range []string{"exec-0", "exec-1", "wait"} {
		r := result(id)
		n := 0
		for key, c := range r.Calls {
			if strings.HasPrefix(key, "loaded.") {
				n += c
			}
		}
		report[id+"_loaded_completions"] = n
		if n < 5 {
			t.Error("mixed workload lacked progress", id, n)
		}
		n = 0
		for key, c := range r.Calls {
			if strings.HasPrefix(key, "cancel.") {
				n += c
			}
		}
		if n < 3 {
			t.Error("mixed workload stalled during cancellation", id, n)
		}
		report[id+"_cancellation_completions"] = n
	}
	for _, o := range syncOwners {
		s := status(o)
		if r := resources(o); r.Ingress.ObservationBytes != 0 || r.Scheduler.Active != 0 || r.Scheduler.Queued != 0 {
			t.Fatal("sync resources not released", r)
		}
		report[o.ProjectID+"_payload_bytes"] = s.BulkPayloadBytes
	}
	if status(controlOwner).BulkPayloadBytes != 0 || status(controlOwner).Traffic[broker.LaneBulk] != (observe.TrafficSnapshot{}) {
		t.Fatal("sync traffic crossed projects")
	}
	if out, err := ssh("import os,sys,hashlib\np=os.path.expanduser('~/'+sys.argv[1])\nassert not os.path.exists(p+'/cancel-0/payload')\nfor name in ('mixed-0','mixed-1','cancel-1'):\n data=open(p+'/'+name+'/payload','rb').read();assert len(data)==12582912;assert hashlib.sha256(data).hexdigest()=='" + hex.EncodeToString(sum[:]) + "'\n"); err != nil {
		t.Fatal("mixed transfer integrity", err, string(out))
	}
	// The transparent relay knows only principal hashes and byte counts. Compare
	// its actual frames with owner-scoped CLI and MCP projections after quiescence.
	for _, o := range syncOwners {
		projected := frontendStatus(o, false)
		expected := map[broker.Lane]observe.TrafficSnapshot{}
		raw, err := os.ReadFile(trafficLog)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte{'\n'}) {
			var row struct {
				Owner string
				Lane  broker.Lane
				observe.TrafficSnapshot
			}
			if err := json.Unmarshal(line, &row); err != nil {
				t.Fatal(err)
			}
			if row.Owner == proto.PrincipalID(o.ClientID, o.ProjectID) {
				n := expected[row.Lane]
				n.SentBytes += row.SentBytes
				n.ReceivedBytes += row.ReceivedBytes
				expected[row.Lane] = n
			}
		}
		for _, lane := range []broker.Lane{broker.LaneControl, broker.LaneExec, broker.LaneBulk} {
			got := projected.Scheduler.Traffic[lane]
			if got != expected[lane] {
				t.Fatalf("sync byte ledger mismatch %s: %+v %+v", lane, got, expected[lane])
			}
		}
		m := frontendStatus(o, true)
		if m.Scheduler.BulkPayloadBytes != projected.Scheduler.BulkPayloadBytes {
			t.Fatal("MCP sync payload projection")
		}
		for _, lane := range []broker.Lane{broker.LaneControl, broker.LaneExec, broker.LaneBulk} {
			got := m.Scheduler.Traffic[lane]
			if got != projected.Scheduler.Traffic[lane] {
				t.Fatal("CLI/MCP sync traffic differs")
			}
		}
		if m.Ingress.ObservationBytes != 0 || projected.Ingress.ObservationBytes != 0 {
			t.Fatal("frontend reports retained sync reservation after completion")
		}
	}
	awaitRuntime(t, 8*time.Second, "idle bulk reclaimed while base stays", func() bool {
		r := call(admin, broker.Request{Operation: "pool.health"})
		return r.OK && r.Pool != nil && r.Pool.BaseTransports == 1 && r.Pool.BulkTransports == 0
	})
	out, err := ssh(remoteAgentPIDScript)
	var pids []int
	if err != nil || json.Unmarshal(out, &pids) != nil || len(pids) != 1 || pids[0] != basePID {
		t.Fatal("bulk idle reclamation replaced base", err, string(out))
	}
	report["idle_bulk_reclaimed"] = true
	report["cli_mcp_exact_sync_traffic"] = true
	stop := broker.Request{Operation: proto.OpJobStop, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: jobID, Signal: "TERM", GraceSec: 1}}}
	if r := call(jobOwner, issue(jobOwner, stop)); !r.OK {
		t.Fatal("job cleanup", r.Error)
	}
	raw, _ = json.Marshal(report)
	t.Logf("mixed-load runtime evidence: %s", raw)
}
