package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

// Real daemon and OpenSSH: configured-but-cold hosts, sequential capacity,
// running request preservation, control access during cold admission, canceled
// waiters, reload shrink, TTL and final-client recovery.
func TestRemoteBrokerWarmPool(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "pool-shared", ProjectID: "work"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "other"}
	admin := runtimeApprovalAdmin()
	policy := broker.NewPolicy()
	for _, op := range []string{"approval.create", "pool.health"} {
		if err := policy.Grant(admin.Key(), op); err != nil {
			t.Fatal(err)
		}
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"status", proto.OpPing, proto.OpExec, proto.OpJobStart, proto.OpJobStatus, proto.OpJobWait, proto.OpJobStop, proto.OpJobRm} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := policy.Grant(a.Key(), "job.events"); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"pool.health", "audit.health"} {
		if err := policy.GrantHost(b.Key(), "runtime-host", op, op); err != nil {
			t.Fatal(err)
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	var hosts struct {
		Hosts []map[string]any `json:"hosts"`
	}
	path := filepath.Join(d.dir, "hosts.json")
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &hosts) != nil {
		t.Fatal("read runtime hosts")
	}
	names := []string{"runtime-host"}
	for i := 1; i < 100; i++ {
		h := map[string]any{}
		for k, v := range hosts.Hosts[0] {
			h[k] = v
		}
		h["name"] = fmt.Sprintf("warm-%03d", i)
		names = append(names, h["name"].(string))
		hosts.Hosts = append(hosts.Hosts, h)
	}
	data, _ = json.Marshal(hosts)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	config := broker.Config{MaxHosts: 128, IdleTTL: time.Minute, WarmIdleTTL: time.Hour}
	saveConfig := func() {
		data, _ := json.Marshal(config)
		if err := os.WriteFile(d.socket+".json", data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	saveConfig()
	cleanupRemoteJobSupervisors(t, sshRun)
	d.start()
	wires := []*runtimeWire{}
	connect := func(owner broker.Owner) *runtimeWire {
		w := d.dial(owner, d.token(owner, "10m"), true)
		wires = append(wires, w)
		return w
	}
	wa, wb, wh := connect(a), connect(b), connect(admin)
	health := func() broker.PoolHealth {
		r := policyRuntimeRequest(t, wh, broker.Request{Owner: admin, Operation: "pool.health"})
		if !r.OK || r.Pool == nil {
			t.Fatal("pool admin health unavailable")
		}
		return *r.Pool
	}
	if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 || health().ReservedHosts != 0 {
		t.Fatal("configured cold hosts created transports")
	}
	for _, owner := range []broker.Owner{a, b} {
		w := wa
		if owner == b {
			w = wb
		}
		r := w.call(t, owner, "pool.health")
		if r.OK || r.Pool != nil {
			t.Fatal("ordinary owner read global pool")
		}
		if r = w.call(t, owner, "status"); !r.OK || r.Pool != nil {
			t.Fatal("ordinary status leaked global pool")
		}
	}
	for _, op := range []string{"pool.health", "audit.health"} {
		r := policyRuntimeRequest(t, wb, broker.Request{Owner: b, Operation: op, Host: "runtime-host"})
		if r.OK || r.Pool != nil || r.AuditHealth != nil {
			t.Fatal("exact-host health grant leaked global state")
		}
	}
	ping := func(w *runtimeWire, owner broker.Owner, host string) int {
		r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpPing, Host: host, Wire: &proto.Request{Op: proto.OpPing}})
		if !r.OK || r.Wire == nil || !r.Wire.OK || r.Wire.Ping == nil {
			t.Fatalf("ping %s failed: %s", host, r.Error)
		}
		return r.Wire.Ping.PID
	}
	peakSSH := 0
	for i, name := range names {
		ping(wa, a, name)
		h := health()
		n := countDaemonSSHChildren(t, d.cmd.Process.Pid)
		peakSSH = max(peakSSH, n)
		if h.Limit != 16 || h.ReservedHosts > 16 || h.BaseTransports > 16 || n > 16 {
			t.Fatalf("sequential visit %d exceeded capacity: SSH=%d health=%+v", i, n, h)
		}
	}
	if h := health(); h.BaseTransports != 16 || h.Evictions["capacity_lru"].Count != 84 {
		t.Fatalf("100-host LRU evidence incomplete: %+v", h)
	}
	var remotePIDs []int
	awaitRuntime(t, 5*time.Second, "remote warm agents within cap", func() bool {
		out, err := sshRun(remoteAgentPIDScript)
		return err == nil && json.Unmarshal(out, &remotePIDs) == nil && len(remotePIDs) == 16
	})
	t.Logf("100 configured aliases: cold SSH=0, sequential requests=100, peak retained SSH=%d, final remote agents=%d, LRU retirements=84", peakSSH, len(remotePIDs))
	basePID := ping(wa, a, "runtime-host")
	// Hold actual remote execution while shrinking and sweeping the pool.
	execWire := connect(a)
	execReq := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf started > "$1/pool-started"; while [ ! -f "$1/pool-finish" ]; do sleep 0.05; done; printf pool-exec-preserved`, "pool-proof", namespace}}}
	req := broker.Request{Owner: a, Host: "runtime-host", Operation: proto.OpExec, Wire: execReq, Approval: d.approve(a, execReq)}
	_ = execWire.SetDeadline(time.Now().Add(time.Minute))
	if err := execWire.enc.Encode(req); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "real long exec started", func() bool {
		out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nprint(os.path.exists(p+'/pool-started'))\n")
		return err == nil && strings.TrimSpace(string(out)) == "True"
	})
	config.MaxWarmHosts = 1
	config.WarmIdleTTL = time.Second
	saveConfig()
	if err := d.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 10*time.Second, "reload shrink preserving exec", func() bool { h := health(); return h.Limit == 1 && h.ReservedHosts == 1 && h.ActiveLeases == 1 })
	canceled := connect(b)
	coldReq := broker.Request{Owner: b, Host: "warm-001", Operation: proto.OpPing, Wire: &proto.Request{Op: proto.OpPing}}
	if err := canceled.enc.Encode(coldReq); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "cold retained in scheduler queue behind active host", func() bool { return health().Queued == 1 })
	if ping(wb, b, "runtime-host") != basePID {
		t.Fatal("queued cold host evicted active exec")
	}
	canceled.Close()
	awaitRuntime(t, 5*time.Second, "canceled cold reservation released", func() bool { return health().Queued == 0 })
	cold := connect(b)
	_ = cold.SetDeadline(time.Now().Add(time.Minute))
	if err := cold.enc.Encode(coldReq); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "next cold request queued", func() bool { return health().Queued == 1 })
	// Cross at least one real five-second sweep with the exec still running.
	start := time.Now()
	for time.Since(start) < 6*time.Second {
		if ping(wb, b, "runtime-host") != basePID {
			t.Fatal("sweep replaced in-flight base agent")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nopen(p+'/pool-finish','w').close()\n"); err != nil {
		t.Fatalf("release remote exec: %v %s", err, out)
	}
	var response broker.Response
	if err := execWire.dec.Decode(&response); err != nil || !response.OK || response.Wire == nil || response.Wire.Exec == nil || response.Wire.Exec.Stdout != "pool-exec-preserved" {
		t.Fatal("capacity/TTL interrupted remote exec")
	}
	_ = cold.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err := cold.dec.Decode(&response); err != nil || !response.OK || response.Wire == nil || response.Wire.Ping == nil {
		t.Fatal("cold host starved after active release")
	}
	awaitRuntime(t, 10*time.Second, "TTL without closing frontend sockets", func() bool {
		h := health()
		return h.ReservedHosts == 0 && countDaemonSSHChildren(t, d.cmd.Process.Pid) == 0
	})
	if health().Evictions["idle_ttl"].Count == 0 {
		t.Fatal("TTL reason not observable")
	}
	// A detached logical observer survives its last subscriber and retains its
	// resources, while each bounded remote wait yields the active/warm host slot.
	// Exercise both independent limits at one: a continued observation must not
	// permanently prevent another owner from using a cold host.
	config.MaxHosts = 1
	logPath := filepath.Join(d.dir, "daemon.log")
	beforeReload, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	reloads := strings.Count(string(beforeReload), "configuration reloaded")
	saveConfig()
	if err := d.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "active-host limit reloaded for observation handoff", func() bool {
		data, err := os.ReadFile(logPath)
		return err == nil && strings.Count(string(data), "configuration reloaded") > reloads
	})
	jobCall := func(wire *proto.Request) broker.Response {
		req := broker.Request{Owner: a, Host: "runtime-host", Operation: wire.Op, Wire: wire}
		if broker.RequiresApproval(req) {
			req.Approval = d.approve(a, wire)
		}
		return policyRuntimeRequest(t, wa, req)
	}
	startedJob := jobCall(&proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sleep", "120"}}}})
	if !startedJob.OK || startedJob.Wire == nil || startedJob.Wire.Job == nil || startedJob.Wire.Job.Info == nil {
		t.Fatal("detached job start failed")
	}
	jobID := startedJob.Wire.Job.Info.ID
	jobPID := startedJob.Wire.Job.Info.PID
	observationBasePID := ping(wa, a, "runtime-host")
	logicalObservation := func(observers int) bool {
		r := policyRuntimeRequest(t, wa, broker.Request{Owner: a, Operation: "status"})
		return r.OK && r.SharedWaits != nil && r.SharedWaits.Observers == observers && r.SharedWaits.Subscribers == 0 && r.Ingress != nil && (r.Ingress.ObservationBytes > 0) == (observers > 0)
	}
	waitWire := connect(a)
	if err := waitWire.enc.Encode(broker.Request{Owner: a, Host: "runtime-host", Operation: proto.OpJobWait, Wire: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobID}}}); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "shared remote observation lease", func() bool { return health().ActiveLeases == 1 })
	waitWire.Close()
	awaitRuntime(t, 5*time.Second, "zero-subscriber observation retained", func() bool {
		return logicalObservation(1)
	})
	deniedWait := policyRuntimeRequest(t, wb, broker.Request{Owner: b, Host: "runtime-host", Operation: proto.OpJobWait, Wire: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobID}}})
	if deniedWait.OK {
		t.Fatal("pool observer leaked job to other project")
	}
	coldWait := connect(b)
	coldStarted := time.Now()
	_ = coldWait.SetDeadline(coldStarted.Add(10 * time.Second))
	if err := coldWait.enc.Encode(coldReq); err != nil {
		t.Fatal(err)
	}
	if err := coldWait.dec.Decode(&response); err != nil || !response.OK || response.Wire == nil || !response.Wire.OK || response.Wire.Ping == nil {
		t.Fatal("cold request did not progress between bounded observations", err)
	}
	coldElapsed := time.Since(coldStarted)
	if !logicalObservation(1) {
		t.Fatal("cold host admission discarded the detached observer or its resources")
	}
	if h := health(); h.ActiveHosts > 1 || h.ReservedHosts > 1 {
		t.Fatal("observation handoff exceeded active/warm host limits", h)
	}
	reconnectedBasePID := ping(wa, a, "runtime-host")
	if reconnectedBasePID == observationBasePID {
		t.Fatal("cold capacity handoff did not replace the old base connection")
	}
	stillRunning := jobCall(&proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: jobID}})
	if !stillRunning.OK || stillRunning.Wire == nil || !stillRunning.Wire.OK || stillRunning.Wire.Job == nil || stillRunning.Wire.Job.Info == nil || stillRunning.Wire.Job.Info.PID != jobPID || stillRunning.Wire.Job.Info.State != proto.JobRunning {
		t.Fatal("connection handoff replaced or stopped the detached supervisor")
	}
	stopped := jobCall(&proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: jobID, Signal: "TERM"}})
	if !stopped.OK || stopped.Wire == nil || !stopped.Wire.OK {
		t.Fatal("warm job stop blocked by cold waiter")
	}
	awaitRuntime(t, 5*time.Second, "reconnected terminal observer releases its resources", func() bool { return logicalObservation(0) })
	history := policyRuntimeRequest(t, wa, broker.Request{Owner: a, Host: "runtime-host", Operation: "job.events", JobEvents: &broker.JobEventQuery{ID: jobID}})
	if !history.OK || history.History == nil {
		t.Fatal("reconnected job history missing", history.Error)
	}
	terminalEvents := 0
	for _, event := range history.History.Events {
		if event.State == proto.JobExited && event.PID == jobPID {
			terminalEvents++
		}
	}
	if terminalEvents != 1 {
		t.Fatal("reconnected observation lost or duplicated the terminal event", terminalEvents)
	}
	removed := jobCall(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: jobID}})
	if !removed.OK || removed.Wire == nil || !removed.Wire.OK {
		t.Fatal("owned job cleanup failed after eviction")
	}
	t.Logf("zero-subscriber logical observation retained owner/resources with max_hosts=max_warm_hosts=1; cold host progressed in %s; base PID %d -> %d; original supervisor PID %d survived; another project was denied; reconnected stop persisted one terminal event and released observation resources", coldElapsed, observationBasePID, reconnectedBasePID, jobPID)
	// A rejected shrink retains the previous capacity.
	config.MaxWarmHosts = -1
	saveConfig()
	_ = d.cmd.Process.Signal(syscall.SIGHUP)
	awaitRuntime(t, 5*time.Second, "invalid pool config rejected", func() bool {
		data, _ := os.ReadFile(filepath.Join(d.dir, "daemon.log"))
		return strings.Contains(string(data), "reload rejected")
	})
	if health().Limit != 1 {
		t.Fatal("invalid reload changed pool")
	}
	config.MaxWarmHosts = 2
	config.WarmIdleTTL = time.Hour
	config.IdleTTL = time.Second
	saveConfig()
	_ = d.cmd.Process.Signal(syscall.SIGHUP)
	awaitRuntime(t, 5*time.Second, "last-client config reload", func() bool { return health().Limit == 2 })
	ping(wa, a, "runtime-host")
	for _, w := range wires {
		w.Close()
	}
	awaitRuntime(t, 10*time.Second, "last client SSH cleanup", func() bool { return countDaemonSSHChildren(t, d.cmd.Process.Pid) == 0 })
	wh = connect(admin)
	if health().Evictions["last_client"].Count == 0 {
		t.Fatal("last-client reason not observable")
	}
	t.Log("real active exec preserved through capacity shrink and TTL; other project control retained same agent while cold host queued; disconnected waiter released; next cold host progressed; idle and last-client cleanup returned SSH to zero; default pool-health deny and invalid reload retained")
}
