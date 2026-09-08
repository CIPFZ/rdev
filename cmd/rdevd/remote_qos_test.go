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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

type qosResult struct {
	PID      int                  `json:"pid"`
	Calls    int                  `json:"calls"`
	Rejected int                  `json:"rejected"`
	AgentPID int                  `json:"agent_pid,omitempty"`
	Latency  map[string][]float64 `json:"latency_ms,omitempty"`
}
type qosChild struct {
	cmd    *exec.Cmd
	done   chan error
	log    *bytes.Buffer
	id     string
	killed bool
}

// This is a real daemon, twenty frontend processes, actual SSH, and sustained
// remote file reads. Timing windows measure completions with a continuously
// queued backlog; weights are changed by SIGHUP while the workload stays live.
func TestRemoteBrokerQoS(t *testing.T) {
	runRemoteBrokerQoS(t, false)
}

func TestRemoteBrokerQoSWithSecrets(t *testing.T) {
	runRemoteBrokerQoS(t, true)
}

func runRemoteBrokerQoS(t *testing.T, populatedSecrets bool) {
	if os.Getenv("RDEV_QOS_HELPER") == "1" {
		runQoSClient(t)
		return
	}
	// Race instrumentation can need a longer observation to collect the
	// same minimum 50 admissions. Only longer windows are accepted; the
	// per-second starvation check, fairness tolerance and latency SLO stay fixed.
	windowDuration := 25 * time.Second
	if value := os.Getenv("RDEV_QOS_WINDOW"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 25*time.Second || parsed > 40*time.Second {
			t.Fatal("RDEV_QOS_WINDOW must be between 25s and 40s")
		}
		windowDuration = parsed
	}
	d, namespace, sshRun := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "qos-shared-client", ProjectID: "project-a"}
	b := broker.Owner{ClientID: "qos-shared-client", ProjectID: "project-b"}
	ctl := []broker.Owner{{ClientID: "qos-control-a", ProjectID: "control"}, {ClientID: "qos-control-b", ProjectID: "control"}}
	owners := append([]broker.Owner{a, b}, ctl...)
	p := broker.NewPolicy()
	for _, owner := range owners {
		for _, op := range []string{"status", proto.OpPing, proto.OpReadFile} {
			if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Grant(owner.Key(), "status"); err != nil {
			t.Fatal(err)
		}
	}
	var secretOwners []broker.Owner
	if populatedSecrets {
		secretOwners = qosSecretOwners(t, p)
	}
	healthAdmin := broker.Owner{ClientID: "qos-health-admin", ProjectID: "operations"}
	if err := p.Grant(healthAdmin.Key(), "audit.health"); err != nil {
		t.Fatal(err)
	}
	if err := p.Grant(healthAdmin.Key(), "audit_query"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	config := broker.Config{MaxHosts: 128, IdleTTL: time.Minute, BulkIdleTTL: time.Second, QoS: broker.QoSConfig{MaxQueued: 64, PerOwnerQueued: 8}, OwnerWeights: map[string]int{a.Key(): 3, b.Key(): 1}}
	saveConfig := func() {
		data, _ := json.Marshal(config)
		if err := os.WriteFile(d.socket+".json", data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	saveConfig()
	fixture := strings.Repeat("phase5-qos-remote-io\n", 16384)
	digest := sha256.Sum256([]byte(fixture))
	script := "import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nos.makedirs(p,mode=0o700,exist_ok=True)\nwith open(p+'/qos-fixture','w') as f: f.write('phase5-qos-remote-io\\n'*16384)\n"
	if out, err := sshRun(script); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	d.start()
	if populatedSecrets {
		provisionQoSSecrets(t, d, secretOwners, namespace, sshRun)
	}
	wires := make(map[string]*runtimeWire)
	tokens := make(map[string]string)
	for _, owner := range owners {
		tokens[owner.Key()] = d.token(owner, "5m")
		wires[owner.Key()] = d.dial(owner, tokens[owner.Key()], true)
	}
	snapshots := func(owner broker.Owner) broker.SchedulerSnapshot {
		response := policyRuntimeRequest(t, wires[owner.Key()], broker.Request{Owner: owner, Operation: "status"})
		if !response.OK || response.Scheduler == nil {
			t.Fatal("missing owner scheduler snapshot")
		}
		return *response.Scheduler
	}
	warm := policyRuntimeRequest(t, wires[ctl[0].Key()], broker.Request{Owner: ctl[0], Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if !warm.OK || warm.Wire == nil || warm.Wire.Ping == nil {
		t.Fatal("remote warmup failed")
	}
	agentPID := warm.Wire.Ping.PID
	var children []*qosChild
	for i := 0; i < 20; i++ {
		owner, kind := a, "bulk"
		if i >= 9 {
			owner = b
		}
		if i >= 18 {
			owner, kind = ctl[i-18], "control"
		}
		id := fmt.Sprintf("qos-%02d", i)
		cmd := exec.Command(os.Args[0], "-test.run=^TestRemoteBrokerQoS$", "-test.timeout=2m")
		cmd.Env = append(os.Environ(), "RDEV_QOS_HELPER=1", "RDEV_QOS_ID="+id, "RDEV_QOS_KIND="+kind, "RDEV_QOS_DIR="+d.dir, "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PROJECT_ID="+owner.ProjectID, "RDEV_PRINCIPAL_TOKEN="+tokens[owner.Key()], "RDEV_BROKER_SOCKET="+d.socket, "RDEV_QOS_PATH="+namespace+"/qos-fixture", "RDEV_QOS_SIZE="+strconv.Itoa(len(fixture)), "RDEV_QOS_DIGEST="+hex.EncodeToString(digest[:]))
		child := &qosChild{cmd: cmd, done: make(chan error, 1), log: new(bytes.Buffer), id: id}
		cmd.Stdout, cmd.Stderr = child.log, child.log
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		go func() { child.done <- cmd.Wait() }()
		children = append(children, child)
	}
	awaitRuntime(t, 10*time.Second, "twenty QoS frontend processes", func() bool {
		for _, c := range children {
			if _, err := os.Stat(filepath.Join(d.dir, c.id+".ready")); err != nil {
				return false
			}
		}
		return true
	})
	phaseFile := filepath.Join(d.dir, "qos-phase")
	phase := func(value string) {
		if err := os.WriteFile(phaseFile, []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	phase("baseline")
	time.Sleep(5 * time.Second)
	// No remote work or statistics from A/B may leak into an unrelated owner's
	// snapshot, even though client IDs are shared across two distinct projects.
	if st := snapshots(a); st.Started+st.Rejected != 0 || st.Active+st.Queued != 0 {
		t.Fatal("baseline owner isolation failed")
	}
	type window struct {
		Phase                string `json:"phase"`
		A, B                 uint64
		Ratio                float64 `json:"ratio_a_b"`
		Samples, Backlogged  int
		MaxGapA, MaxGapB     float64
		ARejected, BRejected uint64
	}
	var windows []window
	sampleWindow := func(name string, duration time.Duration) window {
		beginA, beginB := snapshots(a), snapshots(b)
		prevA, prevB := beginA.Started, beginB.Started
		lastA, lastB := time.Now(), time.Now()
		result := window{Phase: name}
		until := time.Now().Add(duration)
		for time.Now().Before(until) {
			time.Sleep(time.Second)
			sa, sb := snapshots(a), snapshots(b)
			now := time.Now()
			if sa.Started != prevA {
				result.MaxGapA = max(result.MaxGapA, now.Sub(lastA).Seconds())
				lastA = now
			}
			if sb.Started != prevB {
				result.MaxGapB = max(result.MaxGapB, now.Sub(lastB).Seconds())
				lastB = now
			}
			if now.Sub(lastA) > 3*time.Second || now.Sub(lastB) > 3*time.Second {
				t.Fatal("sustained remote I/O starved an owner for over three seconds")
			}
			if sa.Queued >= 5 && sb.Queued >= 5 {
				result.Backlogged++
			}
			if sa.Active > 1 || sb.Active > 1 || sa.Queued > 8 || sb.Queued > 8 {
				t.Fatal("bulk/per-owner queue capacity exceeded")
			}
			if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 2 {
				t.Fatal("bulk load must use one base and one dedicated transport")
			}
			result.Samples++
			prevA, prevB = sa.Started, sb.Started
		}
		endA, endB := snapshots(a), snapshots(b)
		result.A, result.B = endA.Started-beginA.Started, endB.Started-beginB.Started
		result.ARejected, result.BRejected = endA.Rejected-beginA.Rejected, endB.Rejected-beginB.Rejected
		result.Ratio = float64(result.A) / float64(result.B)
		if result.A < 50 || result.B < 50 || result.Backlogged < result.Samples*3/4 {
			t.Fatalf("insufficient sustained pressure evidence: %+v", result)
		}
		return result
	}
	phase("weight-3-1")
	awaitRuntime(t, 10*time.Second, "two independent owner backlogs", func() bool { return snapshots(a).Queued >= 5 && snapshots(b).Queued >= 5 })
	t.Log("sustained real SSH phase: weights 3:1, two backlogged owners, 18 bulk processes and two control processes")
	first := sampleWindow("weight-3-1", windowDuration)
	if first.Ratio < 2.7 || first.Ratio > 3.3 {
		t.Fatalf("weighted remote fairness outside 10%%: %+v", first)
	}
	windows = append(windows, first)
	config.OwnerWeights = map[string]int{a.Key(): 1, b.Key(): 3}
	saveConfig()
	if err := d.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	// SIGHUP keeps the signing key unchanged, so the same authenticated sessions
	// and queued requests must continue without a restart or reconnect.
	phase("weight-1-3")
	time.Sleep(time.Second)
	second := sampleWindow("weight-1-3", windowDuration)
	if second.Ratio < .30 || second.Ratio > .37 {
		t.Fatalf("live reload did not reverse weighted remote service: %+v", second)
	}
	windows = append(windows, second)
	if first.ARejected+first.BRejected+second.ARejected+second.BRejected == 0 {
		t.Fatal("owner queue overload was not actually exercised")
	}
	// Crash every connection of one principal with both queued and active reads.
	// Remaining clients must progress on the same shared agent.
	for _, c := range children[:9] {
		c.killed = true
		_ = c.cmd.Process.Kill()
		<-c.done
	}
	awaitRuntime(t, 3*time.Second, "crashed owner queue release", func() bool { st := snapshots(a); return st.Active+st.Queued == 0 })
	afterKill := snapshots(b).Started
	phase("after-owner-kill")
	time.Sleep(3 * time.Second)
	if snapshots(b).Started <= afterKill+10 {
		t.Fatal("surviving owner stopped progressing after other-owner crash")
	}
	phase("stop")
	var controls []qosResult
	pids := make(map[int]bool)
	for _, c := range children {
		pids[c.cmd.Process.Pid] = true
		if c.killed {
			continue
		}
		select {
		case err := <-c.done:
			if err != nil {
				t.Fatalf("client %s: %v %s", c.id, err, c.log.String())
			}
		case <-time.After(10 * time.Second):
			t.Fatal("QoS client did not finish")
		}
		data, err := os.ReadFile(filepath.Join(d.dir, c.id+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var result qosResult
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result.PID != c.cmd.Process.Pid || result.Calls == 0 {
			t.Fatal("missing process completion evidence")
		}
		if result.AgentPID != 0 {
			if result.AgentPID != agentPID {
				t.Fatal("control client saw a replaced shared agent")
			}
			controls = append(controls, result)
		}
	}
	if len(pids) != 20 || len(controls) != 2 {
		t.Fatal("process evidence incomplete")
	}
	awaitRuntime(t, 3*time.Second, "remaining owner queues drained", func() bool { return snapshots(b).Active+snapshots(b).Queued == 0 })
	awaitRuntime(t, 7*time.Second, "bulk TTL leaves base transport alive", func() bool { return countDaemonSSHChildren(t, d.cmd.Process.Pid) == 1 })
	out, err := sshRun(remoteAgentPIDScript)
	if err != nil {
		t.Fatal(err)
	}
	var agents []int
	if json.Unmarshal(out, &agents) != nil || len(agents) != 1 || agents[0] != agentPID || countDaemonSSHChildren(t, d.cmd.Process.Pid) != 1 {
		t.Fatalf("shared SSH/agent changed: %s", out)
	}
	latency := make(map[string]map[string]float64)
	for i, c := range controls {
		base := percentile(c.Latency["baseline"], .95)
		loaded := append(c.Latency["weight-3-1"], c.Latency["weight-1-3"]...)
		p95 := percentile(loaded, .95)
		latency[strconv.Itoa(i)] = map[string]float64{"baseline_p95_ms": base, "bulk_p95_ms": p95, "ratio": p95 / base}
		if p95 > 2*base {
			t.Errorf("control p95 SLO failed: bulk %.3f ms / baseline %.3f ms = %.2f", p95, base, p95/base)
		}
		if len(loaded) < 100 {
			t.Fatal("insufficient remote control samples")
		}
	}
	administrator := d.dial(healthAdmin, d.token(healthAdmin, "5m"), true)
	if r := policyRuntimeRequest(t, wires[a.Key()], broker.Request{Owner: a, Operation: "audit.health"}); r.OK || r.AuditHealth != nil {
		t.Fatal("ordinary owner read global sink counters")
	}
	_ = policyRuntimeRequest(t, administrator, broker.Request{Owner: healthAdmin, Operation: "audit_query"})
	health := policyRuntimeRequest(t, administrator, broker.Request{Owner: healthAdmin, Operation: "audit.health"}).AuditHealth
	if health == nil || health.Dropped != 0 || health.Errors != 0 || health.Rotations == 0 {
		t.Fatalf("sustained audit sink lost events or did not rotate: %+v", health)
	}
	for _, path := range []string{d.socket + ".audit", d.socket + ".audit.1"} {
		st, err := os.Stat(path)
		if err != nil || st.Size() > 8<<20 {
			t.Fatal("audit retention exceeded its segment budget")
		}
	}
	report, _ := json.Marshal(map[string]any{"weight_window_ms": windowDuration.Milliseconds(), "populated_secret_versions": len(secretOwners) * 256, "audit_sink": health, "processes": len(pids), "bulk_processes": 18, "control_processes": 2, "windows": windows, "control_latency": latency, "same_base_agent": true, "dedicated_bulk_transport": true, "bulk_idle_ttl_ms": 1000, "bulk_idle_reaped": true, "owner_sigkill_recovery": true, "bytes_per_read": len(fixture), "owner_a_payload_bytes": snapshots(a).BulkPayloadBytes, "owner_b_payload_bytes": snapshots(b).BulkPayloadBytes})
	t.Logf("real remote QoS evidence: %s", report)
}

func runQoSClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	dir, id, kind := os.Getenv("RDEV_QOS_DIR"), os.Getenv("RDEV_QOS_ID"), os.Getenv("RDEV_QOS_KIND")
	if err := os.WriteFile(filepath.Join(dir, id+".ready"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	result := qosResult{PID: os.Getpid(), Latency: make(map[string][]float64)}
	size, _ := strconv.Atoi(os.Getenv("RDEV_QOS_SIZE"))
	for ctx.Err() == nil {
		data, _ := os.ReadFile(filepath.Join(dir, "qos-phase"))
		phase := string(data)
		if phase == "stop" {
			break
		}
		if phase == "" || (phase == "baseline" && kind == "bulk") {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		wire := &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: os.Getenv("RDEV_QOS_PATH"), Limit: int64(size)}}
		if kind == "control" {
			wire = &proto.Request{Op: proto.OpPing}
		}
		started := time.Now()
		resp, err := c.DoContext(ctx, broker.Request{Operation: wire.Op, Host: "runtime-host", Wire: wire})
		elapsed := float64(time.Since(started).Microseconds()) / 1000
		if err != nil {
			t.Fatal(err)
		}
		if !resp.OK && resp.Error == broker.ErrQueueFull.Error() {
			result.Rejected++
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if !resp.OK || resp.Wire == nil || !resp.Wire.OK {
			t.Fatalf("remote %s failed; broker error=%s", wire.Op, resp.Error)
		}
		if kind == "control" {
			if resp.Wire.Ping == nil {
				t.Fatal("missing ping")
			}
			if result.AgentPID != 0 && result.AgentPID != resp.Wire.Ping.PID {
				t.Fatal("shared agent replaced")
			}
			result.AgentPID = resp.Wire.Ping.PID
			result.Latency[phase] = append(result.Latency[phase], elapsed)
			time.Sleep(30 * time.Millisecond)
		} else {
			if resp.Wire.Read == nil {
				t.Fatal("missing read")
			}
			sum := sha256.Sum256([]byte(resp.Wire.Read.Content))
			if resp.Wire.Read.ContentB64 || len(resp.Wire.Read.Content) != size || hex.EncodeToString(sum[:]) != os.Getenv("RDEV_QOS_DIGEST") {
				t.Fatal("remote I/O content integrity failed")
			}
		}
		result.Calls++
	}
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	data, _ := json.Marshal(result)
	if err := os.WriteFile(filepath.Join(dir, id+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}
