package main

import (
	"context"
	"encoding/json"
	"errors"
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

// This helper is a separate authenticated frontend process. SIGKILL below
// exercises kernel socket teardown, not a dispatcher cancellation substitute.
func TestRemoteBrokerLifecycleClient(t *testing.T) {
	if os.Getenv("RDEV_LIFECYCLE_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: "phase5-lifecycle"}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var wire proto.Request
	if err := json.Unmarshal([]byte(os.Getenv("RDEV_LIFECYCLE_REQUEST")), &wire); err != nil {
		t.Fatal(err)
	}
	path := os.Getenv("RDEV_LIFECYCLE_RESULT")
	if err := os.WriteFile(path+".sent", nil, 0600); err != nil {
		t.Fatal(err)
	}
	go func() {
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if _, err := os.Stat(path + ".cancel"); err == nil {
					cancel()
					return
				}
			}
		}
	}()
	response, err := c.DoContext(ctx, broker.Request{Host: "runtime-host", Operation: wire.Op, Wire: &wire})
	if errors.Is(err, context.Canceled) {
		if err := os.WriteFile(path+".cancelled", nil, 0600); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil || !response.OK || response.Wire == nil || !response.Wire.OK {
		t.Fatalf("remote request failed: %v %+v", err, response)
	}
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("RDEV_LIFECYCLE_HOLD") == "1" {
		for {
			if _, err := os.Stat(path + ".release"); err == nil {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
}

type lifecycleProcess struct {
	cmd     *exec.Cmd
	done    chan error
	path    string
	logPath string
}

func startLifecycleProcess(t *testing.T, d *runtimeDaemon, owner broker.Owner, request *proto.Request, hold bool) *lifecycleProcess {
	t.Helper()
	path := filepath.Join(d.dir, owner.ClientID+".result")
	logPath := path + ".log"
	log, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(request)
	cmd := exec.Command(os.Args[0], "-test.run=^TestRemoteBrokerLifecycleClient$", "-test.timeout=50s")
	cmd.Env = append(os.Environ(), "RDEV_LIFECYCLE_HELPER=1", "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PRINCIPAL_TOKEN="+d.token(owner, "2m"), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_LIFECYCLE_REQUEST="+string(data), "RDEV_LIFECYCLE_RESULT="+path)
	if broker.RequiresApproval(broker.Request{Operation: request.Op, Wire: request}) {
		cmd.Env = append(cmd.Env, "RDEV_APPROVAL_TOKEN="+d.approve(owner, request))
	}
	if hold {
		cmd.Env = append(cmd.Env, "RDEV_LIFECYCLE_HOLD=1")
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	p := &lifecycleProcess{cmd: cmd, done: make(chan error, 1), path: path, logPath: logPath}
	go func() { p.done <- cmd.Wait(); log.Close() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	awaitRuntime(t, 5*time.Second, "frontend request sent", func() bool { _, err := os.Stat(path + ".sent"); return err == nil })
	return p
}

func (p *lifecycleProcess) result(t *testing.T) *proto.Response {
	t.Helper()
	select {
	case err := <-p.done:
		if err != nil {
			data, _ := os.ReadFile(p.logPath)
			t.Fatalf("frontend %d: %v %s", p.cmd.Process.Pid, err, data)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("frontend request did not finish")
	}
	data, err := os.ReadFile(p.path)
	if err != nil {
		t.Fatal(err)
	}
	var response broker.Response
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatal(err)
	}
	return response.Wire
}

func awaitRuntime(t *testing.T, timeout time.Duration, label string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}

func remoteWireCall(t *testing.T, w *runtimeWire, owner broker.Owner, wire *proto.Request) *proto.Response {
	t.Helper()
	_ = w.SetDeadline(time.Now().Add(15 * time.Second))
	if err := w.enc.Encode(broker.Request{ID: "remote-call", Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire}); err != nil {
		t.Fatal(err)
	}
	var response broker.Response
	if err := w.dec.Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Wire == nil || !response.Wire.OK {
		t.Fatalf("remote request failed: %+v", response)
	}
	return response.Wire
}

func lifecyclePolicy(t *testing.T, d *runtimeDaemon, names ...string) []broker.Owner {
	t.Helper()
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	owners := make([]broker.Owner, len(names))
	for i, name := range names {
		owners[i] = broker.Owner{ClientID: name, ProjectID: "phase5-lifecycle"}
		policy.Grant(owners[i].Key(), "ping")
		policy.Grant(owners[i].Key(), "exec")
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	return owners
}

func awaitDispatchCancellation(t *testing.T, d *runtimeDaemon, owner broker.Owner, started time.Time) time.Duration {
	t.Helper()
	awaitRuntime(t, time.Second, "broker dispatch cancellation for "+owner.ClientID, func() bool {
		data, _ := os.ReadFile(d.socket + ".audit")
		for _, line := range strings.Split(string(data), "\n") {
			var event broker.AuditEvent
			if json.Unmarshal([]byte(line), &event) == nil && event.Owner == broker.AuditOwnerID(owner.Key()) && event.Result == "dispatch_error" {
				return true
			}
		}
		return false
	})
	return time.Since(started)
}

const gatedSSHWrapper = `#!/usr/bin/env python3
import os,sys,time
if any('exec "$1" -state "$2"' in arg for arg in sys.argv[1:]):
 gate=os.environ['RDEV_TEST_DIAL_GATE']
 if os.path.exists(gate):
  with open(gate+'.entered','w') as f: f.write(str(os.getpid()))
  while os.path.exists(gate): time.sleep(.01)
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'): args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
os.execv(args[0],args+sys.argv[1:])
`

func TestRemoteBrokerRetryCancellation(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	owners := lifecyclePolicy(t, d, "retry", "waiter", "survivor", "foreground-cancel", "foreground-survive")
	gate := filepath.Join(d.dir, "dial-gate")
	t.Cleanup(func() { _ = os.Remove(gate) })
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(gatedSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_TEST_DIAL_GATE="+gate)
	d.start()
	w := d.dial(owners[2], d.token(owners[2], "2m"), true)
	first := remoteWireCall(t, w, owners[2], &proto.Request{Op: proto.OpPing}).Ping
	if first == nil || first.PID <= 0 {
		t.Fatal("missing remote PID")
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	// Kill only the agent that the daemon just identified, after verifying its
	// exact executable path belongs to this test's remote namespace.
	out, err := sshRun(fmt.Sprintf("import os,sys,signal\npid=%d\nargs=open('/proc/'+str(pid)+'/cmdline','rb').read().split(b'\\0')\nassert os.fsdecode(args[0])==os.path.expanduser('~/'+sys.argv[1]+'/rdev-agent')\nos.kill(pid,signal.SIGKILL)\n", first.PID))
	if err != nil {
		t.Fatalf("inject remote agent crash: %v %s", err, out)
	}
	retry := startLifecycleProcess(t, d, owners[0], &proto.Request{Op: proto.OpPing}, false)
	var blockedPID int
	awaitRuntime(t, 10*time.Second, "retry agent startup barrier", func() bool {
		data, _ := os.ReadFile(gate + ".entered")
		blockedPID, _ = strconv.Atoi(string(data))
		return blockedPID > 0
	})
	waiter := startLifecycleProcess(t, d, owners[1], &proto.Request{Op: proto.OpPing}, false)
	time.Sleep(50 * time.Millisecond)
	cancelStarted := time.Now()
	if err := os.WriteFile(waiter.path+".cancel", nil, 0600); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, time.Second, "frontend DoContext cancellation", func() bool { _, err := os.Stat(waiter.path + ".cancelled"); return err == nil })
	waiterCancel := awaitDispatchCancellation(t, d, owners[1], cancelStarted)
	// The first dial remains blocked while the independent waiter unwinds.
	if err := syscall.Kill(blockedPID, 0); err != nil {
		t.Fatal("waiter cancellation closed the initiating transport")
	}
	survivor := startLifecycleProcess(t, d, owners[2], &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "printf 'once\\n' >> \"$HOME/$1/mutation\"; printf survived", "rdev", namespace}, TimeoutSec: 10}}, false)
	cancelStarted = time.Now()
	_ = retry.cmd.Process.Kill()
	retryCancel := awaitDispatchCancellation(t, d, owners[0], cancelStarted)
	awaitRuntime(t, 5*time.Second, "independent owner replacement dial", func() bool {
		data, _ := os.ReadFile(gate + ".entered")
		pid, _ := strconv.Atoi(string(data))
		return pid > 0 && pid != blockedPID
	})
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	result := survivor.result(t)
	if result.Exec == nil || result.Exec.Stdout != "survived" || result.Exec.ExitCode != 0 {
		t.Fatalf("survivor exec: %+v", result.Exec)
	}
	second := remoteWireCall(t, w, owners[2], &proto.Request{Op: proto.OpPing}).Ping
	if second.PID == first.PID || second.CallerID != first.CallerID {
		t.Fatal("retry did not preserve principal on a fresh agent")
	}
	out, err = sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/mutation')\nassert open(p).read()=='once\\n'\n")
	if err != nil {
		t.Fatalf("mutation duplicated or missing: %v %s", err, out)
	}

	// With the replacement transport established, killing one foreground owner
	// must cancel its remote process while another owner's exec still completes.
	canceled := startLifecycleProcess(t, d, owners[3], &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "echo $$ > \"$HOME/$1/canceled.pid\"; sleep 20; printf bad > \"$HOME/$1/canceled-output\"", "rdev", namespace}, TimeoutSec: 30}}, false)
	kept := startLifecycleProcess(t, d, owners[4], &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "echo $$ > \"$HOME/$1/survivor.pid\"; sleep 1; printf owner-alive", "rdev", namespace}, TimeoutSec: 10}}, false)
	awaitRuntime(t, 5*time.Second, "both remote execs started", func() bool {
		out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nassert os.path.exists(p+'/canceled.pid') and os.path.exists(p+'/survivor.pid')\n")
		_ = out
		return err == nil
	})
	cancelStarted = time.Now()
	_ = canceled.cmd.Process.Kill()
	foregroundCancel := awaitDispatchCancellation(t, d, owners[3], cancelStarted)
	result = kept.result(t)
	if result.Exec == nil || result.Exec.Stdout != "owner-alive" || result.Exec.ExitCode != 0 {
		t.Fatalf("other owner canceled: %+v", result.Exec)
	}
	awaitRuntime(t, 5*time.Second, "canceled remote process removed", func() bool {
		_, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\npid=int(open(p+'/canceled.pid').read())\nassert not os.path.exists('/proc/'+str(pid))\nassert not os.path.exists(p+'/canceled-output')\n")
		return err == nil
	})
	last := remoteWireCall(t, w, owners[2], &proto.Request{Op: proto.OpPing}).Ping
	if last.PID != second.PID {
		t.Fatal("frontend cancellation replaced the shared transport")
	}
	if n := countDaemonSSHChildren(t, d.cmd.Process.Pid); n != 1 {
		t.Fatalf("active shared SSH processes=%d", n)
	}
	t.Logf("real SSH retry/cancellation: old_agent=%d replacement_agent=%d waiter_cancel_ms=%.3f retry_cancel_ms=%.3f foreground_cancel_ms=%.3f mutation_executions=1 survivor_exec=passed shared_transport_preserved=true", first.PID, second.PID, float64(waiterCancel.Microseconds())/1000, float64(retryCancel.Microseconds())/1000, float64(foregroundCancel.Microseconds())/1000)
}

func TestRemoteBrokerLeaseLifecycle(t *testing.T) {
	d, _, sshRun := newRemoteRuntime(t)
	const cycles = 6
	names := make([]string, cycles+1)
	names[0] = "transient"
	for i := 1; i < len(names); i++ {
		names[i] = fmt.Sprintf("lease-%d", i)
	}
	owners := lifecyclePolicy(t, d, names...)
	// Reaping runs every five seconds; a live frontend must keep its transport
	// even after both the configured grace and at least one reaper tick elapse.
	config, _ := json.Marshal(broker.Config{MaxHosts: 128, IdleTTL: 250 * time.Millisecond})
	if err := os.WriteFile(d.socket+".json", config, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	started := time.Now()
	previousPID := 0
	var reapMS []float64
	for i := 0; i < cycles; i++ {
		p := startLifecycleProcess(t, d, owners[i+1], &proto.Request{Op: proto.OpPing}, true)
		var ping *proto.PingResult
		awaitRuntime(t, 10*time.Second, "held frontend ping result", func() bool {
			data, err := os.ReadFile(p.path)
			if err != nil {
				return false
			}
			var response broker.Response
			if json.Unmarshal(data, &response) != nil || response.Wire == nil {
				return false
			}
			ping = response.Wire.Ping
			return ping != nil
		})
		if ping.PID <= 0 || ping.PID == previousPID {
			t.Fatal("idle generation was reused after reaping")
		}
		holdUntil := time.Now().Add(5200 * time.Millisecond)
		for time.Now().Before(holdUntil) {
			if n := countDaemonSSHChildren(t, d.cmd.Process.Pid); n != 1 {
				t.Fatalf("active frontend lost shared transport: %d SSH children", n)
			}
			time.Sleep(100 * time.Millisecond)
		}
		// Exercise fresh admission and detach against the continuously running reaper
		// while one owner retains its lease; each call must reuse the held agent.
		for n := 0; n < 10; n++ {
			w := d.dial(owners[0], d.token(owners[0], "2m"), true)
			result := remoteWireCall(t, w, owners[0], &proto.Request{Op: proto.OpPing}).Ping
			_ = w.Close()
			if result.PID != ping.PID || result.CallerID == ping.CallerID {
				t.Fatal("new owner lost pool reuse or principal isolation")
			}
		}
		released := time.Now()
		if i%2 == 0 {
			_ = p.cmd.Process.Kill()
			select {
			case <-p.done:
			case <-time.After(time.Second):
				t.Fatal("frontend SIGKILL did not exit")
			}
		} else {
			if err := os.WriteFile(p.path+".release", nil, 0600); err != nil {
				t.Fatal(err)
			}
			_ = p.result(t)
		}
		awaitRuntime(t, 7*time.Second, "idle SSH and remote agent removal", func() bool {
			if countDaemonSSHChildren(t, d.cmd.Process.Pid) != 0 {
				return false
			}
			data, err := sshRun(remoteAgentPIDScript)
			var pids []int
			return err == nil && json.Unmarshal(data, &pids) == nil && len(pids) == 0
		})
		elapsed := time.Since(released)
		if elapsed < 250*time.Millisecond {
			t.Fatalf("last-client grace skipped: %s", elapsed)
		}
		reapMS = append(reapMS, float64(elapsed.Microseconds())/1000)
		previousPID = ping.PID
	}
	// No new admission occurred, so another ticker tick must not reap/log the
	// already-empty generation again.
	time.Sleep(5100 * time.Millisecond)
	data, err := os.ReadFile(filepath.Join(d.dir, "daemon.log"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "reaped idle broker connections"); n != cycles {
		t.Fatalf("reaper events=%d for %d idle generations", n, cycles)
	}
	metrics, _ := json.Marshal(map[string]any{"cycles": cycles, "frontend_sigkill": cycles / 2, "frontend_normal_close": cycles / 2, "new_owner_requests": cycles * 10, "elapsed_ms": float64(time.Since(started).Microseconds()) / 1000, "idle_ttl_ms": 250, "reaper_tick_ms": 5000, "reap_ms": reapMS, "active_agent_preserved": true, "final_daemon_ssh": countDaemonSSHChildren(t, d.cmd.Process.Pid)})
	t.Logf("real SSH lease lifecycle: %s", metrics)
}
