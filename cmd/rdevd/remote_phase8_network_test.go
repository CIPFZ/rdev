package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

// The owned real OpenSSH master is killed after an acknowledged detached job.
// No fake ssh process, transport timer or product fault hook is used.
func TestRemotePhase8MasterDisappearance(t *testing.T) {
	d, namespace, sshRun := newRemoteRuntime(t)
	cleanupRemoteJobSupervisors(t, sshRun)
	remote := os.Getenv("RDEV_TEST_REMOTE")
	if remote == "" {
		remote = "service-deploy"
	}
	ctlDir := filepath.Join(d.dir, "rdev-ctl")
	if err := os.Mkdir(ctlDir, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:0", remote)))
	ctl := filepath.Join(ctlDir, hex.EncodeToString(sum[:])[:16])
	d.env = append(d.env, "TMPDIR="+d.dir)
	base := []string{}
	if config := os.Getenv("RDEV_TEST_SSH_CONFIG"); config != "" {
		base = append(base, "-F", config)
	}
	master := exec.Command("ssh", append(append([]string{}, base...), "-N", "-o", "BatchMode=yes", "-o", "ControlMaster=yes", "-o", "ControlPersist=no", "-o", "ControlPath="+ctl, remote)...)
	if err := master.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = master.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			_ = master.Process.Kill()
			<-done
		}
		// A recovered ControlPersist master is also scoped to this one socket.
		_ = exec.Command("ssh", append(append([]string{}, base...), "-S", ctl, "-O", "exit", remote)...).Run()
	})
	awaitRuntime(t, 5*time.Second, "private real SSH master", func() bool {
		st, err := os.Stat(ctl)
		return err == nil && st.Mode()&os.ModeSocket != 0
	})
	a := broker.Owner{ClientID: "phase8-master", ProjectID: "owner"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "observer"}
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{proto.OpPing, proto.OpJobStart, proto.OpJobStatus, proto.OpJobStop, "mutation.status"} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	wa, wb := d.dial(a, d.token(a, "5m"), true), d.dial(b, d.token(b, "5m"), true)
	first := remoteWireCall(t, wa, a, &proto.Request{Op: proto.OpPing})
	if first.Ping == nil || first.Ping.PID == 0 {
		t.Fatal("initial agent identity missing")
	}
	start := &proto.Request{Op: proto.OpJobStart, OperationID: "op_phase8_master_once", Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf x >> "$1"; exec sleep 120`, "master-proof", namespace + "/master-marker"}}}}
	call := func(w *runtimeWire, owner broker.Owner, wire *proto.Request) broker.Response {
		req := broker.Request{Owner: owner, Host: "runtime-host", Operation: wire.Op, Wire: wire}
		if broker.RequiresApproval(req) {
			req.Approval = d.approve(owner, wire)
		}
		return policyRuntimeRequest(t, w, req)
	}
	started := call(wa, a, start)
	if !started.OK || started.Wire == nil || started.Wire.Job == nil || started.Wire.Job.Info == nil {
		t.Fatal("actual detached job did not start")
	}
	job := started.Wire.Job.Info
	if err := master.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("owned master did not exit")
	}
	awaitRuntime(t, 20*time.Second, "actual SSH reconnect after master SIGKILL", func() bool {
		r := call(wb, b, &proto.Request{Op: proto.OpPing})
		return r.OK && r.Wire != nil && r.Wire.Ping != nil && r.Wire.Ping.PID != first.Ping.PID
	})
	status := call(wa, a, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: job.ID}})
	if !status.OK || status.Wire == nil || status.Wire.Job == nil || status.Wire.Job.Info == nil || status.Wire.Job.Info.PID != job.PID || status.Wire.Job.Info.State != proto.JobRunning {
		t.Fatal("master loss changed or killed detached supervisor")
	}
	if denied := call(wb, b, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: job.ID}}); denied.OK || !strings.Contains(denied.Error, "job owner mismatch or unknown job") {
		t.Fatal("recovery changed owner isolation")
	}
	retained := policyRuntimeRequest(t, wa, broker.Request{Owner: a, Operation: "mutation.status", MutationID: start.OperationID})
	if !retained.OK || retained.Mutation == nil || retained.Mutation.State != "completed" || retained.Mutation.JobID != job.ID {
		t.Fatal("acknowledged mutation lost its committed identity")
	}
	out, err := sshRun("import os,sys,time\np=os.path.expanduser('~/'+sys.argv[1]+'/master-marker')\nend=time.monotonic()+5\nwhile not os.path.exists(p) and time.monotonic()<end:time.sleep(.01)\nassert open(p).read()=='x'\nprint('once')\n")
	if err != nil || strings.TrimSpace(string(out)) != "once" {
		t.Fatal("master recovery lost or duplicated the actual marker")
	}
	if stopped := call(wa, a, &proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: job.ID}}); !stopped.OK {
		t.Fatal("recovered owner could not stop retained job")
	}
	t.Log("real master SIGKILL: new serving agent, same detached supervisor, completed mutation retained, exact marker x, other-project denial")
}

func TestRemotePhase8DNSFailure(t *testing.T) {
	d, _, _ := newRemoteRuntime(t)
	// Record only the actual OpenSSH failure category, never argv or stderr.
	// This prevents a policy/permission/bootstrap failure from falsely passing
	// the DNS case merely because the final request was rejected.
	const wrapper = `#!/usr/bin/env python3
import json,os,subprocess,sys
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'):args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
result=subprocess.run(args+sys.argv[1:],stdin=sys.stdin.buffer,stdout=sys.stdout.buffer,stderr=subprocess.PIPE,env={**os.environ,'LC_ALL':'C'})
with open(os.environ['RDEV_PHASE8_DNS_LOG'],'a') as output:
 output.write(json.dumps({'resolution_failed':result.returncode==255 and b'Could not resolve hostname' in result.stderr and b'.invalid' in result.stderr})+'\n')
sys.stderr.buffer.write(result.stderr)
sys.exit(result.returncode)
`
	audit := filepath.Join(d.dir, "dns-category.jsonl")
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(wrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_PHASE8_DNS_LOG="+audit)
	path := filepath.Join(d.dir, "hosts.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var hosts struct {
		Hosts []map[string]any `json:"hosts"`
	}
	if err := json.Unmarshal(data, &hosts); err != nil {
		t.Fatal(err)
	}
	// RFC 2606 .invalid exercises real name resolution; no live target is used.
	hosts.Hosts[0]["addr"] = "rdev-" + filepath.Base(d.dir) + ".invalid"
	data, _ = json.Marshal(hosts)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	a := broker.Owner{ClientID: "phase8-dns", ProjectID: "isolated"}
	policy := broker.NewPolicy()
	if err := policy.Grant(a.Key(), proto.OpPing); err != nil {
		t.Fatal(err)
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	w := d.dial(a, d.token(a, "5m"), true)
	r := policyRuntimeRequest(t, w, broker.Request{Owner: a, Host: "runtime-host", Operation: proto.OpPing, Wire: &proto.Request{Op: proto.OpPing}})
	if r.OK || r.Error == "" || r.Wire != nil && r.Wire.OK {
		t.Fatal("real DNS failure did not fail closed")
	}
	observed, err := os.ReadFile(audit)
	if err != nil || !strings.Contains(string(observed), `"resolution_failed": true`) {
		t.Fatal("request rejection did not include an actual OpenSSH resolution failure")
	}
	t.Log("real OpenSSH .invalid resolution failed before agent installation or business request")
}
