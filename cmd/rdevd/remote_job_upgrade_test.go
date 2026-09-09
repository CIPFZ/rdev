package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

// Both executables change: the original supervisors must keep executing their
// old agent inode while a new agent serves requests from the upgraded broker.
func TestRemoteBrokerJobUpgrade(t *testing.T) {
	oldDaemon := os.Getenv("RDEV_TEST_PREDECESSOR_DAEMON_BINARY")
	oldAgents := os.Getenv("RDEV_TEST_PREDECESSOR_AGENT_DIR")
	if oldDaemon == "" || oldAgents == "" {
		t.Skip("set predecessor daemon and agent artifacts for real upgrade coverage")
	}
	for _, stop := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		t.Run(stop.String(), func(t *testing.T) {
			d, namespace, sshRun := newRemoteRuntime(t)
			cleanupRemoteJobSupervisors(t, sshRun)
			currentDaemon := d.bin
			currentArgs := append([]string(nil), d.extraArgs...)
			currentAgents := os.Getenv("RDEV_TEST_CURRENT_AGENT_DIR")
			if currentAgents == "" {
				currentAgents = filepath.Join(repoRoot(t), "cmd/rdev/agents")
			}
			for i, arg := range currentArgs {
				if arg == "-agent-dir" {
					currentArgs[i+1] = currentAgents
				}
			}
			d.bin = oldDaemon
			d.extraArgs = append([]string(nil), currentArgs...)
			for i, arg := range d.extraArgs {
				if arg == "-agent-dir" {
					d.extraArgs[i+1] = oldAgents
				}
			}
			a := broker.Owner{ClientID: "job-upgrade", ProjectID: "phase5-lifecycle"}
			b := broker.Owner{ClientID: a.ClientID, ProjectID: "other-project"}
			owners := []broker.Owner{a, b}
			policy := broker.NewPolicy()
			if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
				t.Fatal(err)
			}
			for _, owner := range owners {
				for _, op := range []string{proto.OpJobStart, proto.OpJobList, proto.OpJobStatus, proto.OpJobLogs, proto.OpJobWait, proto.OpJobStop, proto.OpJobRm, proto.OpPing} {
					if err := policy.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
						t.Fatal(err)
					}
				}
				for _, op := range []string{"status", "job.events"} {
					if err := policy.Grant(owner.Key(), op); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := policy.Save(d.socket + ".policy"); err != nil {
				t.Fatal(err)
			}
			wires := map[broker.Owner]*runtimeWire{}
			connect := func() {
				for _, owner := range owners {
					wires[owner] = d.dial(owner, d.token(owner, "5m"), true)
				}
			}
			call := func(owner broker.Owner, wire *proto.Request) *proto.Response {
				t.Helper()
				req := broker.Request{Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire}
				if broker.RequiresApproval(req) {
					req.Approval = d.approve(owner, wire)
				}
				r := policyRuntimeRequest(t, wires[owner], req)
				if !r.OK || r.Wire == nil || !r.Wire.OK {
					t.Fatalf("upgrade operation %s failed: broker=%s", wire.Op, r.Error)
				}
				return r.Wire
			}
			d.start()
			connect()
			jobs := map[broker.Owner]*proto.JobInfo{}
			for i, owner := range owners {
				r := call(owner, &proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf once >> "$1"; printf ready; exec sleep 120`, "upgrade-proof", fmt.Sprintf("%s/proof-%d", namespace, i)}}}})
				if r.Job == nil || r.Job.Info == nil {
					t.Fatal("predecessor did not acknowledge job")
				}
				jobs[owner] = r.Job.Info
			}
			hashFile := func(path string) string {
				t.Helper()
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(data)
				return hex.EncodeToString(sum[:])
			}
			oldHash := hashFile(filepath.Join(oldAgents, "rdev-agent-linux-amd64"))
			newHash := hashFile(filepath.Join(currentAgents, "rdev-agent-linux-amd64"))
			if oldHash == newHash {
				t.Fatal("upgrade fixture must change the agent artifact")
			}
			inspectProcesses := func(agentHash string) {
				t.Helper()
				ping := call(a, &proto.Request{Op: proto.OpPing}).Ping
				if ping == nil {
					t.Fatal("base agent PID missing")
				}
				out, err := sshRun(fmt.Sprintf(`import hashlib,json
pids=[%d,%d,%d]
print(json.dumps([hashlib.sha256(open('/proc/'+str(p)+'/exe','rb').read()).hexdigest() for p in pids]))
`, ping.PID, jobs[a].PID, jobs[b].PID))
				var got []string
				if err != nil || json.Unmarshal(out, &got) != nil || len(got) != 3 || got[0] != agentHash || got[1] != oldHash || got[2] != oldHash {
					t.Fatalf("agent/supervisor executable identity changed unexpectedly: %v", err)
				}
			}
			assertOwned := func() {
				t.Helper()
				for _, owner := range owners {
					list := call(owner, &proto.Request{Op: proto.OpJobList, Job: &proto.JobParams{Limit: 1}}).Job
					if list == nil || len(list.List) != 1 || list.Total != 1 || list.List[0].ID != jobs[owner].ID {
						t.Fatal("upgrade lost ownership or scoped pagination")
					}
					info := call(owner, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: jobs[owner].ID}}).Job
					if info == nil || info.Info == nil || info.Info.PID != jobs[owner].PID || info.Info.State != proto.JobRunning {
						t.Fatal("upgrade replaced or stopped the original supervisor")
					}
				}
				for _, op := range []string{proto.OpJobStatus, proto.OpJobLogs, proto.OpJobWait, proto.OpJobStop, proto.OpJobRm} {
					for i, owner := range owners {
						req := broker.Request{Owner: owner, Operation: op, Host: "runtime-host", Wire: &proto.Request{Op: op, Job: &proto.JobParams{ID: jobs[owners[1-i]].ID}}}
						if broker.RequiresApproval(req) {
							req.Approval = d.approve(owner, req.Wire)
						}
						r := policyRuntimeRequest(t, wires[owner], req)
						if r.OK || r.Wire != nil {
							t.Fatal("upgrade allowed access to another project's job")
						}
					}
				}
			}
			assertOwned()
			inspectProcesses(oldHash)
			d.stop(stop)
			d.bin, d.extraArgs = currentDaemon, currentArgs
			d.start()
			connect()
			assertOwned()
			inspectProcesses(newHash)
			if err := d.cmd.Process.Signal(syscall.SIGHUP); err != nil {
				t.Fatal(err)
			}
			waitDaemonLog(t, d, "configuration reloaded")
			assertOwned()
			pending := startLifecycleProcess(t, d, a, &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobs[a].ID, WaitTimeoutSec: 40}}, false)
			awaitRuntime(t, 5*time.Second, "upgraded wait observation", func() bool {
				r := policyRuntimeRequest(t, wires[a], broker.Request{Owner: a, Operation: "status"})
				return r.OK && r.SharedWaits != nil && r.SharedWaits.Observers == 1 && r.SharedWaits.Subscribers == 1
			})
			before := time.Now()
			d.stop(syscall.SIGTERM)
			if time.Since(before) > 5*time.Second {
				t.Fatal("upgraded wait stranded graceful shutdown")
			}
			select {
			case <-pending.done:
			case <-time.After(5 * time.Second):
				t.Fatal("upgraded shutdown stranded the frontend process")
			}
			d.start()
			connect()
			assertOwned()
			inspectProcesses(newHash)
			for _, owner := range owners {
				call(owner, &proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: jobs[owner].ID, Signal: "TERM", GraceSec: 1}})
				wait := call(owner, &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobs[owner].ID, TailOnExit: 1, WaitTimeoutSec: 5}}).Job
				if wait == nil || wait.Info == nil || wait.Info.State == proto.JobRunning || wait.Logs != "ready" {
					t.Fatalf("upgraded wait lost predecessor terminal output: %+v", wait)
				}
				call(owner, &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: jobs[owner].ID}})
			}
			d.stop(syscall.SIGKILL)
			d.start()
			connect()
			for _, owner := range owners {
				list := call(owner, &proto.Request{Op: proto.OpJobList}).Job
				if list == nil || len(list.List) != 0 || list.Total != 0 {
					t.Fatal("removed predecessor job reappeared after crash")
				}
			}
			out, err := sshRun("import os,sys,json\np=os.path.expanduser('~/'+sys.argv[1])\nprint(json.dumps([open(p+'/proof-'+str(n)).read() for n in range(2)]))\n")
			var proofs []string
			if err != nil || json.Unmarshal(out, &proofs) != nil || fmt.Sprint(proofs) != "[once once]" {
				t.Fatal("upgrade repeated or lost a detached job execution")
			}
			t.Logf("real broker and agent upgrade after %s: original supervisor PIDs %d/%d and old executable hashes retained; new base agent executable verified; two-project ownership/list/status/log/wait/stop/rm isolation; reload and observed-wait drain/reconnect; old output preserved; removal durable after SIGKILL; each command executed once", stop, jobs[a].PID, jobs[b].PID)
		})
	}
}
