package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Real processes, the official MCP SDK, and the isolated real SSH harness must
// agree on execution, observation, job lifetime, and literal path semantics.
func TestRemotePhase6FrontendContracts(t *testing.T) {
	d, namespace, ssh := newRemoteRuntime(t)
	cleanupRemoteJobSupervisors(t, ssh)
	cli := os.Getenv("RDEV_TEST_CLI_BINARY")
	if cli == "" {
		cli = filepath.Join(d.dir, "rdev")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build CLI: %v %s", err, out)
		}
	}
	owner := broker.Owner{ClientID: "phase6-frontends", ProjectID: "contract-runtime"}
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{proto.OpExec, proto.OpJobStart, proto.OpJobWait, proto.OpJobStatus, proto.OpJobStop, "sync.push"} {
		if err := policy.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
			t.Fatal(err)
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	// Standalone frontends use an isolated global registry, so no project trust
	// exception or process-global HOME mutation is needed for the test.
	frontendHome := filepath.Join(d.dir, "frontend-home")
	if err := os.MkdirAll(filepath.Join(frontendHome, ".rdev"), 0700); err != nil {
		t.Fatal(err)
	}
	hosts, err := os.ReadFile(filepath.Join(d.dir, "hosts.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(frontendHome, ".rdev", "hosts.json"), hosts, 0600); err != nil {
		t.Fatal(err)
	}
	d.start()
	// Keep business destinations outside the agent's changing state tree. A
	// single-file rename binds its parent snapshot; using namespace itself would
	// correctly fail when sync staging/outcome records change that same tree.
	if out, err := ssh("import os,sys\nos.makedirs(os.path.expanduser('~/'+sys.argv[1]+'/literal-targets'),mode=0o700)\n"); err != nil {
		t.Fatalf("literal sync fixture: %v %s", err, out)
	}
	token := d.token(owner, "10m")
	adminWire := d.dial(runtimeApprovalAdmin(), d.token(runtimeApprovalAdmin(), "10m"), true)
	for _, mode := range []struct {
		name        string
		shared, mcp bool
	}{{"standalone-cli", false, false}, {"broker-cli", true, false}, {"standalone-mcp", false, true}, {"broker-mcp", true, true}} {
		t.Run(mode.name, func(t *testing.T) {
			work := filepath.Join(d.dir, mode.name)
			if err := os.Mkdir(work, 0700); err != nil {
				t.Fatal(err)
			}
			command := func(args ...string) *exec.Cmd {
				cmd := exec.CommandContext(t.Context(), cli, args...)
				cmd.Dir = work
				cmd.Env = append(append([]string(nil), d.env...), "HOME="+frontendHome, "RDEV_BROKER_SOCKET=", "RDEV_PRINCIPAL_TOKEN=", "RDEV_APPROVAL_TOKEN=", "RDEV_OPERATION_ID=")
				if mode.shared {
					cmd.Env = append(cmd.Env, "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PROJECT_ID="+owner.ProjectID, "RDEV_PRINCIPAL_TOKEN="+token)
				}
				return cmd
			}
			var session *mcp.ClientSession
			if mode.mcp {
				mc := mcp.NewClient(&mcp.Implementation{Name: "phase6-contract-proof", Version: "1"}, nil)
				session, err = mc.Connect(t.Context(), &mcp.CommandTransport{Command: command("serve")}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
			}
			call := func(tool string, args map[string]any, out any) (bool, string) {
				t.Helper()
				ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer cancel()
				r, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: args})
				if err != nil {
					// Schema failures are MCP protocol errors rather than tool results.
					return true, err.Error()
				}
				if r.IsError {
					var detail strings.Builder
					for _, content := range r.Content {
						if text, ok := content.(*mcp.TextContent); ok {
							detail.WriteString(text.Text)
						}
					}
					return true, detail.String()
				}
				data, err := json.Marshal(r.StructuredContent)
				if err != nil || out != nil && json.Unmarshal(data, out) != nil {
					t.Fatalf("invalid %s structured response", tool)
				}
				return false, ""
			}
			approval := func(wire *proto.Request) string {
				if !mode.shared {
					return ""
				}
				return d.approve(owner, wire)
			}
			runCLI := func(approved string, args ...string) ([]byte, string, error) {
				cmd := command(args...)
				cmd.Env = append(cmd.Env, "RDEV_APPROVAL_TOKEN="+approved)
				var stderr bytes.Buffer
				cmd.Stderr = &stderr
				out, err := cmd.Output()
				return out, stderr.String(), err
			}
			marker := mode.name
			argv := []string{"sh", "-c", `echo $$ > "$HOME/$1/$2.pid"; printf began; sleep 30; printf bad > "$HOME/$1/$2.bad"`, "rdev", namespace, marker}
			wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: argv, TimeoutSec: 1}}
			approved := approval(wire)
			started := time.Now()
			if mode.mcp {
				var result proto.ExecResult
				failed, detail := call("rdev_exec", map[string]any{"host": "runtime-host", "argv": argv, "login_shell": false, "timeout_sec": 1, "approval_token": approved}, &result)
				if failed || !result.TimedOut || result.Stdout != "began" {
					t.Fatalf("exec timeout response failed=%t timed_out=%t stdout=%q detail=%s", failed, result.TimedOut, result.Stdout, detail)
				}
			} else {
				out, detail, err := runCLI(approved, append([]string{"exec", "runtime-host", "-no-login", "-timeout", "1", "--"}, argv...)...)
				if err == nil || string(out) != "began" || !strings.Contains(detail, "foreground command timed out") {
					t.Fatalf("CLI exec timeout: %v stdout=%q stderr=%s", err, out, detail)
				}
			}
			if elapsed := time.Since(started); elapsed < time.Second || elapsed > 15*time.Second {
				t.Fatalf("exec timeout elapsed=%s", elapsed)
			}
			if out, err := ssh(fmt.Sprintf("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]);name=%q\npid=int(open(p+'/'+name+'.pid').read());assert not os.path.exists('/proc/'+str(pid)), 'foreground process survived'\nassert not os.path.exists(p+'/'+name+'.bad')\n", marker)); err != nil {
				t.Fatalf("remote foreground termination: %v %s", err, out)
			}
			for _, bad := range []int{-1, proto.MaxTimeoutSeconds + 1} {
				invalidArgv := []string{"sh", "-c", `printf invalid > "$HOME/$1/$2.invalid"`, "rdev", namespace, marker}
				if mode.mcp {
					if failed, detail := call("rdev_exec", map[string]any{"host": "runtime-host", "argv": invalidArgv, "login_shell": false, "timeout_sec": bad}, nil); !failed || strings.Contains(detail, "approval") {
						t.Fatalf("invalid timeout did not fail at argument boundary: failed=%t detail=%s", failed, detail)
					}
				} else if _, detail, err := runCLI("", append([]string{"exec", "runtime-host", "-timeout", strconv.Itoa(bad), "--"}, invalidArgv...)...); err == nil || strings.Contains(detail, "approval") {
					t.Fatalf("invalid timeout did not fail at argument boundary: %v %s", err, detail)
				}
			}
			if out, err := ssh(fmt.Sprintf("import os,sys\nassert not os.path.exists(os.path.expanduser('~/'+sys.argv[1]+%q))\n", "/"+marker+".invalid")); err != nil {
				t.Fatalf("invalid timeout performed a write: %v %s", err, out)
			}
			jobStart := func(wall int) *proto.JobInfo {
				t.Helper()
				argv := []string{"sleep", "60"}
				var resources *proto.ResourceEnvelope
				if wall != 0 {
					resources = &proto.ResourceEnvelope{WallTimeoutSec: wall}
				}
				approved := approval(&proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: argv}, Resources: resources}})
				var info proto.JobInfo
				if mode.mcp {
					args := map[string]any{"host": "runtime-host", "argv": argv, "login_shell": false, "approval_token": approved}
					if resources != nil {
						args["resources"] = resources
					}
					if failed, detail := call("rdev_job_start", args, &info); failed {
						t.Fatalf("job start: %s", detail)
					}
				} else {
					args := []string{"job", "start", "runtime-host", "-no-login"}
					if wall != 0 {
						args = append(args, "-wall-timeout", strconv.Itoa(wall))
					}
					out, detail, err := runCLI(approved, append(append(args, "--"), argv...)...)
					if err != nil || json.Unmarshal(out, &info) != nil {
						t.Fatalf("CLI job start: %v %s", err, detail)
					}
				}
				if info.ID == "" || info.PID == 0 {
					t.Fatal("job start omitted identity")
				}
				return &info
			}
			jobWait := func(id string, seconds int) (proto.JobInfo, bool) {
				t.Helper()
				if mode.mcp {
					var result struct {
						Job      proto.JobInfo `json:"job"`
						TimedOut bool          `json:"timed_out"`
					}
					if failed, detail := call("rdev_job_wait", map[string]any{"host": "runtime-host", "id": id, "timeout_sec": seconds}, &result); failed {
						t.Fatalf("MCP job wait: %s", detail)
					}
					return result.Job, result.TimedOut
				}
				out, detail, err := runCLI("", "job", "wait", "runtime-host", id, "-timeout", strconv.Itoa(seconds))
				if mode.shared {
					var result proto.JobResult
					if json.Unmarshal(out, &result) != nil || result.Info == nil {
						t.Fatalf("broker CLI wait: %v %s", err, detail)
					}
					return *result.Info, result.TimedOut
				}
				var info proto.JobInfo
				if json.Unmarshal(out, &info) != nil || info.ID == "" {
					t.Fatalf("standalone CLI wait: %v %s", err, detail)
				}
				return info, strings.Contains(detail, "still running after")
			}
			job := jobStart(0)
			if job.Effective.WallTimeoutSec != proto.DefaultJobWallTimeoutSeconds {
				t.Fatalf("default wall timeout=%d", job.Effective.WallTimeoutSec)
			}
			started = time.Now()
			info, timedOut := jobWait(job.ID, 1)
			if !timedOut || info.State != proto.JobRunning || info.PID != job.PID || time.Since(started) < time.Second {
				t.Fatalf("observation altered job: timed_out=%t state=%s pid=%d", timedOut, info.State, info.PID)
			}
			if out, err := ssh(fmt.Sprintf("import os\nos.kill(%d,0)\n", job.PID)); err != nil {
				t.Fatalf("wait timeout killed background supervisor: %v %s", err, out)
			}
			wallJob := jobStart(1)
			info, timedOut = jobWait(wallJob.ID, 5)
			if timedOut || info.State == proto.JobRunning || info.ResourceLimit != "wall_timeout" || info.Effective.WallTimeoutSec != 1 {
				t.Fatalf("wall timeout terminal lost: timed_out=%t state=%s resource_limit=%s effective=%d", timedOut, info.State, info.ResourceLimit, info.Effective.WallTimeoutSec)
			}
			// Explicit cleanup exercises the original live job's stop path after
			// both a separate observation timeout and another job's wall timeout.
			approved = approval(&proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: job.ID, Signal: "KILL"}})
			if mode.mcp {
				if failed, detail := call("rdev_job_stop", map[string]any{"host": "runtime-host", "id": job.ID, "signal": "KILL", "approval_token": approved}, nil); failed {
					t.Fatalf("stop survivor: %s", detail)
				}
			} else if _, detail, err := runCLI(approved, "job", "stop", "runtime-host", job.ID, "-signal", "KILL"); err != nil {
				t.Fatalf("stop survivor: %v %s", err, detail)
			}
			for index, localName := range []string{"-leading-local", "space 雪"} {
				payload := "phase6 literal path: " + mode.name + " " + localName
				if err := os.WriteFile(filepath.Join(work, localName), []byte(payload), 0600); err != nil {
					t.Fatal(err)
				}
				// Local operand spelling is preserved. The remote path deliberately
				// retains the existing safe-ASCII boundary used by rsync over SSH.
				remoteSuffix := fmt.Sprintf("/literal-targets/%s-literal-%d", mode.name, index)
				remote := "~/" + namespace + remoteSuffix
				local := localName
				if mode.shared && mode.mcp {
					local = filepath.Join(work, localName) // shared MCP explicitly requires an absolute local path
				}
				var prepared client.SyncResult
				if mode.shared {
					if mode.mcp {
						if failed, detail := call("rdev_sync", map[string]any{"host": "runtime-host", "direction": "push", "local": local, "remote": remote, "prepare": true}, &prepared); failed {
							t.Fatalf("MCP sync prepare: %s", detail)
						}
					} else {
						out, detail, err := runCLI("", "sync", "runtime-host", "push", "-prepare", "--", local, remote)
						if err != nil || json.Unmarshal(out, &prepared) != nil {
							t.Fatalf("CLI sync prepare: %v %s", err, detail)
						}
					}
					if prepared.PlanID == "" || !prepared.ManifestComplete {
						t.Fatal("sync prepare omitted complete plan")
					}
					opts := &client.SyncOptions{Direction: "push", Local: filepath.Join(work, localName), Remote: remote, PlanID: prepared.PlanID}
					result := policyRuntimeRequest(t, adminWire, broker.Request{Owner: runtimeApprovalAdmin(), Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: owner, Operation: "sync.push", Host: "runtime-host", Sync: opts}})
					if !result.OK || result.Approval == nil {
						t.Fatalf("sync approval: %s", result.Error)
					}
					approved = result.Approval.Token
				} else {
					approved = ""
				}
				if mode.mcp {
					if failed, detail := call("rdev_sync", map[string]any{"host": "runtime-host", "direction": "push", "local": local, "remote": remote, "plan_id": prepared.PlanID, "approval_token": approved}, nil); failed {
						t.Fatalf("MCP literal sync: %s", detail)
					}
				} else {
					args := []string{"sync", "runtime-host", "push"}
					if mode.shared {
						args = append(args, "-plan", prepared.PlanID)
					}
					if _, detail, err := runCLI(approved, append(args, "--", local, remote)...); err != nil {
						t.Fatalf("CLI literal sync: %v %s", err, detail)
					}
				}
				if out, err := ssh(fmt.Sprintf("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+%q)\nassert open(p).read()==%q\n", remoteSuffix, payload)); err != nil {
					t.Fatalf("literal sync destination: %v %s", err, out)
				}
			}
			t.Log("real CLI/official MCP SDK + SSH: exec=1 kills process; invalid timeout has no write; wait=1 preserves job; default wall=3600; wall=1 retains terminal resource_limit; dash/space/Unicode sync writes exact payload")
		})
	}
}
