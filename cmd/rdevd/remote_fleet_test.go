package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The three targets below use independent agent state and business directories
// on one authorized SSH machine. This is real multi-target execution evidence;
// it is deliberately not described as three physical hosts or production scale.
type fleetRuntime struct {
	t                     *testing.T
	d                     *runtimeDaemon
	owner, other          broker.Owner
	namespace, cli, token string
	ssh                   func(string) ([]byte, error)
	wire                  *runtimeWire
	inventory             broker.FleetInventory
}

func newFleetRuntime(t *testing.T, gate bool) *fleetRuntime {
	t.Helper()
	d, namespace, ssh := newRemoteRuntime(t)
	f := &fleetRuntime{t: t, d: d, namespace: namespace, ssh: ssh, owner: broker.Owner{ClientID: "fleet-runtime", ProjectID: "phase7"}, other: broker.Owner{ClientID: "fleet-runtime", ProjectID: "other-project"}}
	remote := os.Getenv("RDEV_TEST_REMOTE")
	if remote == "" {
		remote = "service-deploy"
	}
	var hosts []map[string]any
	for i := 1; i <= 3; i++ {
		hosts = append(hosts, map[string]any{"name": fmt.Sprintf("fleet-%d", i), "addr": remote, "remote_dir": fmt.Sprintf("%s/agent%d", namespace, i), "cwd": fmt.Sprintf("~/%s/business%d", namespace, i), "login_shell": false})
		target := fmt.Sprintf("/agent%d", i)
		cleanupRemoteJobSupervisors(t, func(script string) ([]byte, error) {
			return ssh("import sys\nsys.argv[1]+=" + fmt.Sprintf("%q", target) + "\n" + script)
		})
	}
	// A second alias for exactly the same connection+session must not dispatch twice.
	duplicate := map[string]any{}
	for k, v := range hosts[0] {
		duplicate[k] = v
	}
	duplicate["name"] = "fleet-duplicate"
	hosts = append(hosts, duplicate)
	data, _ := json.Marshal(map[string]any{"hosts": hosts})
	if err := os.WriteFile(filepath.Join(d.dir, "hosts.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if out, err := ssh("import os,sys\nroot=os.path.expanduser('~/'+sys.argv[1])\nfor i in range(1,4):os.makedirs(root+'/business'+str(i),mode=0o700)\n"); err != nil {
		t.Fatalf("Fleet fixture: %v %s", err, out)
	}
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "policy.grant"); err != nil {
		t.Fatal(err)
	}
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{f.owner, f.other} {
		for _, op := range []string{"fleet.plan", "fleet.execute", "fleet.approve", "fleet.status", "fleet.results", "fleet.list", "fleet.pause", "fleet.resume", "fleet.cancel", "fleet.retry", "fleet.reconcile", "audit_query", "mutation.status"} {
			if err := policy.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, op := range []string{"fleet.inventory.import", "fleet.inventory.update", "fleet.inventory.list"} {
		if err := policy.Grant(f.owner.Key(), op); err != nil {
			t.Fatal(err)
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	if gate {
		if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(mutationSSHWrapper), 0700); err != nil {
			t.Fatal(err)
		}
		d.env = append(d.env, "RDEV_MUTATION_GATE="+filepath.Join(d.dir, "fleet-gate"))
	}
	f.cli = os.Getenv("RDEV_TEST_CLI_BINARY")
	if f.cli == "" {
		f.cli = filepath.Join(d.dir, "rdev")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", f.cli, "./cmd/rdev")
		build.Dir = repoRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build Fleet CLI: %v %s", err, out)
		}
	}
	d.start()
	f.connect()
	inventory := f.call("fleet.inventory.list", &broker.FleetRequest{}, "")
	if !inventory.OK || inventory.Inventory == nil {
		t.Fatalf("inventory list: %s", inventory.Error)
	}
	inventory = f.call("fleet.inventory.import", &broker.FleetRequest{Revision: inventory.Inventory.Revision}, "")
	if !inventory.OK || inventory.Inventory == nil || len(inventory.Inventory.Records) != 3 {
		t.Fatalf("inventory import/dedup: %s", inventory.Error)
	}
	f.inventory = *inventory.Inventory
	admin := runtimeApprovalAdmin()
	adminWire := d.dial(admin, d.token(admin, "10m"), true)
	for _, host := range f.inventory.Records {
		for _, op := range []string{proto.OpJobStart, proto.OpJobStatus} {
			r := policyRuntimeRequest(t, adminWire, broker.Request{Owner: admin, Operation: "policy.grant", GrantOwner: f.owner, GrantHost: host.HostID, GrantCapability: broker.CapabilityForOperation(op), GrantOperation: op})
			if !r.OK {
				t.Fatalf("Fleet exact host policy grant: %s", r.Error)
			}
		}
	}
	return f
}

func (f *fleetRuntime) connect() {
	f.token = f.d.token(f.owner, "10m")
	f.wire = f.d.dial(f.owner, f.token, true)
}
func (f *fleetRuntime) call(op string, q *broker.FleetRequest, approval string) broker.Response {
	f.t.Helper()
	return policyRuntimeRequest(f.t, f.wire, broker.Request{Owner: f.owner, Operation: op, Fleet: q, Approval: approval})
}
func (f *fleetRuntime) command(owner broker.Owner, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(f.t.Context(), f.cli, args...)
	cmd.Dir = f.d.dir
	cmd.Env = append(append([]string{}, f.d.env...), "RDEV_BROKER_SOCKET="+f.d.socket, "RDEV_CLIENT_ID="+owner.ClientID, "RDEV_PROJECT_ID="+owner.ProjectID, "RDEV_PRINCIPAL_TOKEN="+f.d.token(owner, "10m"), "RDEV_APPROVAL_TOKEN=", "RDEV_OPERATION_ID=")
	return cmd
}
func (f *fleetRuntime) cliCall(out any, wantExit int, args ...string) {
	f.t.Helper()
	cmd := f.command(f.owner, append([]string{"fleet"}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	exit := 0
	if err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			exit = e.ExitCode()
		} else {
			f.t.Fatal(err)
		}
	}
	if exit != wantExit || json.Unmarshal(data, out) != nil {
		f.t.Fatalf("Fleet CLI %s: exit=%d want=%d stderr=%s output=%s", args[0], exit, wantExit, stderr.String(), data)
	}
}

// Each isolated supervisor executable identifies its own test target. The spec
// uses an explicit common cwd; production Fleet does not inherit session cwd.
const fleetTargetBusiness = `agent=$(readlink "/proc/$PPID/exe"); agent=${agent%/*}; agent=${agent##*/}; cd "business${agent#agent}" || exit 91; `

func (f *fleetRuntime) spec(marker, selector string, parallel int) *broker.FleetSpec {
	return &broker.FleetSpec{Selector: selector, Operation: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", fleetTargetBusiness + `printf x >> "$1"; if [ -f fail ]; then exit 7; fi`, "fleet-proof", marker}, Cwd: "~/" + f.namespace}, Resources: &proto.ResourceEnvelope{WallTimeoutSec: 60}}, Rollout: broker.FleetRollout{Strategy: "all_at_once", MaxParallel: parallel}}
}
func (f *fleetRuntime) plan(spec *broker.FleetSpec) *broker.FleetPlan {
	f.t.Helper()
	r := f.call("fleet.plan", &broker.FleetRequest{Spec: spec}, "")
	if !r.OK || r.Fleet == nil {
		f.t.Fatalf("Fleet plan: %s", r.Error)
	}
	return r.Fleet
}
func (f *fleetRuntime) approve(plan *broker.FleetPlan) string {
	f.t.Helper()
	r := f.call("fleet.approve", &broker.FleetRequest{PlanID: plan.PlanID, Digest: plan.Digest, TTLSeconds: 120}, "")
	if !r.OK || r.Approval == nil {
		f.t.Fatalf("Fleet approve: %s", r.Error)
	}
	return r.Approval.Token
}
func (f *fleetRuntime) execute(plan *broker.FleetPlan, approval string) {
	f.t.Helper()
	r := f.call("fleet.execute", &broker.FleetRequest{PlanID: plan.PlanID, Digest: plan.Digest}, approval)
	if !r.OK {
		f.t.Fatalf("Fleet execute: %s", r.Error)
	}
}
func (f *fleetRuntime) await(plan string, predicate func(*broker.FleetPlan) bool) *broker.FleetPlan {
	f.t.Helper()
	var last *broker.FleetPlan
	awaitRuntime(f.t, 30*time.Second, "Fleet persisted result", func() bool {
		r := f.call("fleet.status", &broker.FleetRequest{PlanID: plan}, "")
		if !r.OK || r.Fleet == nil {
			f.t.Fatalf("Fleet query: %s", r.Error)
		}
		last = r.Fleet
		return predicate(last)
	})
	return last
}
func (f *fleetRuntime) proof(script string) {
	f.t.Helper()
	if out, err := f.ssh("import os,sys\nroot=os.path.expanduser('~/'+sys.argv[1])\n" + script); err != nil {
		f.t.Fatalf("Fleet real side-effect assertion: %v %s", err, out)
	}
}

func TestRemoteFleetFrontendsAndRetry(t *testing.T) {
	f := newFleetRuntime(t, false)
	f.proof("open(root+'/business2/fail','w').close()\n")
	spec := f.spec("counter", "alias=fleet-3,fleet-duplicate,fleet-2,fleet-1", 2)
	path := filepath.Join(f.d.dir, "fleet-spec.json")
	data, _ := json.Marshal(spec)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	var plan broker.FleetPlan
	f.cliCall(&plan, 0, "plan", "-file", path)
	if len(plan.Runs) != 3 {
		t.Fatal("preview failed HostID alias dedup")
	}
	for i, run := range plan.Runs {
		if run.Attempt != 1 || run.OperationID == "" || i > 0 && plan.Runs[i-1].Host.HostID >= run.Host.HostID {
			t.Fatal("preview missing stable sorted HostRun identity")
		}
	}
	if r := f.call("fleet.execute", &broker.FleetRequest{PlanID: plan.PlanID, Digest: plan.Digest}, ""); r.OK {
		t.Fatal("all real job mutations require explicit Fleet approval")
	}
	f.proof("for i in range(1,4):assert not os.path.exists(root+'/business'+str(i)+'/counter')\n")
	var approval broker.Approval
	f.cliCall(&approval, 0, "approve", plan.PlanID, "-digest", plan.Digest, "-ttl", "120")
	if r := f.call("fleet.execute", &broker.FleetRequest{PlanID: plan.PlanID, Digest: strings.Repeat("0", 64)}, approval.Token); r.OK {
		t.Fatal("Fleet approval accepted a substituted operation/target digest")
	}
	if r := f.call("fleet.execute", &broker.FleetRequest{PlanID: plan.PlanID, Digest: plan.Digest, Spec: f.spec("forbidden", "all", 3)}, approval.Token); r.OK {
		t.Fatal("Fleet execution accepted a replacement operation spec")
	}
	otherWire := f.d.dial(f.other, f.d.token(f.other, "10m"), true)
	otherExec := policyRuntimeRequest(t, otherWire, broker.Request{Owner: f.other, Operation: "fleet.execute", Fleet: &broker.FleetRequest{PlanID: plan.PlanID, Digest: plan.Digest}, Approval: approval.Token})
	if otherExec.OK || otherExec.Fleet != nil {
		t.Fatal("Fleet approval transferred to another project")
	}
	var started broker.FleetPlan
	f.cliCall(&started, 0, "execute", plan.PlanID, "-digest", plan.Digest, "-approval", approval.Token)
	f.execute(&plan, approval.Token)
	// A frontend process exits immediately after admission; jobs remain durable.
	final := f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 2 && p.Counts["failed"] == 1 })
	if final.ExitStatus != 1 {
		t.Fatal("partial failed plan exit status")
	}
	var cliResult broker.FleetPlan
	f.cliCall(&cliResult, 1, "results", plan.PlanID, "-limit", "2")
	if len(cliResult.Runs) != 2 || cliResult.Total != 3 || cliResult.NextOffset != 2 {
		t.Fatal("bounded CLI results lost pagination")
	}
	f.proof("for i in range(1,4):assert open(root+'/business'+str(i)+'/counter').read()=='x'\n")
	client := mcp.NewClient(&mcp.Implementation{Name: "phase7-fleet-runtime", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.CommandTransport{Command: f.command(f.owner, "serve")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	callMCP := func(action string, q *broker.FleetRequest, token string) broker.Response {
		t.Helper()
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_fleet", Arguments: map[string]any{"action": action, "request": q, "approval_token": token}})
		if err != nil || result.IsError {
			t.Fatalf("MCP Fleet %s failed: %v %+v", action, err, result)
		}
		data, _ := json.Marshal(result.StructuredContent)
		var got struct {
			Plan     *broker.FleetPlan `json:"plan"`
			Plans    *broker.FleetPage `json:"plans"`
			Approval *broker.Approval  `json:"approval"`
		}
		if json.Unmarshal(data, &got) != nil {
			t.Fatal("MCP Fleet structured result")
		}
		return broker.Response{Fleet: got.Plan, Fleets: got.Plans, Approval: got.Approval}
	}
	mcpResult := callMCP("results", &broker.FleetRequest{PlanID: plan.PlanID}, "").Fleet
	if mcpResult == nil || mcpResult.ExitStatus != 1 || mcpResult.Counts["success"] != 2 {
		t.Fatal("MCP aggregate differs from CLI")
	}
	var failed string
	for _, run := range final.Runs {
		if run.State == "failed" {
			failed = run.Host.HostID
		}
	}
	f.proof("os.remove(root+'/business2/fail')\n")
	child := callMCP("retry", &broker.FleetRequest{PlanID: plan.PlanID, HostIDs: []string{failed}}, "").Fleet
	if child == nil || child.ParentPlanID != plan.PlanID || len(child.Runs) != 1 || child.Runs[0].Host.HostID != failed || child.Runs[0].Attempt != 2 {
		t.Fatal("retry did not fix exact attempt/parent/target subset")
	}
	childApproval := callMCP("approve", &broker.FleetRequest{PlanID: child.PlanID, Digest: child.Digest, TTLSeconds: 120}, "").Approval
	if childApproval == nil {
		t.Fatal("MCP approval missing")
	}
	callMCP("execute", &broker.FleetRequest{PlanID: child.PlanID, Digest: child.Digest}, childApproval.Token)
	f.await(child.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 1 })
	queryOnly := callMCP("plan", &broker.FleetRequest{Spec: f.spec("must-not-run", "all", 2)}, "").Fleet
	if queryOnly == nil {
		t.Fatal("MCP preview missing")
	}
	if canceled := callMCP("cancel", &broker.FleetRequest{PlanID: queryOnly.PlanID}, "").Fleet; canceled == nil || canceled.Counts["canceled"] != 3 {
		t.Fatal("MCP canceled preview admitted an operation")
	}
	if page := callMCP("list", &broker.FleetRequest{Limit: 2}, "").Fleets; page == nil || len(page.Plans) != 2 || page.NextOffset != 2 {
		t.Fatal("MCP Fleet list pagination")
	}
	f.proof("assert open(root+'/business1/counter').read()=='x'\nassert open(root+'/business2/counter').read()=='xx'\nassert open(root+'/business3/counter').read()=='x'\n")
	f.proof("for i in range(1,4):\n for name in ('forbidden','must-not-run'):assert not os.path.exists(root+'/business'+str(i)+'/'+name)\n")
	if r := f.call("fleet.retry", &broker.FleetRequest{PlanID: plan.PlanID, HostIDs: []string{failed}}, ""); r.OK {
		t.Fatal("concurrent/repeated retry dispatched the same HostRun twice")
	}
	denied := f.command(f.other, "fleet", "status", plan.PlanID)
	if out, err := denied.Output(); err == nil || len(out) > 0 {
		t.Fatal("other project read Fleet plan through CLI")
	}
	otherClient := mcp.NewClient(&mcp.Implementation{Name: "phase7-fleet-other", Version: "1"}, nil)
	otherSession, err := otherClient.Connect(t.Context(), &mcp.CommandTransport{Command: f.command(f.other, "serve")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer otherSession.Close()
	otherResult, err := otherSession.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_fleet", Arguments: map[string]any{"action": "status", "request": map[string]any{"plan_id": plan.PlanID}}})
	if err != nil || !otherResult.IsError {
		t.Fatal("other project read Fleet plan through MCP")
	}
	t.Log("real CLI + official MCP SDK, 3 isolated targets on one SSH endpoint: alias dedup/snapshot, approval/no early effects, partial failure, bounded results/exit status, precise subset retry with counters x/xx/x, exact project isolation")
}

func TestRemoteFleetCrashRecovery(t *testing.T) {
	f := newFleetRuntime(t, true)
	gate := filepath.Join(f.d.dir, "fleet-gate")
	t.Cleanup(func() { _ = os.Remove(gate); _ = os.Remove(gate + ".unavailable") })
	plan := f.plan(f.spec("crash-counter", "alias=fleet-1", 1))
	operationID := plan.Runs[0].OperationID
	hold, _ := json.Marshal(map[string]string{"operation_id": operationID})
	if err := os.WriteFile(gate, hold, 0600); err != nil {
		t.Fatal(err)
	}
	approved := f.approve(plan)
	f.execute(plan, approved)
	awaitRuntime(t, 15*time.Second, "real Fleet job mutation ACK held", func() bool {
		data, err := os.ReadFile(gate + ".entered")
		return err == nil && strings.Contains(string(data), operationID)
	})
	f.proof("assert open(root+'/business1/crash-counter').read()=='x'\n")
	f.d.stop(syscall.SIGKILL)
	_ = os.Remove(gate)
	if err := os.WriteFile(gate+".unavailable", nil, 0600); err != nil {
		t.Fatal(err)
	}
	f.d.start()
	f.connect()
	uncertain := f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["ambiguous"] == 1 })
	if uncertain.Runs[0].OperationID != operationID || uncertain.Runs[0].Attempt != 1 {
		t.Fatal("recovery replaced operation identity")
	}
	if r := f.call("fleet.retry", &broker.FleetRequest{PlanID: plan.PlanID, HostIDs: []string{plan.Runs[0].Host.HostID}}, ""); r.OK {
		t.Fatal("ambiguous mutation received automatic new attempt")
	}
	// Failed result lookup must preserve ambiguity, never authorize a new send.
	f.call("fleet.reconcile", &broker.FleetRequest{PlanID: plan.PlanID}, "")
	f.proof("assert open(root+'/business1/crash-counter').read()=='x'\n")
	if err := os.Remove(gate + ".unavailable"); err != nil {
		t.Fatal(err)
	}
	r := f.call("fleet.reconcile", &broker.FleetRequest{PlanID: plan.PlanID}, "")
	if !r.OK {
		t.Fatalf("Fleet reconcile: %s", r.Error)
	}
	result := f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 1 })
	if result.Runs[0].OperationID != operationID || result.Runs[0].Attempt != 1 || result.Runs[0].JobID == "" {
		t.Fatal("reconciliation lost mutation/job identity")
	}
	f.d.stop(syscall.SIGTERM)
	f.d.start()
	f.connect()
	f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 1 })
	f.proof("assert open(root+'/business1/crash-counter').read()=='x'\nfor i in (2,3):assert not os.path.exists(root+'/business'+str(i)+'/crash-counter')\n")
	t.Log("real job_start mutation ACK loss + broker SIGKILL: unavailable query preserves ambiguous, explicit retry rejected, query-only reconcile reaches same operation/job/attempt, graceful second restart retains success, counter exactly once")
}

func TestRemoteFleetPauseCancelAndCanary(t *testing.T) {
	f := newFleetRuntime(t, false)
	spec := f.spec("lifecycle", "all", 1)
	spec.Job.Spec.Argv = []string{"sh", "-c", fleetTargetBusiness + `printf x >> lifecycle; sleep 4; printf done >> lifecycle`}
	plan := f.plan(spec)
	approved := f.approve(plan)
	f.execute(plan, approved)
	f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["running"] == 1 })
	var paused broker.FleetPlan
	f.cliCall(&paused, 0, "pause", plan.PlanID)
	if paused.State != "paused" {
		t.Fatal("CLI pause did not persist pause state")
	}
	// Pause survives client process exit and lets the already submitted background
	// job complete without admitting another host.
	done := f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 1 })
	if done.State != "paused" || done.Counts["pending"] != 2 {
		t.Fatal("pause dispatched another target or canceled admitted job")
	}
	var resumed broker.FleetPlan
	f.cliCall(&resumed, 0, "resume", plan.PlanID)
	f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["running"] == 1 && p.Counts["success"] == 1 })
	var canceled broker.FleetPlan
	f.cliCall(&canceled, 0, "cancel", plan.PlanID)
	f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 2 && p.Counts["canceled"] == 1 })
	f.proof("values=[]\nfor i in range(1,4):\n p=root+'/business'+str(i)+'/lifecycle'\n if os.path.exists(p):values.append(open(p).read())\nassert sorted(values)==['xdone','xdone'],values\n")
	canarySpec := f.spec("canary", "all", 2)
	canarySpec.Job.Spec.Argv = []string{"sh", "-c", fleetTargetBusiness + `printf x >> canary; exit 7`}
	canarySpec.Rollout = broker.FleetRollout{Strategy: "canary", MaxParallel: 2, Canary: 1, WaveSize: 2}
	canary := f.plan(canarySpec)
	f.execute(canary, f.approve(canary))
	stopped := f.await(canary.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["failed"] == 1 && p.State == "paused" })
	if stopped.Counts["pending"] != 2 {
		t.Fatal("failed canary entered another wave")
	}
	f.proof("values=[]\nfor i in range(1,4):\n p=root+'/business'+str(i)+'/canary'\n if os.path.exists(p):values.append(open(p).read())\nassert values==['x'],values\n")
	if r := f.call("fleet.cancel", &broker.FleetRequest{PlanID: canary.PlanID}, ""); !r.OK {
		t.Fatal("failed canary cancel rejected")
	}
	t.Log("real jobs on 3 independent agent targets: max_parallel=1, pause retained after CLI exits, resume preserves successes, batch cancel stops pending admission while background jobs finish, failed canary prevents next wave")
}

func TestRemoteFleetMixedWorkload(t *testing.T) {
	f := newFleetRuntime(t, false)
	admin := runtimeApprovalAdmin()
	adminWire := f.d.dial(admin, f.d.token(admin, "10m"), true)
	syncOwner := broker.Owner{ClientID: f.other.ClientID, ProjectID: "mixed-sync"}
	for _, op := range []string{proto.OpExec, proto.OpJobStart, proto.OpJobStatus, proto.OpJobWait, "sync.push"} {
		principal := f.other
		if op == "sync.push" {
			principal = syncOwner
		}
		r := policyRuntimeRequest(t, adminWire, broker.Request{Owner: admin, Operation: "policy.grant", GrantOwner: principal, GrantHost: "fleet-1", GrantCapability: broker.CapabilityForOperation(op), GrantOperation: op})
		if !r.OK {
			t.Fatalf("mixed ordinary permission: %s", r.Error)
		}
	}
	otherWire := f.d.dial(f.other, f.d.token(f.other, "10m"), true)
	ordinary := func(req broker.Request) broker.Response {
		t.Helper()
		req.Owner = f.other
		req.Host = "fleet-1"
		return policyRuntimeRequest(t, otherWire, req)
	}
	approve := func(operation string, wire *proto.Request, sync *client.SyncOptions) string {
		t.Helper()
		principal := f.other
		if operation == "sync.push" {
			principal = syncOwner
		}
		r := policyRuntimeRequest(t, adminWire, broker.Request{Owner: admin, Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: principal, Operation: operation, Host: "fleet-1", Wire: wire, Sync: sync, TTL: time.Minute}})
		if !r.OK || r.Approval == nil {
			t.Fatalf("mixed ordinary approval: %s", r.Error)
		}
		return r.Approval.Token
	}
	jobWire := &proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "sleep 8; printf ordinary-job"}}, Resources: &proto.ResourceEnvelope{WallTimeoutSec: 30}}}
	job := ordinary(broker.Request{Operation: proto.OpJobStart, Wire: jobWire, Approval: approve(proto.OpJobStart, jobWire, nil)})
	if !job.OK || job.Wire == nil || job.Wire.Job == nil || job.Wire.Job.Info == nil {
		t.Fatalf("ordinary background job: %s", job.Error)
	}
	jobID := job.Wire.Job.Info.ID
	payload := bytes.Repeat([]byte{0, 1, 127, 255, 13, 10}, 2<<20)
	source := filepath.Join(f.d.dir, "mixed-source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	target := "~/" + f.namespace + "/business1/mixed-transfer/"
	syncWire := f.d.dial(syncOwner, f.d.token(syncOwner, "10m"), true)
	prep := policyRuntimeRequest(t, syncWire, broker.Request{Owner: syncOwner, Host: "fleet-1", Operation: "sync.push", Sync: &client.SyncOptions{Direction: "push", Local: source + "/", Remote: target, Prepare: true, DryRun: true}})
	if !prep.OK || prep.Sync == nil || prep.Sync.PlanID == "" {
		t.Fatalf("mixed retained sync prepare: %s", prep.Error)
	}
	options := &client.SyncOptions{Direction: "push", Local: source + "/", Remote: target, PlanID: prep.Sync.PlanID}
	syncApproval := approve("sync.push", nil, options)
	execWire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "printf ordinary-exec; sleep 1"}, TimeoutSec: 10}}
	execApproval := approve(proto.OpExec, execWire, nil)
	fleetSpec := f.spec("mixed-fleet", "all", 3)
	fleetSpec.Job.Spec.Argv = []string{"sh", "-c", fleetTargetBusiness + `printf x >> mixed-fleet; sleep 6; printf done >> mixed-fleet`}
	plan := f.plan(fleetSpec)
	f.execute(plan, f.approve(plan))
	f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["running"] >= 1 })
	type processResult struct {
		out []byte
		err error
	}
	syncDone, execDone := make(chan processResult, 1), make(chan processResult, 1)
	syncCmd := f.command(syncOwner, "sync", "fleet-1", "push", source+"/", target, "-plan", prep.Sync.PlanID)
	syncCmd.Env = append(syncCmd.Env, "RDEV_APPROVAL_TOKEN="+syncApproval)
	execCmd := f.command(f.other, "exec", "fleet-1", "-no-login", "-timeout", "10", "--", "sh", "-c", "printf ordinary-exec; sleep 1")
	execCmd.Env = append(execCmd.Env, "RDEV_APPROVAL_TOKEN="+execApproval)
	go func() { out, err := syncCmd.CombinedOutput(); syncDone <- processResult{out, err} }()
	go func() { out, err := execCmd.CombinedOutput(); execDone <- processResult{out, err} }()
	wait := ordinary(broker.Request{Operation: proto.OpJobWait, Wire: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobID, WaitTimeoutSec: 1}}})
	if !wait.OK || wait.Wire == nil || wait.Wire.Job == nil || !wait.Wire.Job.TimedOut || wait.Wire.Job.Info == nil || wait.Wire.Job.Info.State != proto.JobRunning {
		t.Fatalf("ordinary observation budget affected job under Fleet load: %s", wait.Error)
	}
	if r := f.call("fleet.pause", &broker.FleetRequest{PlanID: plan.PlanID}, ""); !r.OK {
		t.Fatal("mixed Fleet pause")
	}
	if r := f.call("fleet.cancel", &broker.FleetRequest{PlanID: plan.PlanID}, ""); !r.OK {
		t.Fatal("mixed Fleet cancel")
	}
	status := ordinary(broker.Request{Operation: proto.OpJobStatus, Wire: &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: jobID}}})
	if !status.OK || status.Wire == nil || status.Wire.Job == nil || status.Wire.Job.Info == nil || status.Wire.Job.Info.State != proto.JobRunning {
		t.Fatal("Fleet cancel canceled other owner's ordinary job")
	}
	select {
	case r := <-execDone:
		if r.err != nil || string(r.out) != "ordinary-exec" {
			t.Fatalf("ordinary exec starved/altered by Fleet: %v %s", r.err, r.out)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ordinary exec did not progress")
	}
	select {
	case r := <-syncDone:
		if r.err != nil {
			t.Fatalf("retained sync starved/canceled by Fleet: %v %s", r.err, r.out)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("retained sync did not progress")
	}
	final := f.await(plan.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["running"] == 0 && p.Counts["dispatching"] == 0 })
	if final.State != "canceled" || final.Counts["success"] < 1 || final.Counts["canceled"] < 1 {
		t.Fatalf("mixed Fleet outcome: %s %v", final.State, final.Counts)
	}
	completed := ordinary(broker.Request{Operation: proto.OpJobWait, Wire: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobID, WaitTimeoutSec: 10}}})
	if !completed.OK || completed.Wire == nil || completed.Wire.Job == nil || completed.Wire.Job.Info == nil || completed.Wire.Job.Info.State != proto.JobExited || completed.Wire.Job.Info.ExitCode != 0 {
		t.Fatalf("ordinary job did not finish after Fleet cancel: %s", completed.Error)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(payload))
	f.proof(fmt.Sprintf("import hashlib\np=root+'/business1/mixed-transfer/payload'\nassert os.path.getsize(p)==%d\nassert hashlib.sha256(open(p,'rb').read()).hexdigest()==%q\nvalues=[]\nfor i in range(1,4):\n p=root+'/business'+str(i)+'/mixed-fleet'\n if os.path.exists(p):values.append(open(p).read())\nassert len(values)==%d and all(v=='xdone' for v in values),values\n", len(payload), digest, final.Counts["success"]))
	t.Logf("real Fleet + two other owners (ordinary CLI exec/job wait/status and retained CLI sync): all progressed; 1s observer timeout kept job alive; Fleet pause/cancel isolated; %d exact payload bytes and SHA-256 matched; detached Fleet and ordinary jobs finished", len(payload))
}
