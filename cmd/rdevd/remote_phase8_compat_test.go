package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Every case owns its ControlMaster sockets, including masters created inside
// the standalone CLI's private mount namespace. No global socket is inspected
// or closed. Exiting the master also releases that child mount namespace.
func phase8ControlScope(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "rdev-compat-")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", root)
	t.Cleanup(func() {
		phase8CloseControls(t, root)
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	return root
}

func phase8CloseControls(t *testing.T, root string) {
	t.Helper()
	dir := filepath.Join(root, "rdev-ctl")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal("cannot inspect private compatibility control sockets")
	}
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSocket == 0 {
			t.Fatal("unexpected private compatibility control object")
		}
		args := []string{}
		if config := os.Getenv("RDEV_TEST_SSH_CONFIG"); config != "" {
			args = append(args, "-F", config)
		}
		args = append(args, "-S", path, "-O")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		check := exec.CommandContext(ctx, "ssh", append(append([]string{}, args...), "check", os.Getenv("RDEV_TEST_REMOTE"))...)
		data, err := check.CombinedOutput()
		match := regexp.MustCompile(`pid=(\d+)`).FindSubmatch(data)
		if err != nil || len(match) != 2 {
			t.Fatal("private compatibility master identity unconfirmed")
		}
		pid, _ := strconv.Atoi(string(match[1]))
		startIdentity := func() string {
			stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
			if err != nil {
				return ""
			}
			i := strings.LastIndexByte(string(stat), ')')
			if i < 0 {
				return ""
			}
			fields := strings.Fields(string(stat[i+1:]))
			if len(fields) < 20 {
				return ""
			}
			return fields[19]
		}
		start := startIdentity()
		if start == "" {
			t.Fatal("private compatibility master process identity missing")
		}
		stop := exec.CommandContext(ctx, "ssh", append(append([]string{}, args...), "exit", os.Getenv("RDEV_TEST_REMOTE"))...)
		if err := stop.Run(); err != nil {
			t.Fatal("private compatibility master exit was not acknowledged")
		}
		awaitRuntime(t, 5*time.Second, "private compatibility master exit", func() bool {
			_, err := os.Lstat(path)
			return os.IsNotExist(err) && startIdentity() != start
		})
	}
}

func phase8Artifacts(t *testing.T) (current, previous, currentAgents, previousAgents string) {
	t.Helper()
	current = os.Getenv("RDEV_TEST_CURRENT_DAEMON_BINARY")
	previous = os.Getenv("RDEV_TEST_PREDECESSOR_DAEMON_BINARY")
	currentAgents = os.Getenv("RDEV_TEST_CURRENT_AGENT_DIR")
	previousAgents = os.Getenv("RDEV_TEST_PREDECESSOR_AGENT_DIR")
	if current == "" || previous == "" || currentAgents == "" || previousAgents == "" || os.Getenv("RDEV_TEST_CLI_BINARY") == "" {
		t.Skip("actual candidate and predecessor executable paths required")
	}
	return
}

func phase8Select(d *runtimeDaemon, binary, agents string) {
	d.bin = binary
	for i, arg := range d.extraArgs {
		if arg == "-agent-dir" {
			d.extraArgs[i+1] = agents
		}
	}
}

func phase8Digest(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Inspect the installed file and the actual serving inode separately. A success
// response with an unintended automatic agent replacement is not this matrix.
func phase8AgentIdentity(t *testing.T, ssh func(string) ([]byte, error), agents string, pid int) {
	t.Helper()
	want := phase8Digest(t, filepath.Join(agents, "rdev-agent-linux-amd64"))
	script := fmt.Sprintf("import os,sys,hashlib\np=os.path.expanduser('~/'+sys.argv[1]+'/rdev-agent')\nassert hashlib.sha256(open(p,'rb').read()).hexdigest()==%q\n", want)
	if pid > 0 {
		script += fmt.Sprintf("assert hashlib.sha256(open('/proc/%d/exe','rb').read()).hexdigest()==%q\n", pid, want)
	}
	if _, err := ssh(script); err != nil {
		t.Fatal("actual installed/serving agent bytes differ from selected matrix component")
	}
}

func phase8Seed(t *testing.T, d *runtimeDaemon, owner broker.Owner, binary, agents string) {
	t.Helper()
	phase8Select(d, binary, agents)
	d.start()
	w := d.dial(owner, d.token(owner, "5m"), true)
	r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if !r.OK || r.Wire == nil || r.Wire.Ping == nil {
		t.Fatal("predecessor installer did not seed isolated matrix agent")
	}
	w.Close()
	d.stop(syscall.SIGTERM)
}

func TestRemotePhase8SharedCompatibility(t *testing.T) {
	_, previous, _, previousAgents := phase8Artifacts(t)
	phase8ControlScope(t)
	d, namespace, ssh := newRemoteRuntime(t)
	cleanupRemoteJobSupervisors(t, ssh)
	selectedDaemon, selectedAgents := d.bin, os.Getenv("RDEV_TEST_AGENT_DIR")
	owner := broker.Owner{ClientID: "phase8-mixed", ProjectID: "allowed"}
	other := broker.Owner{ClientID: owner.ClientID, ProjectID: "denied"}
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{proto.OpPing, proto.OpList, proto.OpJobStart, proto.OpJobWait, proto.OpCapabilityProbe} {
		if err := policy.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
			t.Fatal(err)
		}
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	// New installers are not claimed to install a legacy candidate that predates
	// their installer interface. Seed using the actual predecessor, then verify
	// whether the selected broker can reuse precisely that existing agent.
	phase8Seed(t, d, owner, previous, selectedAgents)
	phase8Select(d, selectedDaemon, selectedAgents)
	d.start()
	w := d.dial(owner, d.token(owner, "5m"), true)
	ping := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	canServe := selectedAgents != previousAgents
	pid := 0
	if canServe {
		if !ping.OK || ping.Wire == nil || ping.Wire.Ping == nil {
			t.Fatal("mixed broker/agent handshake or reuse rejected")
		}
		pid = ping.Wire.Ping.PID
		probe := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpCapabilityProbe, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpCapabilityProbe}})
		if !probe.OK || probe.Wire == nil || probe.Wire.Capability == nil {
			t.Fatal("actual agent capability probe failed")
		}
		features := map[proto.Feature]bool{}
		for _, feature := range probe.Wire.Capability.Features {
			features[feature] = true
		}
		if !features[proto.FeatureJobResourceEnvelope] || !features[proto.FeatureDurableJobStart] {
			t.Fatal("actual agent omitted required new-job features")
		}
	} else if ping.OK || ping.Wire != nil || ping.ErrorEnvelope == nil || ping.ErrorEnvelope.Code != proto.CodeUnsupportedFeature || ping.ErrorEnvelope.ExecutionState != proto.StateNotSent {
		t.Fatal("new broker accepted a legacy artifact without its required release identity")
	}
	phase8AgentIdentity(t, ssh, selectedAgents, pid)
	wire := &proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf once >> "$HOME/$1/mixed-marker"`, "compat", namespace}}, Resources: &proto.ResourceEnvelope{WallTimeoutSec: 30}}}
	if r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire}); r.OK || canServe && !strings.Contains(r.Error, "approval required") {
		t.Fatal("mixed combination did not enforce exact approval before dispatch")
	}
	if _, err := ssh("import os,sys\nassert not os.path.exists(os.path.expanduser('~/'+sys.argv[1]+'/mixed-marker'))\n"); err != nil {
		t.Fatal("approval rejection produced a business side effect")
	}
	r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire, Approval: d.approve(owner, wire)})
	if canServe {
		if !r.OK || r.Wire == nil || r.Wire.Job == nil || r.Wire.Job.Info == nil {
			t.Fatal("mixed combination failed resource-bounded durable job_start")
		}
		job := r.Wire.Job.Info
		wait := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpJobWait, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: job.ID, WaitTimeoutSec: 5}}})
		if !wait.OK || wait.Wire == nil || wait.Wire.Job == nil || wait.Wire.Job.Info == nil || wait.Wire.Job.Info.State != proto.JobExited {
			t.Fatal("mixed combination lost terminal job observation")
		}
	} else if r.OK || r.ErrorEnvelope == nil || r.ErrorEnvelope.Code != proto.CodeUnsupportedFeature || r.ErrorEnvelope.ExecutionState != proto.StateNotSent {
		t.Fatal("legacy artifact rejection lost before-dispatch semantics")
	}
	trap := filepath.Join(d.dir, "frontend-trap")
	if err := os.Mkdir(trap, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(trap, "fallback")
	for _, tool := range []string{"ssh", "rsync"} {
		if err := os.WriteFile(filepath.Join(trap, tool), []byte("#!/bin/sh\n: > \"$RDEV_COMPAT_FALLBACK\"\nexit 99\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, who := range []broker.Owner{owner, other} {
		command := func(args ...string) *exec.Cmd {
			cmd := exec.CommandContext(t.Context(), os.Getenv("RDEV_TEST_CLI_BINARY"), args...)
			cmd.Dir = d.dir
			cmd.Env = append(append([]string{}, d.env...), "PATH="+trap+":"+os.Getenv("PATH"), "RDEV_COMPAT_FALLBACK="+marker, "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+who.ClientID, "RDEV_PROJECT_ID="+who.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(who, "5m"))
			return cmd
		}
		// A fresh broker deterministically clears dial backoff from the prior
		// expected artifact refusal; no timing sleeps select the error under test.
		if !canServe && who == owner {
			d.stop(syscall.SIGTERM)
			d.start()
		}
		out, err := command("ls", "runtime-host", namespace, "-limit", "1").Output()
		var listing proto.ListResult
		if who == owner && canServe {
			if err != nil || json.Unmarshal(out, &listing) != nil || len(listing.Entries) != 1 {
				t.Fatal("mixed CLI listing failed")
			}
		} else {
			exit, ok := err.(*exec.ExitError)
			want := "denied by default"
			if who == owner {
				want = proto.NewError(proto.CodeUnsupportedFeature, "", proto.StateNotSent).Message
			}
			if !ok || len(out) != 0 || !strings.Contains(string(exit.Stderr), want) {
				t.Fatal("mixed CLI artifact rejection or project denial failed")
			}
		}
		if !canServe && who == owner {
			d.stop(syscall.SIGTERM)
			d.start()
		}
		mc := mcp.NewClient(&mcp.Implementation{Name: "phase8-compat", Version: "1"}, nil)
		session, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command("serve")}, nil)
		if err != nil {
			t.Fatal(err)
		}
		result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_list", Arguments: map[string]any{"host": "runtime-host", "path": namespace, "limit": 1}})
		session.Close()
		if err != nil || result == nil || result.IsError != (who == other || !canServe) {
			t.Fatal("mixed MCP listing or project denial failed")
		}
		if result.IsError {
			want := "denied by default"
			if who == owner {
				want = proto.NewError(proto.CodeUnsupportedFeature, "", proto.StateNotSent).Message
			}
			data, _ := json.Marshal(result)
			if !strings.Contains(string(data), want) {
				t.Fatal("mixed MCP rejection reason differs from exact boundary")
			}
		}
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("mixed shared client attempted private SSH fallback")
	}
	phase8AgentIdentity(t, ssh, selectedAgents, pid)
	check := "assert open(p).read()=='once'"
	if !canServe {
		check = "assert not os.path.exists(p)"
	}
	if _, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/mixed-marker')\n" + check + "\n"); err != nil {
		t.Fatal("mixed compatibility business effects differ from acceptance contract")
	}
	t.Logf("actual selected component digests retained; can_serve=%t; two-project CLI/MCP isolation, approval-before-effect and no private SSH fallback passed", canServe)
}

func TestRemotePhase8StandaloneCompatibility(t *testing.T) {
	_, previous, currentAgents, previousAgents := phase8Artifacts(t)
	oldClient := os.Getenv("RDEV_TEST_COMPAT_OLD_CLIENT") == "1"
	for _, frontend := range []string{"cli", "mcp"} {
		t.Run(frontend, func(t *testing.T) {
			controls := phase8ControlScope(t)
			d, namespace, ssh := newRemoteRuntime(t)
			owner := broker.Owner{ClientID: "standalone-seed", ProjectID: "phase8"}
			policy := broker.NewPolicy()
			if err := policy.GrantHost(owner.Key(), "runtime-host", "ping", "ping"); err != nil {
				t.Fatal(err)
			}
			if err := policy.Save(d.socket + ".policy"); err != nil {
				t.Fatal(err)
			}
			seedAgents := previousAgents
			if oldClient {
				seedAgents = currentAgents
			}
			phase8Seed(t, d, owner, previous, seedAgents)
			// Force this frontend to create its own actual isolated master.
			phase8CloseControls(t, controls)
			phase8AgentIdentity(t, ssh, seedAgents, 0)
			cli := os.Getenv("RDEV_TEST_CLI_BINARY")
			command := func(args ...string) *exec.Cmd {
				cmd := exec.CommandContext(t.Context(), cli, args...)
				cmd.Dir = d.dir
				cmd.Env = append(append([]string{}, d.env...), "RDEV_BROKER_SOCKET=", "RDEV_PRINCIPAL_TOKEN=", "RDEV_APPROVAL_TOKEN=", "RDEV_OPERATION_ID=")
				return cmd
			}
			argv := []string{"sh", "-c", `printf once >> "$HOME/$1/standalone-marker"; printf standalone-ok`, "compat", namespace}
			if frontend == "cli" {
				// Private mount namespace keeps both the process's HOME value and
				// the user's real registry untouched. No global trust edits occur.
				if err := exec.Command("unshare", "--mount", "true").Run(); err != nil {
					t.Skip("standalone CLI needs a permitted private mount namespace")
				}
				home, err := os.UserHomeDir()
				if err != nil {
					t.Fatal(err)
				}
				isolated := filepath.Join(d.dir, "frontend-home")
				if err := os.MkdirAll(filepath.Join(isolated, ".rdev"), 0700); err != nil {
					t.Fatal(err)
				}
				hosts, err := os.ReadFile(filepath.Join(d.dir, "hosts.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(isolated, ".rdev/hosts.json"), hosts, 0600); err != nil {
					t.Fatal(err)
				}
				args := []string{"--mount", "--propagation", "private", "sh", "-c", `mount --bind "$1" "$2" || exit 97; shift 2; exec "$@"`, "compat", isolated, home, cli, "exec", "runtime-host", "-no-login", "--"}
				cmd := exec.CommandContext(t.Context(), "unshare", append(args, argv...)...)
				cmd.Dir, cmd.Env = d.dir, command().Env
				out, err := cmd.CombinedOutput()
				if oldClient {
					if err == nil || !strings.Contains(string(out), "built later") || strings.Contains(string(out), "standalone-ok") {
						t.Fatal("old standalone CLI did not reject downgrade before business dispatch")
					}
				} else if err != nil || string(out) != "standalone-ok" {
					t.Fatal("new standalone CLI failed actual predecessor upgrade")
				}
			} else {
				mc := mcp.NewClient(&mcp.Implementation{Name: "phase8-standalone", Version: "1"}, nil)
				session, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command("serve")}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				registered, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_session", Arguments: map[string]any{"host": "runtime-host", "addr": os.Getenv("RDEV_TEST_REMOTE"), "remote_dir": namespace, "login_shell": false}})
				if err != nil || registered == nil || registered.IsError {
					t.Fatal("standalone MCP in-memory host registration failed")
				}
				result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_exec", Arguments: map[string]any{"host": "runtime-host", "argv": argv, "login_shell": false}})
				if err != nil || result == nil || result.IsError != oldClient {
					t.Fatal("standalone MCP upgrade/downgrade outcome differs from contract")
				}
				data, _ := json.Marshal(result)
				if oldClient && !strings.Contains(string(data), "built later") || !oldClient && !strings.Contains(string(data), "standalone-ok") {
					t.Fatal("standalone MCP did not return the expected explicit result")
				}
			}
			phase8AgentIdentity(t, ssh, currentAgents, 0)
			check := "assert open(p).read()=='once'"
			if oldClient {
				check = "assert not os.path.exists(p)"
			}
			if _, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/standalone-marker')\n" + check + "\n"); err != nil {
				t.Fatal("standalone compatibility changed business effects incorrectly")
			}
		})
	}
}

func TestRemotePhase8FleetCompatibility(t *testing.T) {
	current, previous, currentAgents, previousAgents := phase8Artifacts(t)
	phase8ControlScope(t)
	t.Setenv("RDEV_TEST_DAEMON_BINARY", previous)
	t.Setenv("RDEV_TEST_AGENT_DIR", previousAgents)
	f := newFleetRuntime(t, false)
	completed := f.plan(f.spec("predecessor-counter", "all", 2))
	f.execute(completed, f.approve(completed))
	completed = f.await(completed.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 3 })
	prepared := f.plan(f.spec("retained-approval-counter", "all", 2))
	approval := f.approve(prepared)
	prepared = f.await(prepared.PlanID, func(p *broker.FleetPlan) bool { return p.ApprovalRef != "" })
	// Keep a real predecessor approval unconsumed until the candidate resumes.
	inventory := f.inventory
	auditIdentity := func(event broker.AuditEvent) string {
		data, _ := json.Marshal(event)
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	}
	priorAudit := map[string]bool{}
	awaitRuntime(t, 5*time.Second, "predecessor Fleet audit", func() bool {
		r := policyRuntimeRequest(t, f.wire, broker.Request{Owner: f.owner, Operation: "audit_query"})
		if !r.OK || r.AuditIncomplete {
			return false
		}
		for _, event := range r.Audit {
			if event.PlanRef != "" && event.Attempt == 1 && event.Owner == broker.AuditOwnerID(f.owner.Key()) {
				priorAudit[auditIdentity(event)] = true
			}
		}
		return len(priorAudit) > 0
	})
	assertAudit := func() {
		t.Helper()
		r := policyRuntimeRequest(t, f.wire, broker.Request{Owner: f.owner, Operation: "audit_query"})
		if !r.OK || r.AuditIncomplete {
			t.Fatal("broker transition lost clean audit continuity")
		}
		found := map[string]bool{}
		for _, event := range r.Audit {
			if event.Owner != broker.AuditOwnerID(f.owner.Key()) {
				t.Fatal("historical audit crossed owner")
			}
			found[auditIdentity(event)] = true
		}
		for fingerprint := range priorAudit {
			if !found[fingerprint] {
				t.Fatal("broker transition lost an exact predecessor audit event")
			}
		}
	}
	f.d.stop(syscall.SIGTERM)
	phase8Select(f.d, current, currentAgents)
	f.d.start()
	f.connect()
	assertContinuity := func() {
		t.Helper()
		r := f.call("fleet.inventory.list", &broker.FleetRequest{}, "")
		if !r.OK || r.Inventory == nil || !reflect.DeepEqual(*r.Inventory, inventory) {
			t.Fatal("broker transition changed immutable inventory/HostID identities")
		}
		for _, before := range []*broker.FleetPlan{completed, prepared} {
			r := f.call("fleet.status", &broker.FleetRequest{PlanID: before.PlanID}, "")
			if !r.OK || r.Fleet == nil || r.Fleet.Digest != before.Digest || !reflect.DeepEqual(r.Fleet.Runs, before.Runs) || r.Fleet.Owner != before.Owner || r.Fleet.ApprovalRef != before.ApprovalRef || r.Fleet.ApprovalPolicy != before.ApprovalPolicy || r.Fleet.ApprovalConsumed != before.ApprovalConsumed || !r.Fleet.ApprovalExpiresAt.Equal(before.ApprovalExpiresAt) {
				t.Fatal("broker transition changed Fleet attempt/operation/job or approval binding")
			}
			foreign := f.d.dial(f.other, f.d.token(f.other, "5m"), true)
			r = policyRuntimeRequest(t, foreign, broker.Request{Owner: f.other, Operation: "fleet.status", Fleet: &broker.FleetRequest{PlanID: before.PlanID}})
			foreign.Close()
			if r.OK || r.Fleet != nil || r.Error != "fleet plan unavailable" {
				t.Fatal("broker transition disclosed another project's retained plan")
			}
		}
		assertAudit()
	}
	assertContinuity()
	f.execute(prepared, approval)
	prepared = f.await(prepared.PlanID, func(p *broker.FleetPlan) bool { return p.Counts["success"] == 3 })
	// Actual broker-only rollback preserves the newer remote executable. This
	// does not claim agent binary/state downgrade or legacy migration fencing.
	for _, binary := range []string{previous, current} {
		f.d.stop(syscall.SIGTERM)
		phase8Select(f.d, binary, currentAgents)
		f.d.start()
		f.connect()
		assertContinuity()
		if r := f.call("fleet.execute", &broker.FleetRequest{PlanID: prepared.PlanID, Digest: prepared.Digest}, approval); !r.OK || r.Fleet == nil || !reflect.DeepEqual(r.Fleet.Runs, prepared.Runs) || r.Fleet.State != prepared.State || !r.Fleet.ApprovalConsumed {
			t.Fatal("terminal execute did not return the retained receipt without a new attempt")
		}
		for _, plan := range []*broker.FleetPlan{completed, prepared} {
			for _, run := range plan.Runs {
				r := policyRuntimeRequest(t, f.wire, broker.Request{Owner: f.owner, Operation: "mutation.status", MutationID: run.OperationID})
				if !r.OK || r.Mutation == nil || r.Mutation.OperationID != run.OperationID || r.Mutation.State != "completed" || !r.Mutation.RemoteOK || r.Mutation.JobID != run.JobID {
					t.Fatal("broker transition lost completed per-attempt mutation identity")
				}
			}
		}
		for i := 1; i <= 3; i++ {
			i := i
			phase8AgentIdentity(t, func(script string) ([]byte, error) {
				return f.ssh(fmt.Sprintf("import sys\nsys.argv[1]+='/agent%d'\n", i) + script)
			}, currentAgents, 0)
		}
		f.proof("for i in range(1,4):\n for name in ('predecessor-counter','retained-approval-counter'):assert open(root+'/business'+str(i)+'/'+name).read()=='x'\n")
	}
	t.Logf("exact predecessor audit event fingerprints preserved across every clean broker transition: %d", len(priorAudit))
	t.Log("e73 -> candidate -> e73 broker-only rollback -> candidate: exact inventory HostIDs, retained approval execution once, owner/plan/digest/attempt/job IDs, completed mutation outcomes and audit continuity preserved; six actual marker files remain single x; agent state/schema downgrade not claimed")
}
