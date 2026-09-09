package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

type phase8SignedCandidate struct {
	policy   artifact.Policy
	manifest artifact.Manifest
	digest   string
}

func phase8ReadSignedCandidate(t *testing.T, key string) phase8SignedCandidate {
	t.Helper()
	path := os.Getenv(key)
	if path == "" {
		t.Skip("explicit already verified signed test bundles required")
	}
	policy, err := artifact.LoadPolicy(path, time.Now())
	if err != nil {
		t.Fatal("signed test policy unavailable")
	}
	manifest, decision, err := artifact.VerifyBundle(t.Context(), policy.BundleDir, policy, time.Now())
	if err != nil || !decision.TestRoot || decision.Unsigned || decision.Signer != "phase8-isolated-test" || policy.AllowUnsignedDev {
		t.Fatal("matrix requires complete trusted bundles under the isolated test root, without unsigned fallback")
	}
	for _, binary := range manifest.Binaries {
		if binary.Name == "rdev-agent-linux-amd64" {
			return phase8SignedCandidate{policy, manifest, binary.SHA256}
		}
	}
	t.Fatal("native signed agent missing")
	return phase8SignedCandidate{}
}

// The candidates are clean source-defined test releases, not formal N/N-1.
// Their agent implementation may be identical: different stamps/versions do not
// certify historical implementation or schema migration. The frozen bundle
// separately proves version reuse refusal without modifying its bytes.
func TestRemotePhase8SignedUpgradeRollback(t *testing.T) {
	previous := phase8ReadSignedCandidate(t, "RDEV_TEST_SIGNED_PREVIOUS_POLICY")
	candidate := phase8ReadSignedCandidate(t, "RDEV_TEST_SIGNED_CANDIDATE_POLICY")
	frozen := phase8ReadSignedCandidate(t, "RDEV_TEST_SIGNED_FROZEN_POLICY")
	if previous.manifest.Source.Commit == candidate.manifest.Source.Commit || candidate.manifest.Source.Commit != frozen.manifest.Source.Commit || previous.digest == candidate.digest || previous.digest == frozen.digest {
		t.Fatal("signed matrix needs real distinct historical artifacts and the original frozen candidate")
	}
	cmp, err := artifact.CompareVersions(candidate.manifest.Version, previous.manifest.Version)
	if err != nil || cmp <= 0 || previous.manifest.Version != frozen.manifest.Version {
		t.Fatal("signed matrix versions do not exercise upgrade, downgrade and version reuse")
	}
	controls := phase8ControlScope(t)
	var confirmRemoteAbsent func() ([]byte, error)
	// Register before the shared fixture, so this runs after its removal and
	// proves absence even though its historical rmtree used ignore_errors.
	t.Cleanup(func() {
		if confirmRemoteAbsent != nil {
			if _, err := confirmRemoteAbsent(); err != nil {
				t.Error("signed fixture namespace cleanup was not confirmed")
			}
		}
	})
	d, namespace, ssh := newRemoteRuntime(t)
	confirmRemoteAbsent = func() ([]byte, error) {
		return ssh("import os,sys\nassert not os.path.lexists(os.path.expanduser('~/'+sys.argv[1]))\n")
	}
	t.Logf("RDEV_SIGNED_SCOPE namespace=%s control=%s", namespace, controls)
	cleanupRemoteJobSupervisors(t, ssh)
	owner := broker.Owner{ClientID: "phase8-signed", ProjectID: "rollback"}
	policy := broker.NewPolicy()
	if err := policy.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{proto.OpPing, proto.OpJobStart, proto.OpJobWait, proto.OpJobStatus, proto.OpJobLogs} {
		if err := policy.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
			t.Fatal(err)
		}
	}
	if err := policy.Grant(owner.Key(), "mutation.status"); err != nil {
		t.Fatal(err)
	}
	if err := policy.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	// The runtime workspace can inherit runner ACLs. Keep administrator trust
	// inputs in the short, private /tmp scope, whose exact cleanup is asserted.
	policyPath := filepath.Join(controls, "release-policy.json")
	d.env = append(d.env, "RDEV_RELEASE_POLICY="+policyPath)
	hostsPath := filepath.Join(d.dir, "hosts.json")
	hostData, err := os.ReadFile(hostsPath)
	if err != nil {
		t.Fatal(err)
	}
	var hosts struct {
		Hosts []map[string]any `json:"hosts"`
	}
	if json.Unmarshal(hostData, &hosts) != nil || len(hosts.Hosts) != 1 {
		t.Fatal("isolated host definition missing")
	}
	target := artifact.TargetKey(os.Getenv("RDEV_TEST_REMOTE"), 0, namespace)
	selectCandidate := func(t *testing.T, selected phase8SignedCandidate, force bool, grants map[string]string) {
		t.Helper()
		d.stop(syscall.SIGTERM)
		copy := selected.policy
		copy.RollbackDigests = grants
		data, _ := json.Marshal(copy)
		if err := os.WriteFile(policyPath, data, 0600); err != nil {
			t.Fatal(err)
		}
		hosts.Hosts[0]["force_agent_upload"] = force
		data, _ = json.Marshal(hosts)
		if err := os.WriteFile(hostsPath, data, 0600); err != nil {
			t.Fatal(err)
		}
		phase8Select(d, d.bin, selected.policy.BundleDir)
		d.start()
	}
	call := func(t *testing.T, request broker.Request) broker.Response {
		t.Helper()
		request.Owner = owner
		w := d.dial(owner, d.token(owner, "5m"), true)
		defer w.Close()
		return policyRuntimeRequest(t, w, request)
	}
	ping := func(t *testing.T) broker.Response {
		return call(t, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	}
	assertInstalled := func(t *testing.T, want phase8SignedCandidate, rollback bool) {
		t.Helper()
		phase8AgentIdentity(t, ssh, want.policy.BundleDir, 0)
		check := fmt.Sprintf("import os,sys,json\np=os.path.expanduser('~/'+sys.argv[1]);r=json.load(open(p+'/.rdev-release.json'))\nassert r['digest']==%q and r['version']==%q and r['signer']=='phase8-isolated-test' and r['channel']==%q\nassert r['test_root'] and not r['unsigned'] and r['rollback']==%s\n", want.digest, want.manifest.Version, want.manifest.Channel, map[bool]string{true: "True", false: "False"}[rollback])
		if _, err := ssh(check); err != nil {
			t.Fatal("remote signed release decision disagrees with verified artifact")
		}
	}
	requirePing := func(t *testing.T, want phase8SignedCandidate, rollback bool) {
		t.Helper()
		r := ping(t)
		if !r.OK || r.Wire == nil || r.Wire.Ping == nil {
			t.Fatal("signed actual SSH handshake failed")
		}
		phase8AgentIdentity(t, ssh, want.policy.BundleDir, r.Wire.Ping.PID)
		assertInstalled(t, want, rollback)
	}
	requireRefusal := func(t *testing.T, code proto.ErrorCode, remains phase8SignedCandidate, rollback bool) {
		t.Helper()
		r := ping(t)
		if r.OK || r.Wire != nil || r.ErrorEnvelope == nil || r.ErrorEnvelope.Validate() != nil || r.ErrorEnvelope.Code != code || r.ErrorEnvelope.ExecutionState != proto.StateNotSent {
			t.Fatalf("signed refusal did not preserve expected %s/not_sent", code)
		}
		assertInstalled(t, remains, rollback)
	}
	selectCandidate(t, previous, false, nil)
	requirePing(t, previous, false)
	wire := &proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf once >> "$HOME/$1/signed-proof"; printf retained`, "signed-test", namespace}}, Resources: &proto.ResourceEnvelope{WallTimeoutSec: 30}}}
	operationID, err := proto.NewOperationID()
	if err != nil {
		t.Fatal(err)
	}
	wire.OperationID = operationID
	started := call(t, broker.Request{Operation: wire.Op, Host: "runtime-host", Wire: wire, Approval: d.approve(owner, wire)})
	if !started.OK || started.Wire == nil || started.Wire.Job == nil || started.Wire.Job.Info == nil || started.Wire.Job.Info.StartOperationID != operationID {
		t.Fatal("signed predecessor job start failed")
	}
	jobID := started.Wire.Job.Info.ID
	waited := call(t, broker.Request{Operation: proto.OpJobWait, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: jobID, WaitTimeoutSec: 5, TailOnExit: 1}}})
	if !waited.OK || waited.Wire == nil || waited.Wire.Job == nil || waited.Wire.Job.Info == nil || waited.Wire.Job.Info.State != proto.JobExited || waited.Wire.Job.Logs != "retained" {
		t.Fatal("signed predecessor job did not finish")
	}
	baseline := call(t, broker.Request{Operation: "mutation.status", MutationID: operationID})
	if !baseline.OK || baseline.Mutation == nil || baseline.Mutation.State != "completed" || baseline.Mutation.JobID != jobID || !baseline.Mutation.RemoteOK {
		t.Fatal("signed predecessor did not durably record the exact preassigned mutation")
	}
	selectCandidate(t, candidate, false, nil)
	requirePing(t, candidate, false)
	t.Log("signed real SSH upgrade completed with new serving inode and original detached-job result retained")
	for _, tc := range []struct {
		name  string
		force bool
		grant map[string]string
	}{
		{"force-without-authorization", true, nil},
		{"wrong-target-authorization", true, map[string]string{strings.Repeat("a", 64): previous.digest}},
		{"wrong-digest-authorization", true, map[string]string{target: strings.Repeat("a", 64)}},
		{"authorization-without-force", false, map[string]string{target: previous.digest}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selectCandidate(t, previous, tc.force, tc.grant)
			requireRefusal(t, proto.CodeReleaseVersion, candidate, false)
		})
	}
	// Inject only this fixture's manifest. Product rollback must neither repair
	// nor delete it. Restore the exact original fixture bytes after the refusal.
	d.stop(syscall.SIGTERM)
	original, err := ssh("import os,sys,json,base64\np=os.path.expanduser('~/'+sys.argv[1]+'/manifest.json')\nprint(json.dumps({'exists':os.path.exists(p),'data':base64.b64encode(open(p,'rb').read()).decode() if os.path.exists(p) else ''}))\n")
	if err != nil {
		t.Fatal("cannot snapshot isolated state manifest")
	}
	var originalManifest struct {
		Exists bool   `json:"exists"`
		Data   string `json:"data"`
	}
	if json.Unmarshal(original, &originalManifest) != nil {
		t.Fatal("invalid manifest snapshot")
	}
	restore := func(t *testing.T) {
		d.stop(syscall.SIGTERM)
		script := "import os,sys,base64\np=os.path.expanduser('~/'+sys.argv[1]+'/manifest.json')\n"
		if originalManifest.Exists {
			script += fmt.Sprintf("open(p,'wb').write(base64.b64decode(%q));os.chmod(p,0o600)\n", originalManifest.Data)
		} else {
			script += "os.unlink(p)\n"
		}
		if _, err := ssh(script); err != nil {
			t.Fatal("isolated injected manifest recovery failed")
		}
	}
	for _, tc := range []struct{ name, data string }{{"future-state", `{"schema_version":999,"namespace":"signed-rollback-fixture"}`}, {"corrupt-state", `{"schema_version":`}} {
		t.Run(tc.name, func(t *testing.T) {
			d.stop(syscall.SIGTERM)
			encoded := base64.StdEncoding.EncodeToString([]byte(tc.data))
			if _, err := ssh(fmt.Sprintf("import os,sys,base64\np=os.path.expanduser('~/'+sys.argv[1]+'/manifest.json');open(p,'wb').write(base64.b64decode(%q));os.chmod(p,0o600)\n", encoded)); err != nil {
				t.Fatal("isolated manifest fault injection failed")
			}
			defer restore(t)
			selectCandidate(t, previous, true, map[string]string{target: previous.digest})
			requireRefusal(t, proto.CodeStateIncompatible, candidate, false)
			if _, err := ssh(fmt.Sprintf("import os,sys,base64\np=os.path.expanduser('~/'+sys.argv[1]+'/manifest.json');assert open(p,'rb').read()==base64.b64decode(%q)\n", encoded)); err != nil {
				t.Fatal("state rejection modified future/corrupt manifest")
			}
		})
	}
	selectCandidate(t, previous, true, map[string]string{target: previous.digest})
	requirePing(t, previous, true)
	if _, err := ssh(fmt.Sprintf("import os,sys,hashlib\np=os.path.expanduser('~/'+sys.argv[1]);assert hashlib.sha256(open(p+'/.rdev-agent.previous','rb').read()).hexdigest()==%q\nassert open(p+'/signed-proof').read()=='once'\n", candidate.digest)); err != nil {
		t.Fatal("authorized signed rollback lost forward-recovery bytes or repeated job")
	}
	outcome := call(t, broker.Request{Operation: "mutation.status", MutationID: operationID})
	if !outcome.OK || outcome.Mutation == nil || outcome.Mutation.State != "completed" || outcome.Mutation.JobID != jobID || !outcome.Mutation.RemoteOK {
		t.Fatal("signed rollback lost original job mutation outcome")
	}
	observed := call(t, broker.Request{Operation: proto.OpJobStatus, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: jobID}}})
	if !observed.OK || observed.Wire == nil || observed.Wire.Job == nil || observed.Wire.Job.Info == nil || observed.Wire.Job.Info.ID != jobID || observed.Wire.Job.Info.State != proto.JobExited {
		t.Fatal("signed rollback lost retained job identity")
	}
	selectCandidate(t, frozen, true, map[string]string{target: frozen.digest})
	requireRefusal(t, proto.CodeReleaseVersion, previous, true)
	// The same-byte branch still verifies the signature, exact grant and health.
	selectCandidate(t, previous, true, map[string]string{target: previous.digest})
	requirePing(t, previous, true)
	t.Logf("actual signed SSH %s/%s -> %s/%s -> %s/%s: exact force+target+digest authorization, future/corrupt state refusal, frozen bundle same-version reuse refusal, retained mutation/job/marker and previous bytes verified; test root only, no formal N-1 claim", previous.manifest.Source.Commit, previous.manifest.Version, candidate.manifest.Source.Commit, candidate.manifest.Version, previous.manifest.Source.Commit, previous.manifest.Version)
}
