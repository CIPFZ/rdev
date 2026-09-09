package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/synctree"
)

// This uses actual e73/047 artifacts and their unchanged secret/outcome formats.
// Only the broker rolls back: the new agent and writer fencing remain in place.
// No manifest is rewritten, and formal N/N-1 or schema migration is not inferred.
func TestRemotePhase8RetainedStateCompatibility(t *testing.T) {
	current, previous, currentAgents, previousAgents := phase8Artifacts(t)
	controls := phase8ControlScope(t)
	t.Setenv("RDEV_TEST_DAEMON_BINARY", previous)
	t.Setenv("RDEV_TEST_AGENT_DIR", previousAgents)
	var absent func() ([]byte, error)
	t.Cleanup(func() {
		if absent != nil {
			if _, err := absent(); err != nil {
				t.Error("retained-state private namespace cleanup unconfirmed")
			}
		}
	})
	d, namespace, ssh := newRemoteRuntime(t)
	absent = func() ([]byte, error) {
		return ssh("import os,sys\nassert not os.path.lexists(os.path.expanduser('~/'+sys.argv[1]))\n")
	}
	t.Logf("RDEV_RETAINED_STATE_SCOPE namespace=%s control=%s", namespace, controls)
	a := broker.Owner{ClientID: "phase8-retained", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	admin := runtimeApprovalAdmin()
	p := broker.NewPolicy()
	if err := p.Grant(admin.Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"secret.set", "secret.delete", "secret.list", "secret.use", "sync.push", "sync.pull", proto.OpPing, proto.OpReadFile, proto.OpExec} {
			if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
		for _, op := range []string{"mutation.status", "audit_query"} {
			if err := p.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(d.dir, "held-sync-response")
	t.Cleanup(func() { _ = os.Remove(gate) })
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(mutationSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_MUTATION_GATE="+gate)
	d.start()
	call := func(owner broker.Owner, req broker.Request) broker.Response {
		t.Helper()
		w := d.dial(owner, d.token(owner, "5m"), true)
		defer w.Close()
		req.Owner = owner
		return policyRuntimeRequest(t, w, req)
	}
	approve := func(owner broker.Owner, req broker.Request) broker.Request {
		t.Helper()
		r := call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: owner, Host: req.Host, Operation: req.Operation, Secret: req.Secret, Sync: req.Sync}})
		if !r.OK || r.Approval == nil {
			t.Fatal("retained-state exact approval failed")
		}
		req.Approval = r.Approval.Token
		var err error
		req.OperationID, err = proto.NewOperationID()
		if err != nil {
			t.Fatal(err)
		}
		return req
	}
	mutations := map[string]broker.MutationIntent{}
	remember := func(req broker.Request, result broker.Response) {
		t.Helper()
		if !result.OK || result.Mutation == nil || result.Mutation.OperationID != req.OperationID || result.Mutation.State != "completed" || !result.Mutation.RemoteOK {
			t.Fatal("predecessor mutation did not durably complete")
		}
		mutations[req.OperationID] = *result.Mutation
	}
	values := []string{"p8-retired-first-credential", "p8-retired-second-credential", "p8-active-alpha-credential", "p8-active-beta-credential"}
	for _, spec := range []struct {
		owner           broker.Owner
		op, name, value string
	}{{a, "secret.set", "rotated", values[0]}, {a, "secret.set", "rotated", values[1]}, {a, "secret.delete", "rotated", ""}, {a, "secret.set", "live", values[2]}, {b, "secret.set", "other", values[3]}} {
		req := approve(spec.owner, broker.Request{Operation: spec.op, Host: "runtime-host", Secret: &broker.SecretParams{Name: spec.name, Value: spec.value}})
		remember(req, call(spec.owner, req))
	}
	archive, err := os.ReadFile(d.socket + ".secrets")
	if err != nil {
		t.Fatal("predecessor archive missing")
	}
	var archiveShape struct {
		Schema  int
		Records []struct{ Active bool }
	}
	if json.Unmarshal(archive, &archiveShape) != nil || archiveShape.Schema != broker.SecretSchemaVersion || len(archiveShape.Records) != 4 {
		t.Fatal("predecessor archive shape differs")
	}
	active := 0
	for _, record := range archiveShape.Records {
		if record.Active {
			active++
		}
	}
	if active != 2 {
		t.Fatal("retirement retained injection authority")
	}
	archiveSHA := phase8Digest(t, d.socket+".secrets")
	encoded, _ := json.Marshal(strings.Join(values, "\n"))
	// Business snapshots cover only business/{push,held}; namespace/.sync is
	// a disjoint subtree and its staging/outcomes never enter the target tree.
	if _, err := ssh(fmt.Sprintf("import os,sys,json\np=os.path.expanduser('~/'+sys.argv[1]+'/business');os.makedirs(p,exist_ok=True)\nopen(p+'/archive','w').write(json.loads(%q))\n", string(encoded))); err != nil {
		t.Fatal("private historical output fixture failed")
	}
	verifySecrets := func() {
		t.Helper()
		if phase8Digest(t, d.socket+".secrets") != archiveSHA {
			t.Fatal("broker transition changed exact secret archive bytes")
		}
		for _, owner := range []broker.Owner{a, b} {
			r := call(owner, broker.Request{Operation: proto.OpReadFile, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: namespace + "/business/archive", Limit: 4096}}})
			if !r.OK || r.Wire == nil || r.Wire.Read == nil || strings.Count(r.Wire.Read.Content, "<redacted:broker_") != len(values) {
				t.Fatal("historical output redaction failed")
			}
			for _, value := range values {
				if strings.Contains(r.Wire.Read.Content, value) {
					t.Fatal("retired credential disclosed")
				}
			}
			list := call(owner, broker.Request{Operation: "secret.list", Host: "runtime-host", Secret: &broker.SecretParams{}})
			name, value := "live", values[2]
			if owner == b {
				name, value = "other", values[3]
			}
			if !list.OK || len(list.Secrets) != 1 || list.Secrets[0].Name != name {
				t.Fatal("retired or foreign secret became injectable")
			}
			inject := func(name string) broker.Response {
				wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"python3", "-c", "import os,hashlib;v=os.environ['TOKEN'];print(hashlib.sha256(v.encode()).hexdigest());print(v)"}, Env: map[string]string{"TOKEN": "secret:" + name}}}
				approval := call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: owner, Operation: wire.Op, Host: "runtime-host", Wire: wire}})
				if !approval.OK || approval.Approval == nil {
					return approval
				}
				return call(owner, broker.Request{Operation: wire.Op, Host: "runtime-host", Wire: wire, Approval: approval.Approval.Token})
			}
			r = inject(name)
			sum := sha256.Sum256([]byte(value))
			if !r.OK || r.Wire == nil || r.Wire.Exec == nil || !strings.HasPrefix(r.Wire.Exec.Stdout, hex.EncodeToString(sum[:])) || strings.Contains(r.Wire.Exec.Stdout, value) || !strings.Contains(r.Wire.Exec.Stdout, "<redacted:broker_") {
				t.Fatal("active secret injection/redaction changed")
			}
			foreign := "other"
			if owner == b {
				foreign = "live"
			}
			for _, denied := range []string{"rotated", foreign} {
				if rejection := inject(denied); rejection.OK || rejection.Error != "secret unavailable for this principal and host" {
					t.Fatal("retired/foreign secret did not return the exact unavailable refusal")
				}
			}
		}
	}
	verifySecrets()
	source, download := filepath.Join(controls, "source"), filepath.Join(controls, "download")
	for _, dir := range []string{source, download} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	payload := []byte("retained-phase8-sync\x00exact-bytes")
	if err := os.WriteFile(filepath.Join(source, "file"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	prepare := func(opts client.SyncOptions) broker.Request {
		t.Helper()
		opts.Prepare, opts.DryRun = true, true
		r := call(a, broker.Request{Operation: "sync." + opts.Direction, Host: "runtime-host", Sync: &opts})
		if !r.OK || r.Sync == nil || r.Sync.PlanID == "" || !r.Sync.ManifestComplete {
			t.Fatal("predecessor retained sync prepare failed")
		}
		opts.Prepare, opts.DryRun, opts.PlanID = false, false, r.Sync.PlanID
		return approve(a, broker.Request{Operation: "sync." + opts.Direction, Host: "runtime-host", Sync: &opts})
	}
	push := prepare(client.SyncOptions{Direction: "push", Local: source + "/", Remote: "~/" + namespace + "/business/push/"})
	remember(push, call(a, push))
	pull := prepare(client.SyncOptions{Direction: "pull", Local: download + "/", Remote: push.Sync.Remote})
	remember(pull, call(a, pull))
	held := prepare(client.SyncOptions{Direction: "push", Local: source + "/", Remote: "~/" + namespace + "/business/held/"})
	gateData, _ := json.Marshal(map[string]string{"operation_id": held.OperationID})
	if err := os.WriteFile(gate, gateData, 0600); err != nil {
		t.Fatal(err)
	}
	pending := d.dial(a, d.token(a, "5m"), true)
	defer pending.Close()
	held.Owner = a
	if err := pending.enc.Encode(held); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 10*time.Second, "predecessor sync durable response barrier", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
	r := call(a, broker.Request{Operation: "mutation.status", MutationID: held.OperationID})
	if !r.OK || r.Mutation == nil || r.Mutation.State != "dispatched" || r.Mutation.RemoteOK {
		t.Fatal("predecessor did not hold the actual pre-ACK persistence window")
	}
	heldIntent := *r.Mutation
	remoteSnapshot := func() []byte {
		t.Helper()
		data, err := ssh("import os,sys,json,hashlib\np=os.path.expanduser('~/'+sys.argv[1]);out={}\nfor name in sorted(os.listdir(p+'/.sync/.outcomes')):\n f=p+'/.sync/.outcomes/'+name;b=open(f,'rb').read();out[name]={'sha256':hashlib.sha256(b).hexdigest(),'outcome':json.loads(b)}\nfor name in ('push','held'):\n f=p+'/business/'+name+'/file';s=os.stat(f);out[name]={'sha256':hashlib.sha256(open(f,'rb').read()).hexdigest(),'inode':s.st_ino,'mtime_ns':s.st_mtime_ns}\nprint(json.dumps(out,sort_keys=True))\n")
		if err != nil {
			t.Fatal("durable remote sync snapshot unavailable")
		}
		return data
	}
	remoteBefore := remoteSnapshot()
	var remoteRecords map[string]json.RawMessage
	if json.Unmarshal(remoteBefore, &remoteRecords) != nil || len(remoteRecords) != 4 {
		t.Fatal("expected exactly two real remote outcomes and two business files")
	}
	for _, req := range []broker.Request{push, held} {
		found := false
		for _, raw := range remoteRecords {
			var item struct{ Outcome synctree.Outcome }
			_ = json.Unmarshal(raw, &item)
			if item.Outcome.OperationID == req.OperationID {
				ownerSum := sha256.Sum256([]byte(proto.PrincipalID(a.ClientID, a.ProjectID)))
				intent := heldIntent
				if req.OperationID == push.OperationID {
					intent = mutations[push.OperationID]
				}
				if item.Outcome.Owner != hex.EncodeToString(ownerSum[:]) || item.Outcome.State != "completed" || item.Outcome.Digest != intent.SyncDigest {
					t.Fatal("remote outcome identity differs from exact approved intent")
				}
				found = true
			}
		}
		if !found {
			t.Fatal("remote predecessor outcome missing")
		}
	}
	localPaths, err := filepath.Glob(d.socket + ".sync/.outcomes/*.json")
	if err != nil || len(localPaths) != 1 {
		t.Fatal("expected one real local pull outcome")
	}
	localBefore, err := os.ReadFile(localPaths[0])
	var localOutcome synctree.Outcome
	ownerSum := sha256.Sum256([]byte(a.Key()))
	if err != nil || json.Unmarshal(localBefore, &localOutcome) != nil || localOutcome.OperationID != pull.OperationID || localOutcome.Owner != hex.EncodeToString(ownerSum[:]) || localOutcome.State != "completed" || localOutcome.Digest != mutations[pull.OperationID].SyncDigest {
		t.Fatal("local pull outcome identity differs")
	}
	localInfo, err := os.Stat(filepath.Join(download, "file"))
	if err != nil {
		t.Fatal(err)
	}
	verify := func(stage string, agents string) {
		t.Helper()
		verifySecrets()
		ping := call(a, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
		if !ping.OK || ping.Wire == nil || ping.Wire.Ping == nil {
			t.Fatal("transition failed actual handshake")
		}
		phase8AgentIdentity(t, ssh, agents, ping.Wire.Ping.PID)
		for id, before := range mutations {
			owner := a
			if before.Owner == b.Key() {
				owner = b
			}
			result := call(owner, broker.Request{Operation: "mutation.status", MutationID: id})
			if !result.OK || result.Mutation == nil || !reflect.DeepEqual(*result.Mutation, before) {
				t.Fatal("transition changed exact retained mutation binding/outcome")
			}
			other := b
			if owner == b {
				other = a
			}
			foreign := call(other, broker.Request{Operation: "mutation.status", MutationID: id})
			if foreign.OK || foreign.Mutation != nil || foreign.Error != broker.ErrMutationUnknown.Error() {
				t.Fatal("retained mutation crossed project")
			}
		}
		for _, req := range []broker.Request{push, pull, held} {
			if rejection := call(a, req); rejection.OK || rejection.Error != "sync plan unavailable or parameters changed" {
				t.Fatal("consumed predecessor sync did not return the exact retained-plan refusal")
			}
		}
		if !reflect.DeepEqual(remoteSnapshot(), remoteBefore) {
			t.Fatal("transition/replay changed exact remote outcomes or business file identity")
		}
		data, err := os.ReadFile(localPaths[0])
		if err != nil || !reflect.DeepEqual(data, localBefore) {
			t.Fatal("transition changed local pull outcome bytes")
		}
		data, err = os.ReadFile(filepath.Join(download, "file"))
		info, statErr := os.Stat(filepath.Join(download, "file"))
		if err != nil || statErr != nil || !reflect.DeepEqual(data, payload) || !os.SameFile(info, localInfo) || !info.ModTime().Equal(localInfo.ModTime()) {
			t.Fatal("transition/replay rewrote retained local bytes")
		}
		// Injection probes also execute approved mutations; this count covers
		// only the five secret and three sync identities retained for comparison.
		t.Logf("P8_RETAINED_STATE stage=%s archive_records=4 active=2 retired=2 tracked_mutations=%d remote_sync_outcomes=2 local_sync_outcomes=1 exact_bytes_unchanged=true", stage, len(mutations))
	}
	// Kill before local ACK, then replace only with the actual authorized newer
	// dev artifacts. The first outcome query must read the old remote record.
	d.stop(syscall.SIGKILL)
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	phase8Select(d, current, currentAgents)
	d.start()
	recovered := call(a, broker.Request{Operation: "mutation.status", MutationID: held.OperationID})
	remember(held, recovered)
	got := *recovered.Mutation
	got.State, got.RemoteOK, got.Updated = heldIntent.State, heldIntent.RemoteOK, heldIntent.Updated
	if !reflect.DeepEqual(got, heldIntent) {
		t.Fatal("cross-version remote outcome recovery changed intent binding")
	}
	verify("new-broker-new-agent-recovered-old-pre-ack-outcome", currentAgents)
	// Prove broker restart and a separate real agent SIGKILL/reconnect, retaining
	// outcomes and archive without rebuilding either binary or namespace.
	d.stop(syscall.SIGKILL)
	d.start()
	verify("new-broker-sigkill-restart", currentAgents)
	ping := call(a, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if !ping.OK || ping.Wire == nil || ping.Wire.Ping == nil {
		t.Fatal("agent crash fixture handshake missing")
	}
	pid := ping.Wire.Ping.PID
	if _, err := ssh(fmt.Sprintf("import os,sys,signal\np=os.path.expanduser('~/'+sys.argv[1]+'/rdev-agent');assert os.readlink('/proc/%d/exe')==p\nos.kill(%d,signal.SIGKILL)\n", pid, pid)); err != nil {
		t.Fatal("exact isolated agent SIGKILL failed")
	}
	awaitRuntime(t, 10*time.Second, "actual agent reconnect", func() bool {
		r := call(a, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
		return r.OK && r.Wire != nil && r.Wire.Ping != nil && r.Wire.Ping.PID != pid
	})
	verify("new-agent-sigkill-reconnect", currentAgents)
	for _, transition := range []struct{ name, binary string }{{"old-broker-only-rollback", previous}, {"new-broker-restored", current}} {
		d.stop(syscall.SIGTERM)
		phase8Select(d, transition.binary, currentAgents)
		d.start()
		verify(transition.name, currentAgents)
	}
	t.Log("actual e73-created secret archive and push/pull outcomes preserved through 047 upgrade, pre-ACK crash recovery, broker/agent SIGKILL and e73 broker-only rollback; same persistent namespace, no deleted evidence or state rewrite; not formal N/N-1, agent rollback or schema migration")
}
