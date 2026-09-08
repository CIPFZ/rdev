package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const secretImportSSHWrapper = `#!/usr/bin/env python3
import os,sys,subprocess,json,threading,time
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'):args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
p=subprocess.Popen(args+sys.argv[1:],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=sys.stderr.buffer)
held=set();gate=os.environ['RDEV_IMPORT_GATE']
def inbound():
 try:
  for line in sys.stdin.buffer:
   try:
    r=json.loads(line)
    if r.get('op')=='read_file' and (r.get('read') or {}).get('path','').endswith('/hold'):held.add(r.get('id'))
   except (ValueError,AttributeError):pass
   p.stdin.write(line);p.stdin.flush()
 except (OSError,BrokenPipeError):pass
 finally:
  try:p.stdin.close()
  except (OSError,BrokenPipeError):pass
threading.Thread(target=inbound,daemon=True).start()
broken=False
try:
 for line in p.stdout:
  try:
   r=json.loads(line)
   if r.get('id') in held and os.path.exists(gate):
    fd=os.open(gate+'.entered',os.O_CREAT|os.O_WRONLY,0o600);os.close(fd)
    while os.path.exists(gate):time.sleep(.01)
  except (ValueError,AttributeError):pass
  sys.stdout.buffer.write(line);sys.stdout.buffer.flush()
except (OSError,BrokenPipeError):broken=True
finally:
 if broken and p.poll() is None:p.terminate()
 try:p.wait(timeout=2)
 except subprocess.TimeoutExpired:p.kill();p.wait()
sys.exit(p.returncode)
`

func TestRemoteBrokerSecretImport(t *testing.T) {
	d, namespace, ssh := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "import-same-client", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	denied := broker.Owner{ClientID: a.ClientID, ProjectID: "no-file-read"}
	admin := runtimeApprovalAdmin()
	p := broker.NewPolicy()
	if err := p.Grant(admin.Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b, denied} {
		for _, op := range []string{"secret.set_from_file", "secret.list", "secret.use", proto.OpExec, proto.OpPing} {
			if err := p.GrantHost(owner.Key(), "runtime-host", broker.CapabilityForOperation(op), op); err != nil {
				t.Fatal(err)
			}
		}
		if owner != denied {
			if err := p.GrantHost(owner.Key(), "runtime-host", "file.read", proto.OpReadFile); err != nil {
				t.Fatal(err)
			}
		}
		for _, op := range []string{"status", "audit_query"} {
			if err := p.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(d.dir, "hold-source")
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(secretImportSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_IMPORT_GATE="+gate)
	const alpha = "remote-import-alpha-credential"
	const beta = "remote-import-beta-credential"
	sourceScript := "import os,sys\np=os.path.expanduser('~/'+sys.argv[1]);os.makedirs(p,exist_ok=True)\nopen(p+'/alpha','w').write('" + alpha + "\\n');open(p+'/beta','w').write('" + beta + "');open(p+'/hold','w').write('held-import-credential');open(p+'/exact','w').write('Q'*65536);open(p+'/large','w').write('x'*65537);open(p+'/binary','wb').write(bytes(range(256)));open(p+'/short','w').write('tiny')\n"
	if out, err := ssh(sourceScript); err != nil {
		t.Fatalf("source fixture: %v %s", err, out)
	}
	d.start()
	wires := map[broker.Owner]*runtimeWire{}
	connect := func() {
		for _, o := range []broker.Owner{a, b, denied, admin} {
			wires[o] = d.dial(o, d.token(o, "5m"), true)
		}
	}
	connect()
	call := func(o broker.Owner, r broker.Request) broker.Response {
		r.Owner = o
		return policyRuntimeRequest(t, wires[o], r)
	}
	params := func(source string) *broker.SecretParams {
		return &broker.SecretParams{Name: "imported", Path: "~/" + namespace + "/" + source}
	}
	issue := func(o broker.Owner, source string) broker.Response {
		return call(admin, broker.Request{Operation: "approval.create", ApprovalSpec: &broker.ApprovalSpec{Owner: o, Host: "runtime-host", Operation: "secret.set_from_file", Secret: params(source)}})
	}
	token := func(o broker.Owner, source string) string {
		t.Helper()
		r := issue(o, source)
		if !r.OK || r.Approval == nil {
			t.Fatal("source import approval failed")
		}
		return r.Approval.Token
	}
	state := func(o broker.Owner) broker.Response {
		return call(o, broker.Request{Operation: "secret.list", Host: "runtime-host", Secret: &broker.SecretParams{}})
	}
	if r := issue(denied, "alpha"); r.OK {
		t.Fatal("import grant implied file-read grant")
	}
	deniedStatus := call(denied, broker.Request{Operation: "status"})
	if !deniedStatus.OK || deniedStatus.Scheduler.Started != 0 {
		t.Fatal("denied source read entered scheduler")
	}
	for _, source := range []string{"missing", "large", "binary", "short"} {
		r := issue(a, source)
		if r.OK || strings.Contains(r.Error, namespace) {
			t.Fatal("invalid source accepted or path exposed in error")
		}
	}
	if r := state(a); !r.OK || len(r.Secrets) != 0 {
		t.Fatal("failed source read registered a key")
	}
	// Source substitution must preserve the original token, and changed contents
	// must fail even with the same approved path.
	beforeRead := call(a, broker.Request{Operation: "status"}).Scheduler.BulkPayloadBytes
	approval := token(a, "alpha")
	afterRead := call(a, broker.Request{Operation: "status"}).Scheduler.BulkPayloadBytes
	if afterRead-beforeRead != uint64(len(alpha)+1) {
		t.Fatal("internal import probe bypassed actual-payload accounting")
	}
	if counter := call(b, broker.Request{Operation: "status"}).Scheduler.BulkPayloadBytes; counter != 0 {
		t.Fatal("internal source bytes charged to another project")
	}

	substituted := call(a, broker.Request{Operation: "secret.set_from_file", Host: "runtime-host", Secret: params("beta"), Approval: approval})
	if substituted.OK {
		t.Fatal("source substitution approved")
	}
	if out, err := ssh("import os,sys\nopen(os.path.expanduser('~/'+sys.argv[1]+'/alpha'),'w').write('changed-import-credential')\n"); err != nil {
		t.Fatalf("rotate source fixture: %v %s", err, out)
	}
	if r := call(a, broker.Request{Operation: "secret.set_from_file", Host: "runtime-host", Secret: params("alpha"), Approval: approval}); r.OK {
		t.Fatal("approval selected changed source value")
	}
	if out, err := ssh(sourceScript); err != nil {
		t.Fatalf("restore source fixture: %v %s", err, out)
	}
	cli := filepath.Join(t.TempDir(), "rdev")
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", cli, "./cmd/rdev")
	build.Dir = repoRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v %s", err, out)
	}
	command := func(o broker.Owner, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(t.Context(), cli, args...)
		cmd.Env = append(os.Environ(), "RDEV_BROKER_SOCKET="+d.socket, "RDEV_CLIENT_ID="+o.ClientID, "RDEV_PROJECT_ID="+o.ProjectID, "RDEV_PRINCIPAL_TOKEN="+d.token(o, "5m"))
		return cmd
	}
	first := command(a, "secret", "set_from_file", "runtime-host", "imported", params("alpha").Path)
	first.Env = append(first.Env, "RDEV_APPROVAL_TOKEN="+approval)
	if out, err := first.Output(); err != nil || strings.Contains(string(out), alpha) {
		t.Fatal("CLI import failed or exposed credential")
	}
	mc := mcp.NewClient(&mcp.Implementation{Name: "secret-import-proof", Version: "1"}, nil)
	session, err := mc.Connect(t.Context(), &mcp.CommandTransport{Command: command(b, "serve")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "rdev_secrets", Arguments: map[string]any{"action": "set_from_file", "host": "runtime-host", "name": "imported", "path": params("beta").Path, "approval_token": token(b, "beta")}})
	if err != nil || result == nil || result.IsError {
		t.Fatal("MCP remote import failed")
	}
	verify := func(o broker.Owner, value string) {
		t.Helper()
		wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"python3", "-c", "import os,hashlib;v=os.environ['TOKEN'];print(hashlib.sha256(v.encode()).hexdigest());print(v)"}, Env: map[string]string{"TOKEN": "secret:imported"}}}
		r := call(o, broker.Request{Operation: wire.Op, Host: "runtime-host", Wire: wire, Approval: d.approve(o, wire)})
		sum := sha256.Sum256([]byte(value))
		if !r.OK || r.Wire == nil || r.Wire.Exec == nil || !strings.HasPrefix(r.Wire.Exec.Stdout, hex.EncodeToString(sum[:])) || strings.Contains(r.Wire.Exec.Stdout, value) {
			t.Fatal("imported source injected incorrectly or escaped redaction")
		}
	}
	verify(a, alpha)
	verify(b, beta)
	// A repeat import reads the original value even though it is already present
	// in the output redactor. The resulting version remains owned by this project.
	if r := call(a, broker.Request{Operation: "secret.set_from_file", Host: "runtime-host", Secret: params("alpha"), Approval: token(a, "alpha")}); !r.OK {
		t.Fatal("repeat import was redacted before value binding")
	}
	verify(a, alpha)

	exact := call(a, broker.Request{Operation: "secret.set_from_file", Host: "runtime-host", Secret: params("exact"), Approval: token(a, "exact")})
	if !exact.OK {
		t.Fatal("exactly 64 KiB source rejected")
	}
	verify(a, strings.Repeat("Q", 65536))
	if r := call(a, broker.Request{Operation: "secret.set_from_file", Host: "runtime-host", Secret: params("alpha"), Approval: token(a, "alpha")}); !r.OK {
		t.Fatal("source reset failed")
	}
	base := call(b, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if !base.OK || base.Wire == nil || base.Wire.Ping == nil {
		t.Fatal("base agent unavailable")
	}
	before := state(a).Secrets[0].Version
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	pending := d.dial(a, d.token(a, "5m"), true)
	if err := pending.enc.Encode(broker.Request{Owner: a, Operation: "secret.set_from_file", Host: "runtime-host", Secret: params("hold")}); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 5*time.Second, "actual SSH source response held", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
	verify(b, beta)
	pending.Close()
	awaitRuntime(t, 5*time.Second, "source approval cancellation drains owner work", func() bool { r := call(a, broker.Request{Operation: "status"}); return r.OK && r.Scheduler.Active == 0 })
	if err := os.Remove(gate); err != nil {
		t.Fatal(err)
	}
	if r := state(a); !r.OK || len(r.Secrets) != 1 || r.Secrets[0].Version != before {
		t.Fatal("canceled source import changed credential")
	}
	after := call(b, broker.Request{Operation: proto.OpPing, Host: "runtime-host", Wire: &proto.Request{Op: proto.OpPing}})
	if !after.OK || after.Wire == nil || after.Wire.Ping == nil || after.Wire.Ping.PID != base.Wire.Ping.PID {
		t.Fatal("source cancellation replaced other-owner base agent")
	}
	d.stop(syscall.SIGKILL)
	d.start()
	connect()
	verify(a, alpha)
	verify(b, beta)
	for _, owner := range []broker.Owner{a, b} {
		if r := call(owner, broker.Request{Operation: "audit_query"}); !r.OK {
			t.Fatal("source audit flush failed")
		}
	}
	for _, suffix := range []string{".audit", ".mutations"} {
		data, err := os.ReadFile(d.socket + suffix)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "secret.set_from_file") {
			t.Fatal("source metadata evidence missing")
		}
		for _, private := range []string{alpha, beta, params("alpha").Path} {
			if strings.Contains(string(data), private) {
				t.Fatal("source details entered low-sensitivity metadata")
			}
		}
	}
	t.Log("real daemon/CLI/MCP/SSH import: separate import/read authority; bounded text validation; unchanged registry on missing/large/binary/short sources; path/content substitution preserves valid token; repeat reads bypass only output redaction; exact-project values; source-response cancellation preserves other-owner exec and prior version; SIGKILL recovery; no source paths/values in audit or mutation records")
}
