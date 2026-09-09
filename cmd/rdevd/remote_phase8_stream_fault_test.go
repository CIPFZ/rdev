package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

// The adapter relays actual OpenSSH bytes. It holds an acknowledged remote
// append at an explicit barrier, then delays delivery or closes only the
// agent-to-client stream (with either zero or a partial terminal frame).
// This is an SSH application-stream half-close, not a TCP packet-loss test.
const phase8StreamFaultSSH = `#!/usr/bin/env python3
import os,sys,json,subprocess,threading,time
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'):args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
p=subprocess.Popen(args+sys.argv[1:],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=sys.stderr.buffer)
gate=os.environ['RDEV_STREAM_FAULT_GATE'];lock=threading.Lock();input_done=threading.Event()
def record(suffix,value):
 fd=os.open(gate+suffix,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
 with os.fdopen(fd,'w') as f:json.dump(value,f)
def inbound():
 try:
  for line in sys.stdin.buffer:
   with lock:p.stdin.write(line);p.stdin.flush()
 except (BrokenPipeError,OSError,ValueError):pass
 finally:
  with lock:
   try:p.stdin.close()
   except (BrokenPipeError,OSError,ValueError):pass
  input_done.set()
threading.Thread(target=inbound,daemon=True).start()
closed=False
try:
 for line in p.stdout:
  try:
   r=json.loads(line)
   with open(gate) as f:rule=json.load(f)
  except (ValueError,FileNotFoundError):r={};rule={}
  if r.get('terminal') and r.get('operation_id')==rule.get('operation_id') and rule:
   mode=rule['mode']
   record('.entered',{'terminal_ok':r.get('ok') is True,'mode':mode,'ssh_alive':p.poll() is None})
   deadline=time.monotonic()+15
   while os.path.exists(gate) and time.monotonic()<deadline:time.sleep(.01)
   if os.path.exists(gate):raise RuntimeError('response barrier deadline')
   if mode!='delay':
    prefix=line[:max(1,len(line)//2)] if mode=='partial' else b''
    if b'\n' in prefix:raise RuntimeError('partial frame unexpectedly complete')
    if prefix:os.write(1,prefix)
    # Publish the intended cut before EOF makes the broker reap this adapter.
    record('.cut',{'mode':mode,'terminal_bytes':len(line),'delivered_bytes':len(prefix),'ssh_alive':p.poll() is None})
    os.close(1);closed=True
    input_done.wait(5)
    break
  sys.stdout.buffer.write(line);sys.stdout.buffer.flush()
except BrokenPipeError:closed=True
finally:
 if closed:
  with lock:
   try:p.stdin.close()
   except (BrokenPipeError,OSError,ValueError):pass
 try:p.wait(timeout=5)
 except subprocess.TimeoutExpired:
  p.terminate()
  try:p.wait(timeout=2)
  except subprocess.TimeoutExpired:p.kill();p.wait()
sys.exit(p.returncode)
`

func TestRemotePhase8StreamFaults(t *testing.T) {
	for _, mode := range []string{"delay", "half-close", "partial"} {
		t.Run(mode, func(t *testing.T) {
			d, namespace, sshRun := newRemoteRuntime(t)
			a := broker.Owner{ClientID: "phase8-stream", ProjectID: "writer"}
			b := broker.Owner{ClientID: a.ClientID, ProjectID: "observer"}
			policy := broker.NewPolicy()
			for owner, operations := range map[broker.Owner][]string{
				runtimeApprovalAdmin(): {"approval.create"},
				a:                      {proto.OpPing, proto.OpWriteFile, "mutation.status"},
				b:                      {proto.OpPing, "mutation.status"},
			} {
				for _, op := range operations {
					if err := policy.Grant(owner.Key(), op); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := policy.Save(d.socket + ".policy"); err != nil {
				t.Fatal(err)
			}
			gate := filepath.Join(d.dir, "stream-fault")
			t.Cleanup(func() { _ = os.Remove(gate) })
			if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(phase8StreamFaultSSH), 0700); err != nil {
				t.Fatal(err)
			}
			d.env = append(d.env, "RDEV_STREAM_FAULT_GATE="+gate)
			d.start()
			wa := d.dial(a, d.token(a, "5m"), true)
			inspect := d.dial(a, d.token(a, "5m"), true)
			wb := d.dial(b, d.token(b, "5m"), true)
			remoteWireCall(t, wa, a, &proto.Request{Op: proto.OpPing})
			write := &proto.Request{Op: proto.OpWriteFile, OperationID: "op_phase8_stream_" + strings.ReplaceAll(mode, "-", "_"), Cat: &proto.WriteParams{Path: namespace + "/marker", Content: "x", Append: true}}
			data, _ := json.Marshal(map[string]string{"mode": mode, "operation_id": write.OperationID})
			if err := os.WriteFile(gate, data, 0600); err != nil {
				t.Fatal(err)
			}
			request := broker.Request{ID: "stream-fault", Owner: a, Host: "runtime-host", Operation: write.Op, Wire: write, Approval: d.approve(a, write)}
			_ = wa.SetDeadline(time.Now().Add(20 * time.Second))
			if err := wa.enc.Encode(request); err != nil {
				t.Fatal(err)
			}
			awaitRuntime(t, 10*time.Second, "real terminal response held", func() bool {
				data, err := os.ReadFile(gate + ".entered")
				var proof struct {
					OK    bool   `json:"terminal_ok"`
					Mode  string `json:"mode"`
					Alive bool   `json:"ssh_alive"`
				}
				return err == nil && json.Unmarshal(data, &proof) == nil && proof.OK && proof.Mode == mode && proof.Alive
			})
			query := func(w *runtimeWire, owner broker.Owner) broker.Response {
				return policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "mutation.status", MutationID: write.OperationID})
			}
			if r := query(inspect, a); !r.OK || r.Mutation == nil || r.Mutation.State != "dispatched" {
				t.Fatal("held successful remote terminal was acknowledged early")
			}
			if r := query(wb, b); r.OK || r.Mutation != nil {
				t.Fatal("held mutation exposed to another project")
			}
			// This ping uses another real lane and must not wait for the held
			// bulk response. The test's barrier, not a random sleep, proves overlap.
			remoteWireCall(t, wb, b, &proto.Request{Op: proto.OpPing})
			if err := os.Remove(gate); err != nil {
				t.Fatal(err)
			}
			var response broker.Response
			if err := wa.dec.Decode(&response); err != nil {
				t.Fatalf("fault did not produce a bounded broker response: %v", err)
			}
			want := "completed"
			if mode == "delay" {
				if !response.OK || response.Wire == nil || !response.Wire.OK {
					t.Fatal("released complete terminal lost its actual success")
				}
			} else {
				want = "ambiguous"
				if response.OK || response.Mutation == nil || response.Mutation.State != want || response.ErrorEnvelope == nil || response.ErrorEnvelope.Validate() != nil || response.ErrorEnvelope.Code != proto.CodeAmbiguousOutcome || response.ErrorEnvelope.ExecutionState != proto.StatePossiblyExecuted || response.ErrorEnvelope.OperationID != write.OperationID {
					state, code, execution := "missing", "missing", "missing"
					if response.Mutation != nil {
						state = response.Mutation.State
					}
					if response.ErrorEnvelope != nil {
						code = string(response.ErrorEnvelope.Code)
						execution = string(response.ErrorEnvelope.ExecutionState)
					}
					t.Fatalf("lost terminal outcome: ok=%t mutation=%s code=%s execution=%s", response.OK, state, code, execution)
				}
				data, err := os.ReadFile(gate + ".cut")
				var cut struct {
					Mode      string `json:"mode"`
					Total     int    `json:"terminal_bytes"`
					Delivered int    `json:"delivered_bytes"`
					Alive     bool   `json:"ssh_alive"`
				}
				if err != nil || json.Unmarshal(data, &cut) != nil || cut.Mode != mode || !cut.Alive || cut.Total == 0 || cut.Delivered >= cut.Total || mode == "partial" && cut.Delivered == 0 || mode == "half-close" && cut.Delivered != 0 {
					t.Fatal("real stream half-close/partial terminal proof missing")
				}
			}
			d.stop(syscall.SIGKILL)
			d.start()
			inspect = d.dial(a, d.token(a, "5m"), true)
			if r := query(inspect, a); !r.OK || r.Mutation == nil || r.Mutation.State != want {
				t.Fatal("broker restart changed the retained mutation outcome")
			}
			request.Approval = d.approve(a, write)
			if r := policyRuntimeRequest(t, inspect, request); r.OK || r.Mutation == nil || r.Mutation.State != want {
				t.Fatal("same mutation was replayed after stream recovery")
			}
			remoteWireCall(t, inspect, a, &proto.Request{Op: proto.OpPing})
			out, err := sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/marker')\nassert open(p).read()=='x'\nprint('once')\n")
			if err != nil || strings.TrimSpace(string(out)) != "once" {
				t.Fatal("fault or recovery duplicated the actual append")
			}
			t.Logf("real SSH stream %s: terminal barrier, independent control progress, retained %s after SIGKILL, no replay, exact marker x", mode, want)
		})
	}
}
