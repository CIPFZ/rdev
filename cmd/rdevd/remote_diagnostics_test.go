package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
)

const diagnosticsSSHWrapper = `#!/usr/bin/env python3
import os,sys,time
mode=os.environ['RDEV_DIAGNOSTICS_MODE']
try:
 with open(mode) as f:action=f.read()
except FileNotFoundError:action=''
if action=='probe' and any('uname -s' in arg for arg in sys.argv[1:]):
 sys.stderr.write('diagnostic-private-canary\n');sys.exit(255)
if action=='hold' and any('exec "$1" -state "$2"' in arg for arg in sys.argv[1:]):
 fd=os.open(mode+'.entered',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600);os.close(fd)
 while os.path.exists(mode):time.sleep(.01)
args=[os.environ['RDEV_TEST_REAL_SSH']]
if os.environ.get('RDEV_TEST_SSH_CONFIG'):args+=['-F',os.environ['RDEV_TEST_SSH_CONFIG']]
os.execv(args[0],args+sys.argv[1:])
`

func TestRemoteBrokerConnectionDiagnostics(t *testing.T) {
	d, _, _ := newRemoteRuntime(t)
	a := broker.Owner{ClientID: "connection-shared", ProjectID: "alpha"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "beta"}
	p := broker.NewPolicy()
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{"status", proto.OpPing} {
			if err := p.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
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
	for _, name := range []string{"probe-host", "canceled-host"} {
		alias := map[string]any{}
		for k, v := range hosts.Hosts[0] {
			alias[k] = v
		}
		alias["name"] = name
		hosts.Hosts = append(hosts.Hosts, alias)
	}
	data, _ = json.Marshal(hosts)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	mode := filepath.Join(d.dir, "diagnostics-mode")
	if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(diagnosticsSSHWrapper), 0700); err != nil {
		t.Fatal(err)
	}
	d.env = append(d.env, "RDEV_DIAGNOSTICS_MODE="+mode)
	d.start()
	wa, wb := d.dial(a, d.token(a, "5m"), true), d.dial(b, d.token(b, "5m"), true)
	status := func(owner broker.Owner, w *runtimeWire) observe.ConnectionSnapshot {
		t.Helper()
		r := policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: "status"})
		if !r.OK || r.Scheduler == nil || len(r.Scheduler.Connection.DialFailures) != 9 {
			t.Fatal("connection diagnostic projection missing")
		}
		raw, _ := json.Marshal(r.Scheduler.Connection)
		for _, private := range []string{a.ClientID, a.ProjectID, b.ProjectID, "diagnostic-private-canary", "probe-host", "canceled-host"} {
			if strings.Contains(string(raw), private) {
				t.Fatal("private data entered diagnostic labels")
			}
		}
		return r.Scheduler.Connection
	}
	ping := func(owner broker.Owner, w *runtimeWire, host string) broker.Response {
		return policyRuntimeRequest(t, w, broker.Request{Owner: owner, Operation: proto.OpPing, Host: host, Wire: &proto.Request{Op: proto.OpPing}})
	}
	first := ping(a, wa, "runtime-host")
	if !first.OK || first.Wire == nil || first.Wire.Ping == nil {
		t.Fatal("initial remote ping failed")
	}
	pid := first.Wire.Ping.PID
	shared := ping(b, wb, "runtime-host")
	if !shared.OK || shared.Wire.Ping.PID != pid {
		t.Fatal("other project lost shared base")
	}
	initial := status(a, wa)
	if initial.DialStarted != 1 || initial.DialSucceeded != 1 || initial.DialInFlight != 0 || initial.DialDurationNS == 0 || initial.MaxDialDurationNS == 0 {
		t.Fatalf("initial diagnostic: %+v", initial)
	}
	if status(b, wb).DialStarted != 0 {
		t.Fatal("shared warm request inherited other project's dial")
	}
	if err := os.WriteFile(mode, []byte("probe"), 0600); err != nil {
		t.Fatal(err)
	}
	if ping(a, wa, "probe-host").OK {
		t.Fatal("probe failure was not injected")
	}
	failed := status(a, wa)
	if failed.DialStarted != 2 || failed.DialSucceeded != 1 || failed.DialFailures["probe"] != 1 || failed.DialInFlight != 0 {
		t.Fatalf("probe diagnostic: %+v", failed)
	}
	if err := os.Remove(mode); err != nil {
		t.Fatal(err)
	}
	if !ping(a, wa, "probe-host").OK {
		t.Fatal("real SSH recovery failed")
	}
	if err := os.WriteFile(mode, []byte("hold"), 0600); err != nil {
		t.Fatal(err)
	}
	pending := d.dial(a, d.token(a, "5m"), true)
	if err := pending.enc.Encode(broker.Request{ID: "held-dial", Owner: a, Operation: proto.OpPing, Host: "canceled-host", Wire: &proto.Request{Op: proto.OpPing}}); err != nil {
		t.Fatal(err)
	}
	awaitRuntime(t, 10*time.Second, "real SSH agent-start barrier", func() bool { _, err := os.Stat(mode + ".entered"); return err == nil })
	if status(a, wa).DialInFlight != 1 {
		t.Fatal("active dial not projected")
	}
	survivor := ping(b, wb, "runtime-host")
	if !survivor.OK || survivor.Wire.Ping.PID != pid {
		t.Fatal("held dial affected other-project control")
	}
	pending.Close()
	awaitRuntime(t, 5*time.Second, "dial cancellation accounting", func() bool { return status(a, wa).DialInFlight == 0 })
	final := status(a, wa)
	if final.DialStarted != 4 || final.DialSucceeded != 2 || final.DialFailures["probe"] != 1 || final.DialFailures["canceled"] != 1 || final.RetryAttempts != 0 {
		t.Fatalf("completed diagnostics: %+v", final)
	}
	other := status(b, wb)
	if other.DialStarted != 0 || other.DialSucceeded != 0 || other.RetryAttempts != 0 {
		t.Fatal("dial failures/cancellation crossed project")
	}
	for _, count := range other.DialFailures {
		if count != 0 {
			t.Fatal("failure count crossed project")
		}
	}
	for _, stage := range []string{"agent_lookup", "agent_install"} {
		t.Run(stage, func(t *testing.T) {
			isolated, _, ssh := newRemoteRuntime(t)
			policy := broker.NewPolicy()
			for _, op := range []string{"status", proto.OpPing} {
				if err := policy.Grant(a.Key(), op); err != nil {
					t.Fatal(err)
				}
			}
			if err := policy.Save(isolated.socket + ".policy"); err != nil {
				t.Fatal(err)
			}
			if stage == "agent_lookup" {
				isolated.extraArgs[3] = filepath.Join(isolated.dir, "missing-agent-binaries")
			} else {
				script := "import os,sys\np=os.path.expanduser('~/'+sys.argv[1]);os.makedirs(p+'/rdev-agent')\n"
				if out, err := ssh(script); err != nil {
					t.Fatalf("private remote installation obstruction: %v %s", err, out)
				}
			}
			isolated.start()
			w := isolated.dial(a, isolated.token(a, "5m"), true)
			r := ping(a, w, "runtime-host")
			if r.OK {
				t.Fatal("cold setup obstruction did not fail")
			}
			got := status(a, w)
			if got.DialStarted != 1 || got.DialSucceeded != 0 || got.DialInFlight != 0 || got.DialFailures[stage] != 1 {
				t.Fatalf("%s failure attribution: %+v", stage, got)
			}
			t.Logf("actual daemon/SSH setup failure attributed to %s; one attempt, no success, no active dial", stage)
		})
	}

	t.Logf("real daemon/SSH diagnostics: starts=%d successes=%d probe_failures=%d canceled=%d inflight=%d dial_ns=%d max_dial_ns=%d; recovered SSH and other-project shared PID preserved; bounded labels contain no host/owner/stderr", final.DialStarted, final.DialSucceeded, final.DialFailures["probe"], final.DialFailures["canceled"], final.DialInFlight, final.DialDurationNS, final.MaxDialDurationNS)
}
