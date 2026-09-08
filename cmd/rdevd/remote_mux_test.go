package main

import (
	"bufio"
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

func TestRemoteBrokerSaturatedControlMaster(t *testing.T) {
	for _, installRace := range []bool{false, true} {
		name := "probe_full"
		if installRace {
			name = "install_race"
		}
		t.Run(name, func(t *testing.T) { remoteMuxCapacity(t, installRace) })
	}
}
func remoteMuxCapacity(t *testing.T, installRace bool) {
	d, namespace, sshRun := newRemoteRuntime(t)
	remote := os.Getenv("RDEV_TEST_REMOTE")
	if remote == "" {
		remote = "service-deploy"
	}
	// This test owns its master and all saturation sessions. The daemon shares
	// only this private control directory; existing user masters are untouched.
	ctlDir := filepath.Join(d.dir, "rdev-ctl")
	if err := os.Mkdir(ctlDir, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:0", remote)))
	ctl := filepath.Join(ctlDir, hex.EncodeToString(sum[:])[:16])
	d.env = append(d.env, "TMPDIR="+d.dir)
	sshPath, err := exec.LookPath("ssh")
	if err != nil {
		t.Fatal(err)
	}
	base := []string{}
	if config := os.Getenv("RDEV_TEST_SSH_CONFIG"); config != "" {
		base = append(base, "-F", config)
	}
	// Read the real server limit rather than changing sshd policy for the test.
	out, err := sshRun("import subprocess,json\ns=subprocess.check_output(['/usr/sbin/sshd','-T'],text=True)\nprint(json.dumps(int(next(x.split()[1] for x in s.splitlines() if x.startswith('maxsessions ')))))\n")
	var limit int
	if err != nil || json.Unmarshal(out, &limit) != nil || limit < 1 || limit > 32 {
		t.Fatal("need readable sshd maxsessions from 1 to 32 for bounded saturation fixture")
	}
	alive := map[*exec.Cmd]<-chan error{}
	track := func(cmd *exec.Cmd) {
		done := make(chan error)
		alive[cmd] = done
		go func() { _ = cmd.Wait(); close(done) }()
		t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })
	}
	t.Cleanup(func() {
		_, err := sshRun(remoteMuxCleanupScript)
		if err != nil {
			t.Error("saturation helper cleanup failed")
		}
	})
	spawn := func(args ...string) *exec.Cmd {
		cmd := exec.Command(sshPath, append(append([]string{}, base...), args...)...)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		track(cmd)
		return cmd
	}
	master := spawn("-N", "-o", "BatchMode=yes", "-o", "ControlMaster=yes", "-o", "ControlPersist=no", "-o", "ControlPath="+ctl, remote)
	awaitRuntime(t, 5*time.Second, "private SSH master", func() bool { st, err := os.Stat(ctl); return err == nil && st.Mode()&os.ModeSocket != 0 })
	channels := make([]*exec.Cmd, 0, limit)
	startChannel := func() {
		args := append(append([]string{}, base...), "-T", "-o", "BatchMode=yes", "-o", "ControlMaster=no", "-o", "ControlPath="+ctl, remote, `python3 -c 'import time; print("ready",flush=True); time.sleep(180)' `+"rdev-mux-"+namespace)
		cmd := exec.Command(sshPath, args...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		track(cmd)
		ready := make(chan bool, 1)
		go func() {
			line, err := bufio.NewReader(stdout).ReadString('\n')
			ready <- err == nil && line == "ready\n"
		}()
		select {
		case ok := <-ready:
			if !ok {
				t.Fatal("mux saturation channel did not start")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("mux saturation channel timed out")
		}
		channels = append(channels, cmd)
	}
	initial := limit
	if installRace {
		initial--
	}
	for range initial {
		startChannel()
	}
	gate := filepath.Join(d.dir, "install-gate")
	if installRace {
		if err := os.WriteFile(gate, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d.dir, "tools", "ssh"), []byte(muxInstallBarrier), 0700); err != nil {
			t.Fatal(err)
		}
		d.env = append(d.env, "RDEV_MUX_INSTALL_GATE="+gate)
	}
	a := broker.Owner{ClientID: "mux-shared", ProjectID: "a"}
	b := broker.Owner{ClientID: a.ClientID, ProjectID: "b"}
	p := broker.NewPolicy()
	if err := p.Grant(runtimeApprovalAdmin().Key(), "approval.create"); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []broker.Owner{a, b} {
		for _, op := range []string{proto.OpPing, proto.OpWriteFile} {
			if err := p.Grant(owner.Key(), op); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	d.start()
	wa := d.dial(a, d.token(a, "5m"), true)
	wb := d.dial(b, d.token(b, "5m"), true)
	var first *proto.Response
	if installRace {
		_ = wa.SetDeadline(time.Now().Add(30 * time.Second))
		if err := wa.enc.Encode(broker.Request{Owner: a, Host: "runtime-host", Operation: proto.OpPing, Wire: &proto.Request{Op: proto.OpPing}}); err != nil {
			t.Fatal(err)
		}
		awaitRuntime(t, 10*time.Second, "probe succeeded before installation barrier", func() bool { _, err := os.Stat(gate + ".entered"); return err == nil })
		startChannel()
		if err := os.Remove(gate); err != nil {
			t.Fatal(err)
		}
		var response broker.Response
		if err := wa.dec.Decode(&response); err != nil || !response.OK || response.Wire == nil || !response.Wire.OK {
			t.Fatalf("installation lost payload after successful probe: %v %s", err, response.Error)
		}
		first = response.Wire
	} else {
		first = remoteWireCall(t, wa, a, &proto.Request{Op: proto.OpPing})
	}
	write := &proto.Request{Op: proto.OpWriteFile, OperationID: "op_runtime_mux_once", Cat: &proto.WriteParams{Path: namespace + "/mux-proof", Content: "once", Append: true}}
	response := policyRuntimeRequest(t, wa, broker.Request{Owner: a, Host: "runtime-host", Operation: write.Op, Wire: write, Approval: d.approve(a, write)})
	if !response.OK || response.Wire == nil || !response.Wire.OK {
		t.Fatalf("bulk setup under saturated mux failed: %s", response.Error)
	}
	second := remoteWireCall(t, wb, b, &proto.Request{Op: proto.OpPing})
	if first.Ping == nil || second.Ping == nil || first.Ping.PID != second.Ping.PID || first.Ping.CallerID == second.Ping.CallerID {
		t.Fatal("fallback lost shared base or principal isolation")
	}
	if count := countDaemonSSHChildren(t, d.cmd.Process.Pid); count != 2 {
		t.Fatalf("expected one base and one bulk SSH, got %d", count)
	}
	for _, cmd := range append(channels, master) {
		select {
		case err := <-alive[cmd]:
			t.Fatalf("fallback closed another SSH session: %v", err)
		default:
		}
		if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
			t.Fatal("fallback killed another SSH session")
		}
	}
	out, err = sshRun("import os,sys\np=os.path.expanduser('~/'+sys.argv[1])\nassert open(p+'/mux-proof').read()=='once'\nprint('once')\n")
	if err != nil || strings.TrimSpace(string(out)) != "once" {
		t.Fatal("saturation duplicated or lost append")
	}
	t.Logf("actual private ControlMaster install_race=%v saturated with %d live sessions: base/bulk setup completed under mux saturation, shared base/principal isolation retained, append exactly once, existing master and all saturation sessions alive", installRace, limit)
}

const remoteMuxCleanupScript = `import os,sys,signal
tag=('rdev-mux-'+sys.argv[1]).encode()
for name in os.listdir('/proc'):
 if not name.isdigit():continue
 try:
  args=open('/proc/'+name+'/cmdline','rb').read().split(bytes([0]))
  if tag in args:os.kill(int(name),signal.SIGKILL)
 except (FileNotFoundError,ProcessLookupError,PermissionError):pass
`

const muxInstallBarrier = `#!/usr/bin/env python3
import os,sys,time
gate=os.environ['RDEV_MUX_INSTALL_GATE']
if any('.rdev-agent.stage-' in arg for arg in sys.argv[1:]):
 fd=os.open(gate+'.entered',os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
 os.close(fd)
 while os.path.exists(gate):time.sleep(.01)
args=[os.environ['RDEV_TEST_REAL_SSH']]
config=os.environ.get('RDEV_TEST_SSH_CONFIG','')
if config:args+=['-F',config]
os.execv(args[0],args+sys.argv[1:])
`
