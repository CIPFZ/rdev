package main

// These tests execute the built daemon and the administrator CLI, use real Unix
// sockets, and send real signals. No dispatcher or lifecycle code is replaced.
import (
	"encoding/json"
	"fmt"
	"net"
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
)

type runtimeDaemon struct {
	t                            *testing.T
	bin, dir, socket, ready, key string
	cmd                          *exec.Cmd
	done                         chan error
}

func newRuntimeDaemon(t *testing.T, bin string) *runtimeDaemon {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rdev-runtime-")
	if err != nil {
		t.Fatal(err)
	}
	d := &runtimeDaemon{t: t, bin: bin, dir: dir, socket: filepath.Join(dir, "broker.sock"), ready: filepath.Join(dir, "ready"), key: filepath.Join(dir, "principal.key")}
	t.Cleanup(func() { d.stop(syscall.SIGKILL); os.RemoveAll(dir) })
	d.admin("principal-keygen", "-out", d.key)
	return d
}

func (d *runtimeDaemon) admin(args ...string) string {
	d.t.Helper()
	cmd := exec.Command(d.bin, args...)
	out, err := cmd.CombinedOutput()
	// Never print credential output on an error path.
	if err != nil {
		d.t.Fatalf("administrator command %s failed: %v", args[0], err)
	}
	return strings.TrimSpace(string(out))
}

func (d *runtimeDaemon) args() []string {
	return []string{"-socket", d.socket, "-ready-file", d.ready, "-principal-key-file", d.key}
}

func (d *runtimeDaemon) start() {
	d.t.Helper()
	logFile, err := os.OpenFile(filepath.Join(d.dir, "daemon.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		d.t.Fatal(err)
	}
	d.cmd = exec.Command(d.bin, d.args()...)
	d.cmd.Dir = d.dir
	d.cmd.Stdout, d.cmd.Stderr = logFile, logFile
	if err := d.cmd.Start(); err != nil {
		logFile.Close()
		d.t.Fatal(err)
	}
	d.done = make(chan error, 1)
	cmd, done := d.cmd, d.done
	go func() { done <- cmd.Wait(); logFile.Close() }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			d.cmd = nil
			logData, _ := os.ReadFile(filepath.Join(d.dir, "daemon.log"))
			d.t.Fatalf("daemon exited before READY: %v\n%s", err, logData)
		default:
		}
		if data, err := os.ReadFile(d.ready); err == nil && string(data) == "READY\n" {
			// A crash may have left a readiness file; demand a live handshake too.
			conn, err := net.DialTimeout("unix", d.socket, 100*time.Millisecond)
			if err == nil {
				conn.Close()
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	d.t.Fatal("daemon did not become ready")
}

func (d *runtimeDaemon) stop(sig syscall.Signal) {
	d.t.Helper()
	if d.cmd == nil {
		return
	}
	_ = d.cmd.Process.Signal(sig)
	select {
	case err := <-d.done:
		if sig == syscall.SIGTERM && err != nil {
			d.t.Errorf("graceful stop: %v", err)
		}
	case <-time.After(12 * time.Second):
		_ = d.cmd.Process.Kill()
		<-d.done
		d.t.Error("daemon exceeded bounded shutdown deadline")
	}
	d.cmd = nil
}

func (d *runtimeDaemon) token(owner broker.Owner, ttl string) string {
	return d.admin("principal-token", "-key-file", d.key, "-client-id", owner.ClientID, "-project-id", owner.ProjectID, "-ttl", ttl)
}

type runtimeWire struct {
	net.Conn
	enc *json.Encoder
	dec *json.Decoder
}

func (d *runtimeDaemon) dial(owner broker.Owner, token string, allowed bool) *runtimeWire {
	d.t.Helper()
	conn, err := net.DialTimeout("unix", d.socket, time.Second)
	if err != nil {
		d.t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	w := &runtimeWire{Conn: conn, enc: json.NewEncoder(conn), dec: json.NewDecoder(conn)}
	d.t.Cleanup(func() { conn.Close() })
	if err := w.enc.Encode(proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion, ClientID: owner.ClientID, ProjectID: owner.ProjectID, PrincipalToken: token}); err != nil {
		d.t.Fatal(err)
	}
	var hello proto.BrokerHelloResponse
	if err := w.dec.Decode(&hello); err != nil {
		d.t.Fatal(err)
	}
	if hello.OK != allowed {
		d.t.Fatalf("handshake allowed=%v, want %v", hello.OK, allowed)
	}
	return w
}

func (w *runtimeWire) call(t *testing.T, owner broker.Owner, operation string) broker.Response {
	t.Helper()
	if err := w.enc.Encode(broker.Request{ID: "runtime", Owner: owner, Operation: operation}); err != nil {
		t.Fatal(err)
	}
	var response broker.Response
	if err := w.dec.Decode(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

func TestDaemonRuntimeLifecycle(t *testing.T) {
	bin := os.Getenv("RDEV_TEST_DAEMON_BINARY")
	if bin == "" {
		bin = filepath.Join(t.TempDir(), "rdevd")
		build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", bin, ".")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build daemon: %v\n%s", err, out)
		}
	}
	owner := broker.Owner{ClientID: "runtime-a", ProjectID: "phase5"}
	t.Run("principal_provision_expire_rotate", func(t *testing.T) {
		d := newRuntimeDaemon(t, bin)
		policy := broker.NewPolicy()
		policy.Grant(owner.Key(), "status")
		if err := policy.Save(d.socket + ".policy"); err != nil {
			t.Fatal(err)
		}
		d.start()
		token := d.token(owner, "1h")
		w := d.dial(owner, token, true)
		if !w.call(t, owner, "status").OK {
			t.Fatal("provisioned owner denied")
		}
		d.dial(owner, "", false).Close()
		d.dial(broker.Owner{ClientID: "runtime-b", ProjectID: owner.ProjectID}, token, false).Close()
		d.dial(broker.Owner{ClientID: owner.ClientID, ProjectID: "other"}, token, false).Close()
		if w.call(t, broker.Owner{ClientID: "runtime-b", ProjectID: owner.ProjectID}, "status").OK {
			t.Fatal("owner switch allowed")
		}
		other := broker.Owner{ClientID: "runtime-b", ProjectID: owner.ProjectID}
		denied := d.dial(other, d.token(other, "1h"), true)
		for _, op := range []string{"ping", "exec", "read_file", "job_list", "secret.use", "fleet.execute", "policy.grant"} {
			if denied.call(t, other, op).OK {
				t.Fatalf("default deny bypassed for %s", op)
			}
		}
		short := d.token(owner, "2s")
		expiring := d.dial(owner, short, true)
		started := time.Now()
		var response broker.Response
		if err := expiring.dec.Decode(&response); err == nil {
			t.Fatal("idle credential did not expire")
		}
		if time.Since(started) > 3*time.Second {
			t.Fatal("expiry did not close established session on time")
		}
		d.dial(owner, short, false).Close()
		// An invalid config must also prevent a simultaneous key rotation.
		d.admin("principal-keygen", "-out", d.key+".next")
		if err := os.Rename(d.key+".next", d.key); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(d.socket+".json", []byte(`{"max_hosts":0}`), 0o600); err != nil {
			t.Fatal(err)
		}
		_ = d.cmd.Process.Signal(syscall.SIGHUP)
		waitDaemonLog(t, d, "reload rejected")
		if !w.call(t, owner, "status").OK {
			t.Fatal("invalid reload replaced old authority")
		}
		if err := os.WriteFile(d.socket+".json", []byte(`{"max_hosts":12,"idle_ttl":1000000000}`), 0o600); err != nil {
			t.Fatal(err)
		}
		_ = d.cmd.Process.Signal(syscall.SIGHUP)
		waitDaemonLog(t, d, "configuration reloaded")
		if err := w.dec.Decode(&response); err == nil {
			t.Fatal("rotation left old established session usable")
		}
		d.dial(owner, token, false).Close()
		next := d.token(owner, "1h")
		if !d.dial(owner, next, true).call(t, owner, "status").OK {
			t.Fatal("rotated credential denied")
		}
		d.stop(syscall.SIGTERM)
		d.start()
		d.dial(owner, token, false).Close()
		if !d.dial(owner, next, true).call(t, owner, "status").OK {
			t.Fatal("rotation did not persist across restart")
		}
		t.Log("real daemon: provisioning, principal/project binding, default deny, expiry, atomic config/key reload, established-session revocation, restart passed")
	})
	t.Run("single_instance_crash_signal", func(t *testing.T) {
		d := newRuntimeDaemon(t, bin)
		d.start()
		if runtime.GOOS == "linux" && os.Getuid() == 0 {
			python, err := exec.LookPath("python3")
			if err != nil {
				t.Fatal("python3 is needed for the foreign-UID runtime check")
			}
			probe := exec.Command(python, "-c", "import socket,sys\ns=socket.socket(socket.AF_UNIX)\ntry: s.connect(sys.argv[1])\nexcept PermissionError: sys.exit(0)\nsys.exit(1)", d.socket)
			probe.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65534, Gid: 65534}}
			if out, err := probe.CombinedOutput(); err != nil {
				t.Fatalf("foreign UID socket isolation: %v %s", err, out)
			}
			t.Log("real UID 65534 process denied access to root-owned broker socket")
		}
		second := exec.Command(bin, d.args()...)
		if err := second.Run(); err == nil {
			t.Fatal("second instance started")
		}
		if _, err := os.Stat(d.ready); err != nil {
			t.Fatal("second instance removed live readiness")
		}
		for _, path := range []string{d.socket, d.ready, d.key} {
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if st.Mode().Perm() != 0o600 {
				t.Fatalf("%s mode=%o", filepath.Base(path), st.Mode().Perm())
			}
		}
		d.stop(syscall.SIGKILL)
		d.start()
		d.dial(owner, d.token(owner, "1h"), true).Close()
		d.stop(syscall.SIGTERM)
		for _, path := range []string{d.ready, d.socket} {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("shutdown left %s", filepath.Base(path))
			}
		}
		t.Log("real daemon: private socket, duplicate-start readiness, SIGKILL stale-socket recovery and SIGTERM cleanup passed")
	})
	t.Run("audit_owner_query_restart", func(t *testing.T) {
		d := newRuntimeDaemon(t, bin)
		legacy, err := json.Marshal(broker.AuditEvent{At: time.Now(), Owner: "a b c", Operation: "status", Result: "accepted"})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(d.socket+".audit", append(legacy, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		// These two owners previously collapsed to the same sanitized string.
		owners := []broker.Owner{{ClientID: "a b", ProjectID: "c"}, {ClientID: "a", ProjectID: "b c"}}
		policy := broker.NewPolicy()
		for _, owner := range owners {
			policy.Grant(owner.Key(), "status")
			policy.Grant(owner.Key(), "audit_query")
		}
		if err := policy.Save(d.socket + ".policy"); err != nil {
			t.Fatal(err)
		}
		d.start()
		const canary = "private-audit-canary-without-redaction-prefix"
		for _, owner := range owners {
			w := d.dial(owner, d.token(owner, "1h"), true)
			if !w.call(t, owner, "status").OK {
				t.Fatal("status failed")
			}
			if w.call(t, owner, canary).OK {
				t.Fatal("unknown operation admitted")
			}
			w.Close()
		}
		check := func() {
			for _, owner := range owners {
				w := d.dial(owner, d.token(owner, "1h"), true)
				response := w.call(t, owner, "audit_query")
				if !response.OK || len(response.Audit) != 2 {
					t.Fatalf("audit history missing or broadened: ok=%v count=%d", response.OK, len(response.Audit))
				}
				if !response.AuditIncomplete {
					t.Fatal("ambiguous legacy identity omitted without an explicit marker")
				}
				for _, event := range response.Audit {
					if event.Schema != 1 || event.Owner != broker.AuditOwnerID(owner.Key()) || event.At.IsZero() {
						t.Fatal("audit owner or timestamp mismatch")
					}
				}
				w.Close()
			}
		}
		check()
		d.stop(syscall.SIGTERM)
		d.start()
		check()
		data, err := os.ReadFile(d.socket + ".audit")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), canary) {
			t.Fatal("untrusted operation text leaked into audit file")
		}
		t.Log("real daemon: colliding owner display names isolated, decision/result timestamps queried before and after restart, untrusted text excluded")
	})
	t.Run("startup_fail_closed", func(t *testing.T) {
		for _, kind := range []string{"missing_key", "weak_key", "public_key", "symlink_key", "bad_config", "null_config", "unknown_config_field", "bad_policy", "bad_jobs"} {
			t.Run(kind, func(t *testing.T) {
				d := newRuntimeDaemon(t, bin)
				switch kind {
				case "missing_key":
					os.Remove(d.key)
				case "weak_key":
					os.WriteFile(d.key, []byte("short"), 0o600)
				case "public_key":
					os.Chmod(d.key, 0o644)
				case "symlink_key":
					os.Rename(d.key, d.key+".real")
					os.Symlink(d.key+".real", d.key)
				case "bad_config":
					os.WriteFile(d.socket+".json", []byte(`{"max_hosts":0}`), 0o600)
				case "null_config":
					os.WriteFile(d.socket+".json", []byte(`null`), 0o600)
				case "unknown_config_field":
					os.WriteFile(d.socket+".json", []byte(`{"typo":1}`), 0o600)
				case "bad_policy":
					os.WriteFile(d.socket+".policy", []byte(`invalid-policy`), 0o600)
				case "bad_jobs":
					os.WriteFile(d.socket+".jobs", []byte(`invalid-jobs`), 0o600)
				}
				if err := os.WriteFile(d.ready, []byte("STALE\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(bin, d.args()...)
				cmd.Dir = d.dir
				if err := cmd.Run(); err == nil {
					t.Fatal("invalid startup succeeded")
				}
				if _, err := os.Stat(d.ready); !os.IsNotExist(err) {
					t.Fatal("invalid startup published readiness")
				}
				if kind == "bad_policy" || kind == "bad_jobs" {
					suffix := strings.TrimPrefix(kind, "bad_")
					data, _ := os.ReadFile(d.socket + "." + suffix)
					if string(data) != "invalid-"+suffix {
						t.Fatal("invalid persisted state was overwritten")
					}
				}
			})
		}
	})
}

func waitDaemonLog(t *testing.T, d *runtimeDaemon, marker string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(filepath.Join(d.dir, "daemon.log"))
		if strings.Contains(string(data), marker) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(fmt.Sprintf("daemon did not report %s", marker))
}

func TestSmokeFailsWhenDaemonNeverCreatesSocket(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "failed-daemon")
	// Simulate a process publishing READY without opening its socket. The old
	// smoke command continued past failed assertions and printed success.
	stub := "#!/bin/sh\nif [ \"$1\" = principal-keygen ]; then exit 0; fi\nwhile [ $# -gt 0 ]; do if [ \"$1\" = -ready-file ]; then shift; printf 'READY\\n' >\"$1\"; fi; shift; done\nexit 0\n"
	if err := os.WriteFile(bin, []byte(stub), 0o700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("sh", filepath.Join(repoRoot(t), "scripts", "rdevd-smoke-runtime.sh"), bin, filepath.Join(dir, "run"))
	out, err := cmd.CombinedOutput()
	if err == nil || strings.Contains(string(out), "smoke: ok") {
		t.Fatalf("failed smoke reported success: %v %s", err, out)
	}
}
