//go:build linux || darwin

package agentinstall

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/proto"
	"golang.org/x/sys/unix"
)

// These are deterministic fault-window unit fixtures. The production agent's
// real SSH health/upgrade is exercised separately by the runtime harness.
func fixture(t *testing.T, tag, failActive string) (string, artifact.Decision) {
	t.Helper()
	dir := t.TempDir()
	r := proto.Response{ID: "rdev-upgrade-health", OK: true, Ping: &proto.PingResult{Version: proto.Version, MinVersion: proto.MinVersion, OS: runtime.GOOS, Arch: runtime.GOARCH, Features: proto.SupportedFeatures()}}
	data, _ := json.Marshal(r)
	script := "#!/bin/sh\n# " + tag + "\n"
	if failActive != "" {
		script += "case \"$0\" in */rdev-agent) exit 8;; esac\n"
	}
	script += "IFS= read -r request\nprintf '%s\\n' '" + string(data) + "'\n"
	path := filepath.Join(dir, "agent")
	if e := os.WriteFile(path, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	return path, artifact.Decision{Channel: "dev", Digest: artifact.Hash([]byte(script)), Unsigned: true}
}
func rootDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	return dir
}
func installed(t *testing.T, root string) string {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(root, targetName))
	if errors.Is(e, os.ErrNotExist) {
		return ""
	}
	if e != nil {
		t.Fatal(e)
	}
	return artifact.Hash(b)
}
func initial(t *testing.T) (string, artifact.Decision) {
	t.Helper()
	root := rootDir(t)
	p, d := fixture(t, "old", "")
	if e := Install(context.Background(), root, p, d, ""); e != nil {
		t.Fatal(e)
	}
	return root, d
}

func TestHealthFailureDoesNotPublishAndPostSwitchFailureRollsBack(t *testing.T) {
	root, old := initial(t)
	bad := filepath.Join(t.TempDir(), "bad")
	_ = os.WriteFile(bad, []byte("#!/bin/sh\nexit 1\n"), 0700)
	d := artifact.Decision{Channel: "dev", Unsigned: true, Digest: artifact.Hash([]byte("#!/bin/sh\nexit 1\n"))}
	if e := Install(context.Background(), root, bad, d, old.Digest); e == nil {
		t.Fatal("bad candidate installed")
	}
	if installed(t, root) != old.Digest {
		t.Fatal("pre-health failure changed active")
	}
	p, next := fixture(t, "bad-only-after-switch", "yes")
	e := Install(context.Background(), root, p, next, old.Digest)
	var te *Error
	if !errors.As(e, &te) || te.Stage != "rolled_back" || te.State != "not_sent" {
		t.Fatalf("rollback result: %v", e)
	}
	if installed(t, root) != old.Digest {
		t.Fatal("post-health failure did not restore known-good bytes")
	}
	if e = Health(context.Background(), filepath.Join(root, targetName), root); e != nil {
		t.Fatal(e)
	}
}
func TestDurableWindowsReconcileWithoutReplayingPublication(t *testing.T) {
	for _, phase := range []string{"prepared", "verified", "switching", "published", "committed"} {
		t.Run(phase, func(t *testing.T) {
			root, old := initial(t)
			p, next := fixture(t, "next", "")
			fail := errors.New("injected interruption")
			err := install(context.Background(), root, p, next, old.Digest, func(point string) error {
				if point == phase {
					return fail
				}
				return nil
			})
			if err == nil {
				t.Fatal("fault window not reached")
			}
			if err = Install(context.Background(), root, p, next, installed(t, root)); err != nil {
				t.Fatalf("recover %s: %v", phase, err)
			}
			if installed(t, root) != next.Digest {
				t.Fatal("wrong active bytes")
			}
			b, e := os.ReadFile(filepath.Join(root, previousName))
			if e != nil || artifact.Hash(b) != old.Digest {
				t.Fatalf("known-good backup lost: %v", e)
			}
			for _, name := range []string{rollbackName, newName} {
				if _, e = os.Lstat(filepath.Join(root, name)); !errors.Is(e, os.ErrNotExist) {
					t.Fatalf("scratch not bounded: %s %v", name, e)
				}
			}
		})
	}
}
func TestLockUsesActualNamespaceAndCAS(t *testing.T) {
	root, old := initial(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, unlock, e := acquire(ctx, root)
	if e != nil {
		t.Fatal(e)
	}
	short, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	if _, release, e := acquire(short, root+"/."); e == nil {
		release()
		unlock()
		t.Fatal("same root bypassed lock")
	}
	other := rootDir(t)
	_, release, e := acquire(ctx, other)
	if e != nil {
		unlock()
		t.Fatalf("independent namespace blocked: %v", e)
	}
	release()
	unlock()
	p, next := fixture(t, "next", "")
	if e = Install(ctx, root, p, next, old.Digest); e != nil {
		t.Fatal(e)
	}
	p2, next2 := fixture(t, "another", "")
	if e = Install(ctx, root, p2, next2, old.Digest); e == nil {
		t.Fatal("stale observed digest overwrote newer target")
	}
	if installed(t, root) != next.Digest {
		t.Fatal("CAS changed winner")
	}
}
func TestCorruptFutureJournalAndStatePreserved(t *testing.T) {
	for _, name := range []string{journalName, "manifest.json"} {
		t.Run(name, func(t *testing.T) {
			root, old := initial(t)
			bad := []byte(`{"schema_version":999}`)
			if e := os.WriteFile(filepath.Join(root, name), bad, 0600); e != nil {
				t.Fatal(e)
			}
			p, next := fixture(t, "next", "")
			if e := Install(context.Background(), root, p, next, old.Digest); e == nil {
				t.Fatal("future state overwritten")
			}
			if installed(t, root) != old.Digest {
				t.Fatal("future state changed active")
			}
			b, _ := os.ReadFile(filepath.Join(root, name))
			if string(b) != string(bad) {
				t.Fatal("future state rewritten")
			}
		})
	}
}
func TestSymlinkAndUnverifiableRollbackFailClosed(t *testing.T) {
	root, old := initial(t)
	p, next := fixture(t, "next", "")
	victim := filepath.Join(t.TempDir(), "victim")
	_ = os.WriteFile(victim, []byte("private"), 0600)
	if e := os.Symlink(victim, filepath.Join(root, newName)); e != nil {
		t.Fatal(e)
	}
	if e := Install(context.Background(), root, p, next, old.Digest); e == nil {
		t.Fatal("unsafe stage accepted")
	}
	b, _ := os.ReadFile(victim)
	if string(b) != "private" {
		t.Fatal("followed symlink")
	}
}
func FuzzUpgradeRecord(f *testing.F) {
	f.Add([]byte(`{"schema_version":1}`))
	f.Add([]byte(`{"schema_version":2}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		var r Record
		if artifact.Decode(b, &r) == nil {
			_ = validateRecord(r)
		}
	})
}

func TestSameBytesActivatesSignedTrustAndDeniesUnsigned(t *testing.T) {
	root, old := initial(t)
	signed := old
	signed.Unsigned = false
	signed.Version = "1.0.0"
	signed.Channel = "stable"
	signed.Signer = "isolated-test"
	signed.TestRoot = true
	if err := Install(context.Background(), root, filepath.Join(root, targetName), signed, old.Digest); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, currentName))
	if err != nil {
		t.Fatal(err)
	}
	var got artifact.Decision
	if err = artifact.Decode(raw, &got); err != nil || got != signed {
		t.Fatalf("trust not activated: %+v %v", got, err)
	}
	if err = Install(context.Background(), root, filepath.Join(root, targetName), old, old.Digest); err == nil {
		t.Fatal("same bytes downgraded signed trust")
	}
}

func TestRecoverFullDeadUploadReservationsAndPreserveUncertain(t *testing.T) {
	root, _ := initial(t)
	makeSlot := func(i, pid int, unknown bool) string {
		path := filepath.Join(root, fmt.Sprintf(".rdev-upload-slot-%d", i))
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "owner"), []byte(strconv.Itoa(pid)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "agent"), []byte("incomplete"), 0700); err != nil {
			t.Fatal(err)
		}
		if unknown {
			if err := os.WriteFile(filepath.Join(path, "unknown"), []byte("preserve"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		ago := time.Now().Add(-11 * time.Minute)
		if err := os.Chtimes(path, ago, ago); err != nil {
			t.Fatal(err)
		}
		return path
	}
	// The child has exited and been reaped; this exact PID is dead at the barrier.
	child := exec.Command("sh", "-c", "exit 0")
	if err := child.Run(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		makeSlot(i, child.Process.Pid, false)
	}
	if err := Recover(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := os.Lstat(filepath.Join(root, fmt.Sprintf(".rdev-upload-slot-%d", i))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("full reservation not reclaimed: %d %v", i, err)
		}
	}
	live := makeSlot(0, os.Getpid(), false)
	unknown := makeSlot(1, child.Process.Pid, true)
	if err := Recover(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{live, unknown} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("uncertain reservation deleted: %v", err)
		}
	}
}

func TestInstallKilledProcessHelper(t *testing.T) {
	if os.Getenv("RDEV_INSTALL_KILL_HELPER") != "1" {
		return
	}
	var decision artifact.Decision
	if err := json.Unmarshal([]byte(os.Getenv("RDEV_INSTALL_DECISION")), &decision); err != nil {
		t.Fatal(err)
	}
	err := install(context.Background(), os.Getenv("RDEV_INSTALL_ROOT"), os.Getenv("RDEV_INSTALL_CANDIDATE"), decision, os.Getenv("RDEV_INSTALL_OLD"), func(point string) error {
		if point == os.Getenv("RDEV_INSTALL_WINDOW") {
			fmt.Fprintln(os.Stdout, "RDEV_WINDOW_READY")
			// A pipe barrier keeps this process and its actual kernel leases alive
			// until the parent delivers SIGKILL; no random timing assumption.
			var b [1]byte
			_, err := os.Stdin.Read(b[:])
			return err
		}
		return nil
	})
	t.Fatalf("helper was not killed: %v", err)
}

func TestSIGKILLDurableWindowsReleaseLockAndRecover(t *testing.T) {
	for _, window := range []string{"prepared", "verified", "switching", "published", "committed"} {
		t.Run(window, func(t *testing.T) {
			root, old := initial(t)
			candidate, next := fixture(t, "new-killed-process", "")
			raw, _ := json.Marshal(next)
			cmd := exec.Command(os.Args[0], "-test.run=^TestInstallKilledProcessHelper$")
			cmd.Env = append(os.Environ(), "RDEV_INSTALL_KILL_HELPER=1", "RDEV_INSTALL_ROOT="+root, "RDEV_INSTALL_CANDIDATE="+candidate, "RDEV_INSTALL_OLD="+old.Digest, "RDEV_INSTALL_DECISION="+string(raw), "RDEV_INSTALL_WINDOW="+window)
			in, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			defer in.Close()
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
			ready := make(chan bool, 1)
			go func() {
				scan := bufio.NewScanner(out)
				for scan.Scan() {
					if scan.Text() == "RDEV_WINDOW_READY" {
						ready <- true
						return
					}
				}
				ready <- false
			}()
			select {
			case ok := <-ready:
				if !ok {
					t.Fatal("child did not reach durable window")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("child barrier timeout")
			}
			// A second process using a different spelling of the same directory
			// must not enter while the killed helper still owns its kernel lock.
			blocked, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
			if _, release, e := acquire(blocked, root+"/."); e == nil {
				release()
				stop()
				t.Fatal("cross-process namespace lock bypass")
			}
			stop()
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = cmd.Wait(); err == nil {
				t.Fatal("expected killed process")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err = Recover(ctx, root); err != nil {
				t.Fatal(err)
			}
			if err = Install(ctx, root, candidate, next, installed(t, root)); err != nil {
				t.Fatal(err)
			}
			if installed(t, root) != next.Digest {
				t.Fatal("wrong recovered bytes")
			}
			previous, err := os.ReadFile(filepath.Join(root, previousName))
			if err != nil || artifact.Hash(previous) != old.Digest {
				t.Fatalf("known good version lost: %v", err)
			}
		})
	}
}

func TestOldFeatureSetCanUpgradeAndRemainRollbackTarget(t *testing.T) {
	root := rootDir(t)
	response := proto.Response{ID: "rdev-upgrade-health", OK: true, Ping: &proto.PingResult{Version: 3, MinVersion: 3, OS: runtime.GOOS, Arch: runtime.GOARCH, Features: []proto.Feature{proto.FeatureOperationID}}}
	raw, _ := json.Marshal(response)
	oldBytes := []byte("#!/bin/sh\nread line\nprintf '%s\\n' '" + string(raw) + "'\n")
	if err := os.WriteFile(filepath.Join(root, targetName), oldBytes, 0700); err != nil {
		t.Fatal(err)
	}
	old := artifact.Hash(oldBytes)
	candidate, next := fixture(t, "fails-only-after-switch", "yes")
	err := Install(context.Background(), root, candidate, next, old)
	var failed *Error
	if !errors.As(err, &failed) || failed.Stage != "rolled_back" {
		t.Fatalf("old compatible peer blocked upgrade/rollback: %v", err)
	}
	if installed(t, root) != old {
		t.Fatal("fallback not restored")
	}
	candidate, next = fixture(t, "valid-new", "")
	if err = Install(context.Background(), root, candidate, next, old); err != nil {
		t.Fatal(err)
	}
	if installed(t, root) != next.Digest {
		t.Fatal("upgrade blocked by missing predecessor features")
	}
}

func TestTransactionFIFORejectedWithoutBlockingOrRemoving(t *testing.T) {
	for _, name := range []string{journalName, currentName, targetName, rollbackName, newName} {
		t.Run(name, func(t *testing.T) {
			root := rootDir(t)
			if err := unix.Mkfifo(filepath.Join(root, name), 0600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tx, unlock, err := acquire(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			defer unlock()
			done := make(chan error, 1)
			go func() { _, err := tx.read(name, 32<<10); done <- err }()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("FIFO accepted")
				}
			case <-ctx.Done():
				t.Fatal("FIFO open exceeded deadline")
			}
			if st, err := os.Lstat(filepath.Join(root, name)); err != nil || st.Mode()&os.ModeNamedPipe == 0 {
				t.Fatal("unknown evidence removed")
			}
		})
	}

}
