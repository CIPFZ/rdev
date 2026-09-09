//go:build linux || darwin

package agentinstall

// This opt-in integration suite uses two real, immutable Go agent binaries.
// The six RDEV_REAL_AGENT_{CURRENT,PREVIOUS}_{PATH,COMMIT,SHA256} variables are
// mandatory together. The previous binary is an identified engineering build,
// not automatically a certified N-1 release. No binary is rebuilt or patched.
//
// Entry tests invoke the production -install-candidate/-recover-upgrades modes.
// Fault tests run this package's installer in a test subprocess and use its
// existing durable-point hooks; all hello/readiness checks execute real agents.
// Fault hooks change permissions or state in private fixtures, never code bytes.
// Decisions model an already authorized caller; signed activation reconstructs
// test-root decision metadata, without supplying or verifying a signature. Tests
// do not exercise SSH upload, signature verification or administrator rollback
// authorization, and cannot certify those entry boundaries by themselves.

import (
	"bufio"
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/state"
	"golang.org/x/sys/unix"
)

type realAgent struct {
	path, commit, digest string
}

func realAgents(t *testing.T) (realAgent, realAgent) {
	t.Helper()
	if os.Getenv("RDEV_REAL_AGENT_CURRENT_PATH") == "" && os.Getenv("RDEV_REAL_AGENT_PREVIOUS_PATH") == "" {
		t.Skip("real immutable agent binaries not supplied; this is not runtime evidence")
	}
	load := func(role string) realAgent {
		t.Helper()
		prefix := "RDEV_REAL_AGENT_" + role + "_"
		a := realAgent{os.Getenv(prefix + "PATH"), os.Getenv(prefix + "COMMIT"), os.Getenv(prefix + "SHA256")}
		if !filepath.IsAbs(a.path) || len(a.commit) != 40 || !validDigest(a.digest) {
			t.Fatalf("%s requires absolute PATH, full COMMIT and SHA256", prefix)
		}
		b, err := artifact.ReadFile(filepath.Dir(a.path), filepath.Base(a.path), maxBinary)
		if err != nil || artifact.Hash(b) != a.digest {
			t.Fatalf("%s immutable byte identity failed: %v", role, err)
		}
		info, err := buildinfo.ReadFile(a.path)
		if err != nil {
			t.Fatal(err)
		}
		settings := map[string]string{}
		for _, s := range info.Settings {
			settings[s.Key] = s.Value
		}
		if info.Path != "github.com/CIPFZ/rdev/cmd/rdev-agent" || settings["vcs.revision"] != a.commit || settings["vcs.modified"] != "false" || settings["GOOS"] != runtime.GOOS || settings["GOARCH"] != runtime.GOARCH {
			t.Fatalf("%s must be an actual clean native agent with the declared source", role)
		}
		t.Logf("real-agent role=%s source=%s sha256=%s toolchain=%s", role, a.commit, a.digest, info.GoVersion)
		return a
	}
	current, previous := load("CURRENT"), load("PREVIOUS")
	if current.commit == previous.commit || current.digest == previous.digest {
		t.Fatal("current and predecessor must have distinct actual source and byte identities")
	}
	return current, previous
}

func (a realAgent) decision() artifact.Decision {
	return artifact.Decision{Channel: "dev", Digest: a.digest, Unsigned: true}
}

func realCopy(t *testing.T, a realAgent, root string) {
	t.Helper()
	b, err := os.ReadFile(a.path)
	if err != nil || artifact.Hash(b) != a.digest {
		t.Fatalf("fixture copy identity: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, targetName), b, 0700); err != nil {
		t.Fatal(err)
	}
}

func realError(t *testing.T, err error, state, stage string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.State != state || e.Stage != stage {
		t.Fatalf("expected %s/%s, got %v", state, stage, err)
	}
}

func realEntry(t *testing.T, a realAgent, want string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, a.path, args...).CombinedOutput()
	if want == "" {
		if err != nil || len(output) != 0 {
			t.Fatalf("real installer entry failed: %v output=%q", err, output)
		}
	} else if err == nil || strings.TrimSpace(string(output)) != want {
		t.Fatalf("real entry must return %q: error=%v output=%q", want, err, output)
	}
}

func realEntryInstall(t *testing.T, current realAgent, root, old, want string) {
	t.Helper()
	raw, err := json.Marshal(current.decision())
	if err != nil {
		t.Fatal(err)
	}
	realEntry(t, current, want, "-install-candidate", root, string(raw), old)
}

func realRecord(t *testing.T, root string) Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, journalName))
	if err != nil {
		t.Fatal(err)
	}
	var r Record
	if err := artifact.Decode(b, &r); err != nil {
		t.Fatal(err)
	}
	if err := validateRecord(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func realPreserved(t *testing.T, path string, want []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || string(b) != string(want) {
		t.Fatalf("retained bytes changed at %s: %v", filepath.Base(path), err)
	}
}

func TestRealAgentBinaryProductionEntry(t *testing.T) {
	current, previous := realAgents(t)
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement=%t", replacement), func(t *testing.T) {
			root := rootDir(t)
			old := ""
			if replacement {
				realCopy(t, previous, root)
				old = previous.digest
			}
			realEntryInstall(t, current, root, old, "")
			if installed(t, root) != current.digest || realRecord(t, root).Phase != "committed" {
				t.Fatal("entry did not commit actual candidate bytes")
			}
			if replacement {
				b, err := os.ReadFile(filepath.Join(root, previousName))
				if err != nil || artifact.Hash(b) != previous.digest {
					t.Fatalf("predecessor backup lost: %v", err)
				}
			} else if _, err := os.Lstat(filepath.Join(root, previousName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("first install invented a known-good predecessor")
			}
			realEntry(t, current, "", "-recover-upgrades", root)
			realEntryInstall(t, current, root, current.digest, "")
			realEntryInstall(t, current, root, old, "RDEV_AGENT_INSTALL_NOT_SENT:prepare")
			if err := Health(context.Background(), filepath.Join(root, targetName), root); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("wrong-bytes-and-arguments", func(t *testing.T) {
		root := rootDir(t)
		raw, _ := json.Marshal(previous.decision())
		realEntry(t, current, "RDEV_AGENT_INSTALL_NOT_SENT:verify", "-install-candidate", root, string(raw), "")
		realEntry(t, current, "RDEV_AGENT_INSTALL_NOT_SENT:arguments", "-install-candidate", root, "{}", "")
		if installed(t, root) != "" {
			t.Fatal("rejected candidate published")
		}
	})
}

func TestRealAgentBinaryHealthAndStateRefusal(t *testing.T) {
	current, previous := realAgents(t)
	t.Run("healthy-state-keeps-exact-bytes", func(t *testing.T) {
		root := rootDir(t)
		manifest := []byte(`{"schema_version":1,"writer_version":"retained-predecessor","namespace":"isolated-real-install"}`)
		path := filepath.Join(root, "manifest.json")
		if err := os.WriteFile(path, manifest, 0600); err != nil {
			t.Fatal(err)
		}
		for _, a := range []realAgent{previous, current} {
			if err := health(context.Background(), a.path, root, false); err != nil {
				t.Fatal(err)
			}
			realPreserved(t, path, manifest)
		}
		if err := Health(context.Background(), current.path, root); err != nil {
			t.Fatal(err)
		}
		realPreserved(t, path, manifest)
		if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && path != filepath.Join(root, "manifest.json") {
				return fmt.Errorf("health created unexpected state file %s", entry.Name())
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	for _, fixture := range []struct{ name, path, bytes string }{
		{"future-manifest", "manifest.json", `{"schema_version":99}`},
		{"corrupt-manifest", "manifest.json", `{"schema_version":`},
		{"future-job", "jobs/future/meta.json", `{"schema_version":99,"id":"future"}`},
		{"corrupt-job", "jobs/corrupt/meta.json", `{"schema_version":`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			root := rootDir(t)
			realCopy(t, previous, root)
			path := filepath.Join(root, filepath.FromSlash(fixture.path))
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			data := []byte(fixture.bytes)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if err := Health(context.Background(), current.path, root); err == nil {
				t.Fatal("readiness accepted future/corrupt state")
			}
			stage := "state_readiness"
			if strings.HasPrefix(fixture.path, "jobs/") {
				stage = "previous_health"
			}
			realEntryInstall(t, current, root, previous.digest, "RDEV_AGENT_INSTALL_NOT_SENT:"+stage)
			realPreserved(t, path, data)
			if installed(t, root) != previous.digest {
				t.Fatal("state rejection changed active bytes")
			}
			if _, err := os.Lstat(filepath.Join(root, journalName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("state rejection began installation transaction")
			}
		})
	}
}

func TestRealAgentBinaryPostSwitchRollback(t *testing.T) {
	current, previous := realAgents(t)
	for _, fault := range []string{"active-not-executable", "rollback-unavailable", "future-state", "first-install"} {
		t.Run(fault, func(t *testing.T) {
			root := rootDir(t)
			old := ""
			if fault != "first-install" {
				realCopy(t, previous, root)
				old = previous.digest
			}
			future := []byte(`{"schema_version":99}`)
			err := install(context.Background(), root, current.path, current.decision(), old, func(point string) error {
				if point != "published" {
					return nil
				}
				if fault == "future-state" {
					return os.WriteFile(filepath.Join(root, "manifest.json"), future, 0600)
				}
				if err := os.Chmod(filepath.Join(root, targetName), 0600); err != nil {
					return err
				}
				if fault == "rollback-unavailable" {
					return os.Remove(filepath.Join(root, rollbackName))
				}
				return nil
			})
			if fault == "active-not-executable" {
				realError(t, err, "not_sent", "rolled_back")
				if installed(t, root) != previous.digest || realRecord(t, root).Phase != "rolled_back" {
					t.Fatal("real predecessor not restored")
				}
				if err := health(context.Background(), filepath.Join(root, targetName), root, false); err != nil {
					t.Fatal(err)
				}
				realEntry(t, current, "", "-recover-upgrades", root)
				return
			}
			stage := "rollback"
			if fault == "future-state" {
				stage = "rollback_state"
			}
			realError(t, err, "ambiguous", stage)
			if installed(t, root) != current.digest || realRecord(t, root).Phase != "switching" {
				t.Fatal("unconfirmed rollback fabricated a restored or committed state")
			}
			if fault == "future-state" {
				realPreserved(t, filepath.Join(root, "manifest.json"), future)
				realEntry(t, current, "RDEV_AGENT_INSTALL_NOT_SENT:state_readiness", "-recover-upgrades", root)
				realPreserved(t, filepath.Join(root, "manifest.json"), future)
			} else {
				realEntry(t, current, "RDEV_AGENT_INSTALL_AMBIGUOUS:rollback", "-recover-upgrades", root)
			}
		})
	}
}

// This subprocess owns the actual installation and state kernel leases. Only a
// parent pipe barrier followed by SIGKILL interrupts a durable window.
func TestRealAgentBinaryFaultChild(t *testing.T) {
	if os.Getenv("RDEV_REAL_AGENT_FAULT_CHILD") != "1" {
		return
	}
	current, _ := realAgents(t)
	root, window := os.Getenv("RDEV_REAL_AGENT_ROOT"), os.Getenv("RDEV_REAL_AGENT_WINDOW")
	err := install(context.Background(), root, current.path, current.decision(), os.Getenv("RDEV_REAL_AGENT_OLD"), func(point string) error {
		if window == "rolled_back" && point == "published" {
			return os.Chmod(filepath.Join(root, targetName), 0600)
		}
		if point == window {
			fmt.Fprintln(os.Stdout, "RDEV_REAL_DURABLE_WINDOW_READY")
			var b [1]byte
			_, err := os.Stdin.Read(b[:])
			return err
		}
		return nil
	})
	t.Fatalf("fault child returned before SIGKILL: %v", err)
}

func realBarrier(t *testing.T, root, old, window string) func() {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRealAgentBinaryFaultChild$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "RDEV_REAL_AGENT_FAULT_CHILD=1", "RDEV_REAL_AGENT_ROOT="+root, "RDEV_REAL_AGENT_OLD="+old, "RDEV_REAL_AGENT_WINDOW="+window)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		in.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	ready := make(chan bool, 1)
	go func() {
		scan := bufio.NewScanner(out)
		for scan.Scan() {
			if scan.Text() == "RDEV_REAL_DURABLE_WINDOW_READY" {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("real-agent child failed before durable barrier")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("real-agent child durable barrier deadline")
	}
	return func() {
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		waited = true
		err := cmd.Wait()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("child must actually exit from SIGKILL: %v", err)
		}
	}
}

func TestRealAgentBinaryCommittedCleanupFailure(t *testing.T) {
	current, previous := realAgents(t)
	root := rootDir(t)
	realCopy(t, previous, root)
	err := install(context.Background(), root, current.path, current.decision(), previous.digest, func(point string) error {
		if point == "published" {
			// Force a real rename failure after a successful active-path hello and
			// durable commit. The retained rollback inode remains available.
			return os.Mkdir(filepath.Join(root, previousName), 0700)
		}
		return nil
	})
	realError(t, err, "committed", "cleanup")
	if installed(t, root) != current.digest || realRecord(t, root).Phase != "committed" {
		t.Fatal("cleanup error disguised a committed binary replacement")
	}
	backup, err := os.ReadFile(filepath.Join(root, rollbackName))
	if err != nil || artifact.Hash(backup) != previous.digest {
		t.Fatalf("cleanup error lost recovery bytes: %v", err)
	}
	realEntry(t, current, "RDEV_AGENT_INSTALL_COMMITTED:cleanup", "-recover-upgrades", root)
	if err := os.Remove(filepath.Join(root, previousName)); err != nil {
		t.Fatal(err)
	}
	realEntry(t, current, "", "-recover-upgrades", root)
	backup, err = os.ReadFile(filepath.Join(root, previousName))
	if err != nil || artifact.Hash(backup) != previous.digest {
		t.Fatalf("cleanup retry lost predecessor: %v", err)
	}
}

func TestRealAgentBinaryExplicitUnsignedDevRollback(t *testing.T) {
	current, previous := realAgents(t)
	root := rootDir(t)
	manifest := []byte(`{"schema_version":1,"writer_version":"retained-predecessor","namespace":"isolated-real-rollback"}`)
	path := filepath.Join(root, "manifest.json")
	if err := os.WriteFile(path, manifest, 0600); err != nil {
		t.Fatal(err)
	}
	realCopy(t, previous, root)
	realEntryInstall(t, current, root, previous.digest, "")
	decision := previous.decision()
	decision.Rollback = true
	// The predecessor has no installer entry. The current library is the helper;
	// this is explicit dev rollback plus real binary/state health, not signed
	// transport policy or formal N-1 authorization evidence.
	if err := Install(context.Background(), root, previous.path, decision, current.digest); err != nil {
		t.Fatal(err)
	}
	if installed(t, root) != previous.digest || realRecord(t, root).Phase != "committed" {
		t.Fatal("explicit binary rollback did not restore actual predecessor")
	}
	realPreserved(t, path, manifest)
	if err := health(context.Background(), filepath.Join(root, targetName), root, false); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(filepath.Join(root, previousName))
	if err != nil || artifact.Hash(backup) != current.digest {
		t.Fatalf("rollback lost forward recovery bytes: %v", err)
	}
}

func TestRealAgentBinarySIGKILLAndRecovery(t *testing.T) {
	current, previous := realAgents(t)
	for _, replacement := range []bool{false, true} {
		for _, window := range []string{"prepared", "verified", "switching", "published", "committed", "rolled_back"} {
			if !replacement && window == "rolled_back" {
				continue // A first installation has no prior binary to roll back to.
			}
			t.Run(fmt.Sprintf("replacement=%t/%s", replacement, window), func(t *testing.T) {
				root := rootDir(t)
				old := ""
				if replacement {
					realCopy(t, previous, root)
					old = previous.digest
				}
				kill := realBarrier(t, root, old, window)
				kill()
				realEntry(t, current, "", "-recover-upgrades", root)
				want := old
				if window == "published" || window == "committed" {
					want = current.digest
				}
				if installed(t, root) != want {
					t.Fatal("recovery selected wrong actual bytes")
				}
				before, err := os.ReadFile(filepath.Join(root, journalName))
				if err != nil {
					t.Fatal(err)
				}
				realEntry(t, current, "", "-recover-upgrades", root)
				realPreserved(t, filepath.Join(root, journalName), before)
				if installed(t, root) != want {
					t.Fatal("repeated recovery replayed publication")
				}
				realEntryInstall(t, current, root, want, "")
				if installed(t, root) != current.digest || realRecord(t, root).Phase != "committed" {
					t.Fatal("reconciled transaction did not accept a fresh valid request")
				}
				if replacement {
					b, err := os.ReadFile(filepath.Join(root, previousName))
					if err != nil || artifact.Hash(b) != previous.digest {
						t.Fatalf("SIGKILL lost sole known-good predecessor: %v", err)
					}
				}
			})
		}
	}
}

func TestRealAgentBinaryCrossProcessLockAndCAS(t *testing.T) {
	current, previous := realAgents(t)
	root := rootDir(t)
	realCopy(t, previous, root)
	kill := realBarrier(t, root, previous.digest, "prepared")
	var before unix.Stat_t
	if err := unix.Stat(filepath.Join(root, ".rdev-upgrade.lock"), &before); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err := Install(ctx, root+"/.", current.path, current.decision(), previous.digest)
	cancel()
	realError(t, err, "not_sent", "lock")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("alias must remain blocked by the live other process")
	}
	if _, err := state.Migrate(root, false); !errors.Is(err, state.ErrMigrationLocked) {
		t.Fatalf("migration must be fenced by the live installer process: %v", err)
	}
	other := rootDir(t)
	realEntryInstall(t, current, other, "", "")
	if installed(t, other) != current.digest {
		t.Fatal("independent namespace was blocked by a global upgrade lock")
	}
	kill()
	realEntry(t, current, "", "-recover-upgrades", root)
	realEntryInstall(t, current, root, previous.digest, "")
	realEntryInstall(t, current, root, previous.digest, "RDEV_AGENT_INSTALL_NOT_SENT:prepare")
	var after unix.Stat_t
	if err := unix.Stat(filepath.Join(root, ".rdev-upgrade.lock"), &after); err != nil || before.Dev != after.Dev || before.Ino != after.Ino {
		t.Fatalf("lock inode was replaced after SIGKILL: %v", err)
	}
	if installed(t, root) != current.digest {
		t.Fatal("stale CAS request overwrote the committed winner")
	}
}

// Reconstruct the disk-visible publication boundary using the existing hook.
// The recovery under test is the unmodified production executable. This is not
// an additional SIGKILL/fsync fault or a physical power-loss test.
func realPublished(t *testing.T, current, previous realAgent, replacement bool) string {
	t.Helper()
	root := rootDir(t)
	old := ""
	if replacement {
		realCopy(t, previous, root)
		old = previous.digest
	}
	err := install(context.Background(), root, current.path, current.decision(), old, func(point string) error {
		if point == "published" {
			return errors.New("stop for disk-state reconstruction")
		}
		return nil
	})
	realError(t, err, "ambiguous", "published")
	if installed(t, root) != current.digest || realRecord(t, root).Phase != "switching" {
		t.Fatal("fixture did not reach the actual publication boundary")
	}
	return root
}

func realDecisionBytes(t *testing.T, decision artifact.Decision) []byte {
	t.Helper()
	b, err := json.Marshal(decision)
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}

func realActiveUnchanged(t *testing.T, root string, before os.FileInfo, digest string) {
	t.Helper()
	after, err := os.Stat(filepath.Join(root, targetName))
	if err != nil || !os.SameFile(before, after) || installed(t, root) != digest {
		t.Fatal("recovery republished or changed the active binary")
	}
}

func TestRealAgentBinaryDecisionJournalRecovery(t *testing.T) {
	current, previous := realAgents(t)
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("published-decision-before-journal/replacement=%t", replacement), func(t *testing.T) {
			root := realPublished(t, current, previous, replacement)
			decision := realDecisionBytes(t, current.decision())
			if err := os.WriteFile(filepath.Join(root, currentName), decision, 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(filepath.Join(root, targetName))
			if err != nil {
				t.Fatal(err)
			}
			realEntry(t, current, "", "-recover-upgrades", root)
			if realRecord(t, root).Phase != "committed" || realRecord(t, root).Candidate != current.decision() {
				t.Fatal("recovery did not reconcile the committed decision")
			}
			journal, err := os.ReadFile(filepath.Join(root, journalName))
			if err != nil {
				t.Fatal(err)
			}
			realEntry(t, current, "", "-recover-upgrades", root)
			realPreserved(t, filepath.Join(root, currentName), decision)
			realPreserved(t, filepath.Join(root, journalName), journal)
			realActiveUnchanged(t, root, before, current.digest)
			if replacement {
				backup, err := os.ReadFile(filepath.Join(root, previousName))
				if err != nil || artifact.Hash(backup) != previous.digest {
					t.Fatal("reconciliation lost the actual predecessor")
				}
			}
		})
	}
	for _, legacyJournal := range []bool{false, true} {
		t.Run(fmt.Sprintf("same-bytes-trust-activation/journal=%t", legacyJournal), func(t *testing.T) {
			root := rootDir(t)
			if legacyJournal {
				realEntryInstall(t, current, root, "", "")
			} else {
				realCopy(t, current, root)
			}
			// This is already-authorized metadata from an isolated test root.
			// It is not publisher signature verification or SSH authorization.
			signed := current.decision()
			signed.Unsigned, signed.TestRoot = false, true
			signed.Channel, signed.Version, signed.Signer = "stable", "1.0.0", "isolated-test-metadata"
			decision := realDecisionBytes(t, signed)
			if err := os.WriteFile(filepath.Join(root, currentName), decision, 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(filepath.Join(root, targetName))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				realEntry(t, current, "", "-recover-upgrades", root)
				realEntryInstall(t, current, root, current.digest, "RDEV_AGENT_INSTALL_NOT_SENT:direction")
				realPreserved(t, filepath.Join(root, currentName), decision)
				if legacyJournal {
					if !realRecord(t, root).Candidate.Unsigned {
						t.Fatal("recovery fabricated completion of interrupted activation")
					}
				} else if _, err := os.Lstat(filepath.Join(root, journalName)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("recovery or refused downgrade invented a journal")
				}
			}
			realEntry(t, current, "", "-install-candidate", root, string(decision), current.digest)
			if realRecord(t, root).Candidate != signed || realRecord(t, root).Phase != "committed" {
				t.Fatal("authorized activation retry did not finish the journal")
			}
			realEntry(t, current, "", "-recover-upgrades", root)
			realEntryInstall(t, current, root, current.digest, "RDEV_AGENT_INSTALL_NOT_SENT:direction")
			realPreserved(t, filepath.Join(root, currentName), decision)
			realActiveUnchanged(t, root, before, current.digest)
		})
	}
}

func TestRealAgentBinaryRecordScratchRecovery(t *testing.T) {
	current, previous := realAgents(t)
	fragment := []byte(`{"interrupted_record":`)
	t.Run("before-first-prepared-record", func(t *testing.T) {
		root := rootDir(t)
		path := filepath.Join(root, journalName+".tmp")
		if err := os.WriteFile(path, fragment, 0600); err != nil {
			t.Fatal(err)
		}
		realEntry(t, current, "", "-recover-upgrades", root)
		realPreserved(t, path, fragment)
		if installed(t, root) != "" {
			t.Fatal("record-less recovery invented an installation")
		}
		realEntryInstall(t, current, root, "", "")
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("safe partial record scratch was not reclaimed on retry")
		}
	})
	for _, name := range []string{currentName, journalName} {
		for _, kind := range []string{"partial", "symlink", "fifo", "directory", "unsafe-mode", "oversized"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				root := realPublished(t, current, previous, true)
				// A journal scratch write occurs only after the current decision
				// write completed. Recreate that ordering, not arbitrary JSON.
				if name == journalName {
					if err := os.WriteFile(filepath.Join(root, currentName), realDecisionBytes(t, current.decision()), 0600); err != nil {
						t.Fatal(err)
					}
				}
				path := filepath.Join(root, name+".tmp")
				victim := filepath.Join(t.TempDir(), "untouched")
				if err := os.WriteFile(victim, fragment, 0600); err != nil {
					t.Fatal(err)
				}
				content := fragment
				var err error
				switch kind {
				case "symlink":
					err = os.Symlink(victim, path)
				case "fifo":
					err = unix.Mkfifo(path, 0600)
				case "directory":
					err = os.Mkdir(path, 0700)
				case "oversized":
					content = make([]byte, (32<<10)+1)
					err = os.WriteFile(path, content, 0600)
				default:
					err = os.WriteFile(path, content, 0600)
					if err == nil && kind == "unsafe-mode" {
						err = os.Chmod(path, 0620)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				activeBefore, err := os.Stat(filepath.Join(root, targetName))
				if err != nil {
					t.Fatal(err)
				}
				if kind != "partial" {
					scratchBefore, err := os.Lstat(path)
					if err != nil {
						t.Fatal(err)
					}
					marker := "RDEV_AGENT_INSTALL_AMBIGUOUS:record"
					if name == journalName {
						marker = "RDEV_AGENT_INSTALL_COMMITTED:record"
					}
					for i := 0; i < 2; i++ {
						realEntry(t, current, marker, "-recover-upgrades", root)
						after, err := os.Lstat(path)
						if err != nil || !os.SameFile(scratchBefore, after) || scratchBefore.Mode() != after.Mode() {
							t.Fatal("refusal removed or replaced unsafe scratch evidence")
						}
						if after.Mode().IsRegular() {
							realPreserved(t, path, content)
						}
						realPreserved(t, victim, fragment)
						realActiveUnchanged(t, root, activeBefore, current.digest)
						if realRecord(t, root).Phase != "switching" {
							t.Fatal("failed record persistence fabricated a durable phase")
						}
						backup, err := os.ReadFile(filepath.Join(root, rollbackName))
						if err != nil || artifact.Hash(backup) != previous.digest {
							t.Fatal("record failure lost rollback evidence")
						}
					}
					// Only remove this test-owned object after proving conservative
					// refusal; product recovery never removes the unsafe evidence.
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				}
				realEntry(t, current, "", "-recover-upgrades", root)
				realEntry(t, current, "", "-recover-upgrades", root)
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("safe record scratch survived successful reconciliation")
				}
				if realRecord(t, root).Phase != "committed" {
					t.Fatal("record retry did not commit")
				}
				realActiveUnchanged(t, root, activeBefore, current.digest)
				realPreserved(t, victim, fragment)
				backup, err := os.ReadFile(filepath.Join(root, previousName))
				if err != nil || artifact.Hash(backup) != previous.digest {
					t.Fatal("record retry lost known-good predecessor")
				}
			})
		}
	}
}
