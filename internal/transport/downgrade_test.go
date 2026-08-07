// Tests for the agent downgrade guard.
//
// ensureAgent compares content hashes to decide whether an upload is needed, but a
// hash cannot say which of two differing builds is older. Without the build-stamp
// check, whoever connects last wins forever: two people on a shared dev box
// overwrite each other's agent on every connect, and so do two windows of one
// person's own rdev.
package transport

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/buildinfo"
)

func buildAt(t *testing.T, commit, stamp string) buildinfo.Build {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		t.Fatal(err)
	}
	return buildinfo.Build{Version: "0.1.0", Commit: commit, Time: ts.UTC()}
}

var testHost = Host{Name: "dev", Addr: "u@h"}

// The case this exists for: the installed agent was built later, so replacing it
// would be the downgrade that causes the flip-flop.
func TestDowngradeIsRefused(t *testing.T) {
	installed := buildAt(t, "bbbbbbb", "2026-08-07T16:00:00Z")
	local := buildAt(t, "aaaaaaa", "2026-08-07T15:39:00Z")

	err := downgradeError(installed, local, testHost)
	if err == nil {
		t.Fatal("replacing a newer installed agent must be refused, not done silently")
	}
	msg := err.Error()
	// The message has to carry both builds and the way out; a bare refusal would
	// leave the reader with nothing to do next.
	for _, want := range []string{"bbbbbbb", "aaaaaaa", "make all", "-force-agent-upload"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message is missing %q:\n%s", want, msg)
		}
	}
}

// The normal upgrade path must not be blocked by the new check.
func TestUpgradeProceeds(t *testing.T) {
	installed := buildAt(t, "aaaaaaa", "2026-08-07T15:39:00Z")
	local := buildAt(t, "bbbbbbb", "2026-08-07T16:00:00Z")

	if err := downgradeError(installed, local, testHost); err != nil {
		t.Errorf("a newer local build must be installable: %v", err)
	}
}

// Equal commit times are not a downgrade. Two builds of one commit differing only
// in content (different Go version, say) still need the upload to happen.
func TestEqualTimesProceed(t *testing.T) {
	installed := buildAt(t, "aaaaaaa", "2026-08-07T16:00:00Z")
	local := buildAt(t, "aaaaaaa", "2026-08-07T16:00:00Z")

	if err := downgradeError(installed, local, testHost); err != nil {
		t.Errorf("equal build times must not be treated as a downgrade: %v", err)
	}
}

// Every case where the comparison cannot be trusted must proceed with the upload.
// Refusing on unknown input would leave a broken remote agent unrepairable, which
// is a worse failure than the flip-flop being prevented.
func TestUnknownOrDirtyBuildsProceed(t *testing.T) {
	newer := buildAt(t, "bbbbbbb", "2026-08-07T16:00:00Z")
	older := buildAt(t, "aaaaaaa", "2026-08-07T15:39:00Z")

	dirtyNewer := newer
	dirtyNewer.Commit += "-dirty"
	dirtyNewer.Dirty = true

	noTime := buildinfo.Build{Version: "0.1.0", Commit: "bbbbbbb"}
	unstamped := buildinfo.Build{Version: "0.1.0", Commit: "unknown"}

	cases := []struct {
		name             string
		installed, local buildinfo.Build
	}{
		{"installed agent predates the stamp field", buildinfo.Build{}, older},
		{"installed agent has no timestamp", noTime, older},
		{"local build is unstamped go build", newer, unstamped},
		{"installed agent is from a dirty tree", dirtyNewer, older},
		{"local build is from a dirty tree", newer, func() buildinfo.Build {
			b := older
			b.Commit += "-dirty"
			b.Dirty = true
			return b
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := downgradeError(tc.installed, tc.local, testHost); err != nil {
				t.Errorf("an unorderable pair must proceed with the upload, got: %v", err)
			}
		})
	}
}

// fakeSSH puts an `ssh` on PATH that echoes a canned -version reply, so the wiring
// from ensureAgent through runSSH to the parser can be exercised without a host.
//
// It records each invocation, which is how the tests below tell whether an upload
// was attempted rather than inferring it from the error alone.
func fakeSSH(t *testing.T, versionOut string, versionExit int) (logPath string) {
	t.Helper()
	dir := t.TempDir()
	logPath = filepath.Join(dir, "calls.log")

	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + logPath + "\n" +
		"case \"$*\" in\n" +
		"  *-version*)\n" +
		"    printf '%s\\n' " + shellQuote(versionOut) + "\n" +
		"    exit " + strconv.Itoa(versionExit) + " ;;\n" +
		"esac\n" +
		"exit 0\n"

	if err := os.WriteFile(filepath.Join(dir, "ssh"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logPath
}

func sshCalls(t *testing.T, logPath string) string {
	t.Helper()
	b, err := os.ReadFile(logPath)
	if err != nil {
		return ""
	}
	return string(b)
}

// End to end through runSSH: a newer installed agent stops the upload before any
// bytes are written. The upload is what must not happen, so the assertion is on
// the absence of a dd call, not just on the error.
func TestEnsureAgentRefusesDowngradeBeforeUploading(t *testing.T) {
	orig, origTime := buildinfo.Commit, buildinfo.CommitTime
	t.Cleanup(func() { buildinfo.Commit, buildinfo.CommitTime = orig, origTime })
	buildinfo.Commit, buildinfo.CommitTime = "aaaaaaa", "2026-08-07T15:39:00Z"

	log := fakeSSH(t, buildinfo.StampPrefix+" 0.1.0 bbbbbbb 2026-08-07T16:00:00Z", 0)

	c := &Conn{
		host:      testHost,
		stderr:    &lockedBuilder{},
		ctlPath:   filepath.Join(t.TempDir(), "ctl"),
		agentPath: "/home/u/.cache/rdev/rdev-agent",
		stateDir:  "/home/u/.cache/rdev",
	}
	bin := &AgentBinary{Data: []byte("local agent"), SHA256: "localsha"}

	err := c.ensureAgent(context.Background(), bin, "remotesha")
	if err == nil {
		t.Fatal("ensureAgent should refuse to overwrite a newer installed agent")
	}
	if calls := sshCalls(t, log); strings.Contains(calls, "dd of=") {
		t.Errorf("refusal happened after an upload was attempted; ssh calls were:\n%s", calls)
	}
}

// The escape hatch has to actually skip the check, and must not even ask for the
// version -- the point is to proceed regardless of the answer.
func TestForceAgentUploadSkipsTheCheck(t *testing.T) {
	orig, origTime := buildinfo.Commit, buildinfo.CommitTime
	t.Cleanup(func() { buildinfo.Commit, buildinfo.CommitTime = orig, origTime })
	buildinfo.Commit, buildinfo.CommitTime = "aaaaaaa", "2026-08-07T15:39:00Z"

	log := fakeSSH(t, buildinfo.StampPrefix+" 0.1.0 bbbbbbb 2026-08-07T16:00:00Z", 0)

	host := testHost
	host.ForceAgentUpload = true
	c := &Conn{
		host:      host,
		stderr:    &lockedBuilder{},
		ctlPath:   filepath.Join(t.TempDir(), "ctl"),
		agentPath: "/home/u/.cache/rdev/rdev-agent",
		stateDir:  "/home/u/.cache/rdev",
	}
	bin := &AgentBinary{Data: []byte("local agent"), SHA256: "localsha"}

	if err := c.ensureAgent(context.Background(), bin, "remotesha"); err != nil {
		t.Fatalf("-force-agent-upload must install regardless of the installed build: %v", err)
	}
	calls := sshCalls(t, log)
	if strings.Contains(calls, "-version") {
		t.Errorf("forced upload should not bother probing the version; ssh calls were:\n%s", calls)
	}
	if !strings.Contains(calls, "dd of=") {
		t.Errorf("forced upload never uploaded; ssh calls were:\n%s", calls)
	}
}

// A matching hash is the fast path and must cost nothing: no version probe, no
// upload. This is what keeps a warm connect at one round trip.
func TestMatchingHashSkipsEverything(t *testing.T) {
	log := fakeSSH(t, "", 0)

	c := &Conn{
		host:      testHost,
		stderr:    &lockedBuilder{},
		ctlPath:   filepath.Join(t.TempDir(), "ctl"),
		agentPath: "/home/u/.cache/rdev/rdev-agent",
		stateDir:  "/home/u/.cache/rdev",
	}
	bin := &AgentBinary{Data: []byte("agent"), SHA256: "samesha"}

	if err := c.ensureAgent(context.Background(), bin, "samesha"); err != nil {
		t.Fatalf("an already-current agent must be a no-op: %v", err)
	}
	if calls := sshCalls(t, log); calls != "" {
		t.Errorf("current agent should cost no ssh calls, got:\n%s", calls)
	}
}

// A first install has no installed agent to compare against, so it must proceed
// without a version probe.
func TestFirstInstallSkipsTheCheck(t *testing.T) {
	log := fakeSSH(t, "", 0)

	c := &Conn{
		host:      testHost,
		stderr:    &lockedBuilder{},
		ctlPath:   filepath.Join(t.TempDir(), "ctl"),
		agentPath: "/home/u/.cache/rdev/rdev-agent",
		stateDir:  "/home/u/.cache/rdev",
	}
	bin := &AgentBinary{Data: []byte("agent"), SHA256: "localsha"}

	// Empty installedSHA is what the probe reports when no agent is present.
	if err := c.ensureAgent(context.Background(), bin, ""); err != nil {
		t.Fatalf("first install must proceed: %v", err)
	}
	calls := sshCalls(t, log)
	if strings.Contains(calls, "-version") {
		t.Errorf("nothing is installed, so there is no version to probe; calls were:\n%s", calls)
	}
	if !strings.Contains(calls, "dd of=") {
		t.Errorf("first install never uploaded; calls were:\n%s", calls)
	}
}

// An installed binary that cannot report a version -- truncated upload, wrong
// architecture, not an agent at all -- must be replaceable. Refusing here would
// make a broken remote agent permanently unrepairable.
func TestUnrunnableInstalledAgentIsReplaced(t *testing.T) {
	orig, origTime := buildinfo.Commit, buildinfo.CommitTime
	t.Cleanup(func() { buildinfo.Commit, buildinfo.CommitTime = orig, origTime })
	buildinfo.Commit, buildinfo.CommitTime = "aaaaaaa", "2026-08-07T15:39:00Z"

	log := fakeSSH(t, "Exec format error", 1)

	c := &Conn{
		host:      testHost,
		stderr:    &lockedBuilder{},
		ctlPath:   filepath.Join(t.TempDir(), "ctl"),
		agentPath: "/home/u/.cache/rdev/rdev-agent",
		stateDir:  "/home/u/.cache/rdev",
	}
	bin := &AgentBinary{Data: []byte("agent"), SHA256: "localsha"}

	if err := c.ensureAgent(context.Background(), bin, "remotesha"); err != nil {
		t.Fatalf("a broken installed agent must be replaceable: %v", err)
	}
	if calls := sshCalls(t, log); !strings.Contains(calls, "dd of=") {
		t.Errorf("broken agent was not replaced; calls were:\n%s", calls)
	}
}
