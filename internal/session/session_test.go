package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/transport"
)

func TestHostDefaultsLoginShell(t *testing.T) {
	r := NewRegistry()
	r.Add(transport.Host{Name: "dev", Addr: "user@host"})

	// Login shell defaults on so tools in ~/.local/bin resolve; a "command not
	// found" for an installed tool is far more costly than a small startup cost.
	if st := r.State("dev"); !st.LoginShell {
		t.Error("LoginShell should default to true")
	}
}

func TestHostAutoRegistersSSHDestination(t *testing.T) {
	r := NewRegistry()

	h, err := r.Host("user@1.2.3.4:2222")
	if err != nil {
		t.Fatalf("ssh destination should be accepted: %v", err)
	}
	if h.Addr != "user@1.2.3.4" || h.Port != 2222 {
		t.Errorf("parsed addr=%q port=%d, want user@1.2.3.4 / 2222", h.Addr, h.Port)
	}
}

func TestHostRejectsBareName(t *testing.T) {
	r := NewRegistry()
	// A bare word is more likely a typo than a hostname, so it is reported as
	// an unknown alias rather than silently dialed.
	if _, err := r.Host("typo"); err == nil {
		t.Error("bare name should not be auto-registered")
	}
}

func TestUpdateAndStateIsolation(t *testing.T) {
	r := NewRegistry()
	r.Add(transport.Host{Name: "dev", Addr: "user@host"})
	r.Update("dev", func(s *State) {
		s.Cwd = "~/nexus"
		s.Env = map[string]string{"A": "1"}
	})

	st := r.State("dev")
	if st.Cwd != "~/nexus" {
		t.Errorf("Cwd = %q, want ~/nexus", st.Cwd)
	}

	// State returns a copy: mutating it must not corrupt the registry.
	st.Env["A"] = "mutated"
	if again := r.State("dev"); again.Env["A"] != "1" {
		t.Errorf("registry env was mutated through returned copy: %q", again.Env["A"])
	}
}

func TestMergeEnvOverrideWins(t *testing.T) {
	base := map[string]string{"A": "base", "B": "base"}
	over := map[string]string{"B": "call"}

	got := MergeEnv(base, over)
	if got["A"] != "base" {
		t.Errorf("A = %q, want base", got["A"])
	}
	if got["B"] != "call" {
		t.Errorf("B = %q, want call (per-call value should win)", got["B"])
	}
	// The inputs must be left alone; callers reuse the sticky map.
	if base["B"] != "base" {
		t.Error("MergeEnv mutated its base argument")
	}
}

func TestMergeEnvEmpty(t *testing.T) {
	if got := MergeEnv(nil, nil); got != nil {
		t.Errorf("MergeEnv(nil, nil) = %v, want nil", got)
	}
}

func TestParseDestination(t *testing.T) {
	tests := []struct {
		in       string
		wantAddr string
		wantPort int
		wantErr  bool
	}{
		{"user@host", "user@host", 0, false},
		{"user@host:22", "user@host", 22, false},
		{"host:2222", "host", 2222, false},
		{"bareword", "", 0, true},
		{"user@host:notaport", "", 0, true},
		{"", "", 0, true},
	}
	for _, tt := range tests {
		h, err := parseDestination(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseDestination(%q) should fail", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseDestination(%q) unexpected error: %v", tt.in, err)
			continue
		}
		if h.Addr != tt.wantAddr || h.Port != tt.wantPort {
			t.Errorf("parseDestination(%q) = addr %q port %d, want %q / %d",
				tt.in, h.Addr, h.Port, tt.wantAddr, tt.wantPort)
		}
	}
}

func TestNamesSorted(t *testing.T) {
	r := NewRegistry()
	r.Add(transport.Host{Name: "zulu", Addr: "u@z"})
	r.Add(transport.Host{Name: "alpha", Addr: "u@a"})

	got := r.Names()
	if len(got) != 2 || got[0] != "alpha" || got[1] != "zulu" {
		t.Errorf("Names() = %v, want [alpha zulu]", got)
	}
}

func TestProjectScopeOverridesGlobal(t *testing.T) {
	// Simulate a repo that pins "dev" to its own machine while a global alias of
	// the same name exists. The project definition must win.
	dir := t.TempDir()
	t.Chdir(dir)

	if err := os.MkdirAll(filepath.Join(dir, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	projectJSON := `{"hosts":[{"name":"dev","addr":"user@project-box","port":2222,"cwd":"~/proj"}]}`
	if err := os.WriteFile(filepath.Join(dir, ".rdev", "hosts.json"), []byte(projectJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewRegistry()
	// Load only the project file here; the global one belongs to the real home
	// directory and must not be touched by a test.
	p, err := ProjectConfigPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadFile(p, ScopeProject); err != nil {
		t.Fatal(err)
	}

	h, err := r.Host("dev")
	if err != nil {
		t.Fatal(err)
	}
	if h.Addr != "user@project-box" || h.Port != 2222 {
		t.Errorf("host = %s:%d, want user@project-box:2222", h.Addr, h.Port)
	}
	if got := r.ScopeOf("dev"); got != ScopeProject {
		t.Errorf("ScopeOf(dev) = %q, want project", got)
	}
	if got := r.State("dev").Cwd; got != "~/proj" {
		t.Errorf("Cwd = %q, want ~/proj", got)
	}
}

func TestSaveOnlyWritesMatchingScope(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	r := NewRegistry()
	r.Add(transport.Host{Name: "proj", Addr: "user@p"})
	r.SetScope("proj", ScopeProject)
	r.Add(transport.Host{Name: "glob", Addr: "user@g"})
	r.SetScope("glob", ScopeGlobal)

	if err := r.Save(ScopeProject); err != nil {
		t.Fatal(err)
	}

	// Saving the project file must not absorb global hosts, or a private machine
	// would silently leak into a committed config.
	b, err := os.ReadFile(filepath.Join(dir, ".rdev", "hosts.json"))
	if err != nil {
		t.Fatal(err)
	}
	body := string(b)
	if !strings.Contains(body, "proj") {
		t.Error("project host missing from project file")
	}
	if strings.Contains(body, "glob") {
		t.Error("global host leaked into the project file")
	}
}

func TestScopeDefaultsToGlobalForUnknownHost(t *testing.T) {
	r := NewRegistry()
	if got := r.ScopeOf("never-registered"); got != ScopeGlobal {
		t.Errorf("ScopeOf() = %q, want global as the default", got)
	}
}

// rdev_session reported saved=true while dropping env and login_shell, so a
// caller's sticky context silently vanished on restart.
func TestSaveRoundTripsEnvAndLoginShell(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	r := NewRegistry()
	r.Add(transport.Host{Name: "dev", Addr: "u@h", Port: 36000})
	r.SetScope("dev", ScopeGlobal)
	r.Update("dev", func(s *State) {
		s.Cwd = "~/proj"
		s.Env = map[string]string{"PROXY": "http://p:1", "TOKEN": "secret:tok"}
		s.LoginShell = false
	})
	if err := r.Save(ScopeGlobal); err != nil {
		t.Fatal(err)
	}

	fresh := NewRegistry()
	if err := fresh.Load(); err != nil {
		t.Fatal(err)
	}
	st := fresh.State("dev")
	if st.Cwd != "~/proj" {
		t.Errorf("Cwd = %q, want ~/proj", st.Cwd)
	}
	if st.Env["PROXY"] != "http://p:1" {
		t.Errorf("Env[PROXY] = %q, want it persisted", st.Env["PROXY"])
	}
	// A secret reference is stored by name and resolved at request time, so it
	// must survive verbatim rather than being expanded on save.
	if st.Env["TOKEN"] != "secret:tok" {
		t.Errorf("Env[TOKEN] = %q, want the unresolved reference", st.Env["TOKEN"])
	}
	if st.LoginShell {
		t.Error("LoginShell = true, want the saved false to survive")
	}
}

// The default is true, so an absent field must not be read as false.
func TestLoadDefaultsLoginShellTrueWhenAbsent(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	r := NewRegistry()
	r.Add(transport.Host{Name: "dev", Addr: "u@h"})
	r.SetScope("dev", ScopeGlobal)
	r.Update("dev", func(s *State) { s.Cwd = "~/x" })
	if err := r.Save(ScopeGlobal); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(filepath.Join(dir, ".rdev", "hosts.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "login_shell") {
		t.Errorf("a default-true login_shell should not be written: %s", b)
	}

	fresh := NewRegistry()
	if err := fresh.Load(); err != nil {
		t.Fatal(err)
	}
	if !fresh.State("dev").LoginShell {
		t.Error("LoginShell = false, want the true default when the field is absent")
	}
}

// ForceAgentUpload suppresses the downgrade refusal, so it has to survive a save
// and reload: a flag that silently reverted to false on the next session would
// bring back the agent flip-flop it was set to stop.
func TestSaveRoundTripsForceAgentUpload(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)

	r := NewRegistry()
	r.Add(transport.Host{Name: "shared", Addr: "u@h", ForceAgentUpload: true})
	r.SetScope("shared", ScopeGlobal)
	r.Add(transport.Host{Name: "normal", Addr: "u@h2"})
	r.SetScope("normal", ScopeGlobal)
	if err := r.Save(ScopeGlobal); err != nil {
		t.Fatal(err)
	}

	fresh := NewRegistry()
	if err := fresh.Load(); err != nil {
		t.Fatal(err)
	}
	h, err := fresh.Host("shared")
	if err != nil {
		t.Fatal(err)
	}
	if !h.ForceAgentUpload {
		t.Error("ForceAgentUpload did not survive the round trip")
	}
	// And the default stays off: forcing uploads everywhere would defeat the check.
	other, err := fresh.Host("normal")
	if err != nil {
		t.Fatal(err)
	}
	if other.ForceAgentUpload {
		t.Error("a host that never asked for it must not come back with ForceAgentUpload set")
	}
}
