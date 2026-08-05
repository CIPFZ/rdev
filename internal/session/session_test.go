package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tonynyyan/rdev/internal/transport"
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
