package session

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
		{"-oProxyCommand=touch-pwned", "", 0, true},
		{"user@host -oProxyCommand=x", "", 0, true},
		{"user@host:0", "", 0, true},
		{"user@host:65536", "", 0, true},
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

func TestUntrustedProjectCannotOverrideGlobalUntilDigestApproval(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)

	globalDir := filepath.Join(home, ".rdev")
	projectDir := filepath.Join(project, ".rdev")
	for _, dir := range []string{globalDir, projectDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	globalJSON := []byte(`{"hosts":[{"name":"dev","addr":"u@global"}]}`)
	projectJSON := []byte(`{"hosts":[{"name":"dev","addr":"u@project"}]}`)
	if err := os.WriteFile(filepath.Join(globalDir, "hosts.json"), globalJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	projectPath := filepath.Join(projectDir, "hosts.json")
	if err := os.WriteFile(projectPath, projectJSON, 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewRegistry()
	err := r.Load()
	var untrusted *UntrustedProjectError
	if !errors.As(err, &untrusted) {
		t.Fatalf("Load error = %v, want UntrustedProjectError", err)
	}
	host, err := r.Host("dev")
	if err != nil {
		t.Fatal(err)
	}
	if host.Addr != "u@global" {
		t.Fatalf("untrusted project overrode global host with %q", host.Addr)
	}
	trust := r.ProjectTrustStatus()
	canonicalProjectPath, err := ProjectConfigPath()
	if err != nil {
		t.Fatal(err)
	}
	if trust.Approved || trust.Path != canonicalProjectPath || trust.Digest == "" {
		t.Fatalf("pending trust = %+v", trust)
	}
	if got := r.SecuritySnapshot().SecurityRejects["project_untrusted"]; got != 1 {
		t.Errorf("project_untrusted metric = %d, want 1", got)
	}

	if _, err := r.ApproveProject(strings.Repeat("0", 64)); err == nil {
		t.Fatal("wrong digest approved the project")
	}
	approved, err := r.ApproveProject(trust.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if !approved.Approved {
		t.Fatalf("approval = %+v", approved)
	}
	host, _ = r.Host("dev")
	if host.Addr != "u@project" {
		t.Errorf("approved project did not override global host: %q", host.Addr)
	}

	trustStore := filepath.Join(globalDir, "trusted-projects.json")
	info, err := os.Stat(trustStore)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("trust store mode = %o, want 600", info.Mode().Perm())
	}
	fresh := NewRegistry()
	if err := fresh.Load(); err != nil {
		t.Fatalf("persisted approval was not honored: %v", err)
	}
	if host, _ := fresh.Host("dev"); host.Addr != "u@project" {
		t.Errorf("fresh approved load resolved %q", host.Addr)
	}

	// Any byte change invalidates the approval and falls back to the global host.
	changed := []byte(`{"hosts":[{"name":"dev","addr":"u@changed"}]}`)
	if err := os.WriteFile(projectPath, changed, 0o600); err != nil {
		t.Fatal(err)
	}
	afterChange := NewRegistry()
	if err := afterChange.Load(); !errors.As(err, &untrusted) {
		t.Fatalf("changed project Load error = %v, want untrusted", err)
	}
	if host, _ := afterChange.Host("dev"); host.Addr != "u@global" {
		t.Errorf("changed unapproved project overrode global with %q", host.Addr)
	}
}

func TestApprovalDoesNotBlessInvalidProjectConfig(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)
	if err := os.MkdirAll(filepath.Join(home, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(project, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	projectPath := filepath.Join(project, ".rdev", "hosts.json")
	b := []byte(`{"hosts":[{"name":"dev","addr":"-oProxyCommand=touch-pwned"}]}`)
	if err := os.WriteFile(projectPath, b, 0o600); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry()
	var untrusted *UntrustedProjectError
	if err := r.Load(); !errors.As(err, &untrusted) {
		t.Fatalf("Load error = %v", err)
	}
	if _, err := r.ApproveProject(untrusted.Trust.Digest); err == nil {
		t.Fatal("invalid destination was persisted as trusted")
	}
	if _, err := os.Stat(filepath.Join(home, ".rdev", "trusted-projects.json")); !os.IsNotExist(err) {
		t.Fatalf("invalid config created trust state: %v", err)
	}
}

func TestInvalidConfigIsRejectedBeforeAnyEntryMerges(t *testing.T) {
	r := NewRegistry()
	b := []byte(`{"hosts":[
		{"name":"safe","addr":"u@safe"},
		{"name":"bad","addr":"-oProxyCommand=touch-pwned"}
	]}`)
	if err := r.loadBytes("project/.rdev/hosts.json", b, ScopeProject); err == nil {
		t.Fatal("mixed config with an invalid destination was accepted")
	}
	if names := r.Names(); len(names) != 0 {
		t.Fatalf("config validation partially merged entries: %v", names)
	}
	if got := r.SecuritySnapshot().SecurityRejects["destination_invalid"]; got != 1 {
		t.Errorf("destination_invalid metric = %d, want 1", got)
	}
}

func TestSaveRejectsConfigFileAndDirectorySymlinks(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)
		configDir := filepath.Join(home, ".rdev")
		if err := os.MkdirAll(configDir, 0o700); err != nil {
			t.Fatal(err)
		}
		external := filepath.Join(t.TempDir(), "victim")
		if err := os.WriteFile(external, []byte("sentinel"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(external, filepath.Join(configDir, "hosts.json")); err != nil {
			t.Fatal(err)
		}
		r := NewRegistry()
		_ = r.Add(transport.Host{Name: "dev", Addr: "u@h"})
		r.SetScope("dev", ScopeGlobal)
		if err := r.Save(ScopeGlobal); err == nil {
			t.Fatal("Save followed a hosts.json symlink")
		}
		if b, _ := os.ReadFile(external); string(b) != "sentinel" {
			t.Fatalf("symlink target changed to %q", b)
		}
	})

	t.Run("directory", func(t *testing.T) {
		home := t.TempDir()
		project := t.TempDir()
		external := t.TempDir()
		t.Setenv("HOME", home)
		t.Chdir(project)
		if err := os.Symlink(external, filepath.Join(project, ".rdev")); err != nil {
			t.Fatal(err)
		}
		r := NewRegistry()
		_ = r.Add(transport.Host{Name: "dev", Addr: "u@h"})
		r.SetScope("dev", ScopeProject)
		if err := r.Save(ScopeProject); err == nil {
			t.Fatal("Save followed a .rdev directory symlink")
		}
		if _, err := os.Stat(filepath.Join(external, "hosts.json")); !os.IsNotExist(err) {
			t.Fatalf("directory symlink target was written: %v", err)
		}
	})
}

func TestLoadRejectsProjectConfigSymlink(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)
	if err := os.MkdirAll(filepath.Join(home, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(project, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "hosts.json")
	if err := os.WriteFile(external, []byte(`{"hosts":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(project, ".rdev", "hosts.json")); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry()
	if err := r.Load(); err == nil || !strings.Contains(err.Error(), "no-follow") {
		t.Fatalf("Load symlink error = %v", err)
	}
	if got := r.SecuritySnapshot().SecurityRejects["config_symlink"]; got != 1 {
		t.Errorf("config_symlink metric = %d, want 1", got)
	}
}

func TestSaveIsAtomicAndRepairsModes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configDir := filepath.Join(home, ".rdev")
	if err := os.MkdirAll(configDir, 0o777); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(configDir, "hosts.json")
	if err := os.WriteFile(path, []byte("old"), 0o666); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry()
	_ = r.Add(transport.Host{Name: "dev", Addr: "u@h"})
	r.SetScope("dev", ScopeGlobal)
	if err := r.Save(ScopeGlobal); err != nil {
		t.Fatal(err)
	}
	fileInfo, _ := os.Stat(path)
	dirInfo, _ := os.Stat(configDir)
	if fileInfo.Mode().Perm() != 0o600 || dirInfo.Mode().Perm() != 0o700 {
		t.Errorf("modes file=%o dir=%o, want 600/700", fileInfo.Mode().Perm(), dirInfo.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil || !json.Valid(b) {
		t.Fatalf("saved config is not complete JSON: %q, %v", b, err)
	}
}

func TestAtomicConfigWriteConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hosts.json")
	payloads := [][]byte{
		[]byte(`{"hosts":[{"name":"a","addr":"u@a"}]}`),
		[]byte(`{"hosts":[{"name":"b","addr":"u@b"}]}`),
	}
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if err := atomicWriteConfigFile(path, payloads[(worker+i)%len(payloads)]); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b) || (string(b) != string(payloads[0]) && string(b) != string(payloads[1])) {
		t.Fatalf("concurrent writers exposed partial content: %q", b)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "hosts.json" {
		t.Fatalf("temporary files leaked after writes: %v", entries)
	}
}

func setupApprovalTransaction(t *testing.T) (*Registry, ProjectTrust) {
	t.Helper()
	home, project := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)
	if err := os.MkdirAll(filepath.Join(home, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(project, ".rdev"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".rdev", "hosts.json"), []byte(`{"hosts":[{"name":"dev","addr":"u@old","cwd":"old"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(project, ".rdev", "hosts.json"), []byte(`{"hosts":[{"name":"dev","addr":"u@new","port":2200,"remote_dir":"state/new","cwd":"new"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry()
	var untrusted *UntrustedProjectError
	if err := r.Load(); !errors.As(err, &untrusted) {
		t.Fatalf("Load = %v", err)
	}
	return r, untrusted.Trust
}

func TestApproveProjectFailurePreservesCompleteLiveSnapshot(t *testing.T) {
	stages := []string{"read", "marshal", "write", "file-fsync", "rename", "dir-fsync"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			r, pending := setupApprovalTransaction(t)
			changes := 0
			r.SetHostChangeHook(func(string, uint64) { changes++ })
			switch stage {
			case "read":
				original := r.io.read
				r.io.read = func(path string) ([]byte, error) {
					if filepath.Base(path) == "trusted-projects.json" {
						return nil, errors.New("injected read failure")
					}
					return original(path)
				}
			case "marshal":
				r.io.marshal = func(any, string, string) ([]byte, error) { return nil, errors.New("injected marshal failure") }
			default:
				r.io.write = func(path string, data []byte) error {
					return atomicWriteConfigFileWithHook(path, data, func(current string) error {
						if current == stage {
							return errors.New("injected " + stage + " failure")
						}
						return nil
					})
				}
			}
			if _, err := r.ApproveProject(pending.Digest); err == nil {
				t.Fatal("injected failure was ignored")
			}
			h, err := r.Host("dev")
			if err != nil {
				t.Fatal(err)
			}
			if h.Addr != "u@old" || r.ScopeOf("dev") != ScopeGlobal || r.State("dev").Cwd != "old" {
				t.Fatalf("partial live publication: host=%+v scope=%s state=%+v", h, r.ScopeOf("dev"), r.State("dev"))
			}
			if got := r.ProjectTrustStatus(); got != pending || got.Approved {
				t.Fatalf("trust changed: %+v", got)
			}
			if changes != 0 {
				t.Fatalf("connection invalidations=%d, want 0", changes)
			}
			p, _ := trustPath()
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("failed approval persisted trust state: %v", err)
			}
		})
	}
}

func TestApproveProjectPublishesOnlyAfterDurableWrite(t *testing.T) {
	r, pending := setupApprovalTransaction(t)
	entered, release := make(chan struct{}), make(chan struct{})
	original := r.io.write
	r.io.write = func(path string, data []byte) error {
		close(entered)
		<-release
		return original(path, data)
	}
	done := make(chan error, 1)
	go func() { _, err := r.ApproveProject(pending.Digest); done <- err }()
	<-entered
	if h, _ := r.Host("dev"); h.Addr != "u@old" {
		t.Fatalf("staged host visible before commit: %+v", h)
	}
	if r.ProjectTrustStatus().Approved {
		t.Fatal("approval visible before commit")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if h, _ := r.Host("dev"); h.Addr != "u@new" {
		t.Fatalf("committed host=%+v", h)
	}
	if !r.ProjectTrustStatus().Approved {
		t.Fatal("approval not published with hosts")
	}
}

func trustStoreContains(t *testing.T, path, digest string) bool {
	t.Helper()
	p, err := trustPath()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	var tf trustFile
	if err := json.Unmarshal(b, &tf); err != nil {
		t.Fatalf("parse trust store: %v", err)
	}
	for _, project := range tf.Projects {
		if project.Path == path && project.Digest == digest {
			return true
		}
	}
	return false
}

func seedPriorTrustStore(t *testing.T) {
	t.Helper()
	p, err := trustPath()
	if err != nil {
		t.Fatal(err)
	}
	prior := []byte("{\n  \"version\": 1,\n  \"projects\": [{\"path\": \"/prior\", \"digest\": \"old\"}]\n}\n")
	if err := atomicWriteConfigFile(p, prior); err != nil {
		t.Fatal(err)
	}
}

func TestApproveProjectWriteOutcomeSemantics(t *testing.T) {
	tests := []struct {
		name          string
		seedPrior     bool
		failStages    map[string]bool
		ambiguous     bool
		committed     bool
		diskApproved  bool
		livePublished bool
	}{
		{
			name:       "first directory fsync rolls back",
			failStages: map[string]bool{"dir-fsync": true},
		},
		{
			name:         "rollback rename failure is fatal ambiguous",
			seedPrior:    true,
			failStages:   map[string]bool{"dir-fsync": true, "rollback-rename": true},
			ambiguous:    true,
			diskApproved: true,
		},
		{
			name:         "rollback unlink failure is fatal ambiguous",
			failStages:   map[string]bool{"dir-fsync": true, "rollback-unlink": true},
			ambiguous:    true,
			diskApproved: true,
		},
		{
			name:       "rollback fsync failure is fatal ambiguous",
			failStages: map[string]bool{"dir-fsync": true, "rollback-fsync": true},
			ambiguous:  true,
		},
		{
			name:          "backup unlink failure is committed warning",
			seedPrior:     true,
			failStages:    map[string]bool{"backup-unlink": true},
			committed:     true,
			diskApproved:  true,
			livePublished: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, pending := setupApprovalTransaction(t)
			if tc.seedPrior {
				seedPriorTrustStore(t)
			}
			r.io.write = func(path string, data []byte) error {
				return atomicWriteConfigFileWithHook(path, data, func(stage string) error {
					if tc.failStages[stage] {
						return errors.New("injected " + stage + " failure")
					}
					return nil
				})
			}
			trust, err := r.ApproveProject(pending.Digest)
			if err == nil {
				t.Fatal("injected write failure returned nil")
			}
			var ambiguous *ConfigWriteAmbiguousError
			if got := errors.As(err, &ambiguous); got != tc.ambiguous {
				t.Fatalf("ambiguous=%v, want %v: %v", got, tc.ambiguous, err)
			}
			var committed *ConfigWriteCommittedError
			if got := errors.As(err, &committed); got != tc.committed {
				t.Fatalf("committed=%v, want %v: %v", got, tc.committed, err)
			}
			if got := trustStoreContains(t, pending.Path, pending.Digest); got != tc.diskApproved {
				t.Fatalf("disk approval=%v, want %v", got, tc.diskApproved)
			}
			r.mu.RLock()
			liveHost := r.hosts["dev"]
			liveTrust := r.projectTrust
			fatal := r.fatal
			r.mu.RUnlock()
			if got := liveHost.Addr == "u@new" && liveTrust.Approved; got != tc.livePublished {
				t.Fatalf("live publication=%v host=%+v trust=%+v", got, liveHost, liveTrust)
			}
			if tc.committed && (!trust.Approved || fatal != nil) {
				t.Fatalf("committed warning did not return published trust: trust=%+v fatal=%v", trust, fatal)
			}
			if tc.ambiguous {
				if fatal == nil {
					t.Fatal("ambiguous write did not stop registry")
				}
				if _, resolveErr := r.Resolve("dev"); resolveErr == nil || !strings.Contains(resolveErr.Error(), "registry stopped") {
					t.Fatalf("ordinary service continued after ambiguous write: %v", resolveErr)
				}
			} else if fatal != nil {
				t.Fatalf("non-ambiguous outcome stopped registry: %v", fatal)
			}
		})
	}
}

func TestHostIdentityGenerationChangesAndNeverReuses(t *testing.T) {
	r := NewRegistry()
	for i, h := range []transport.Host{
		{Name: "dev", Addr: "u@one", Port: 22, RemoteDir: "a"},
		{Name: "dev", Addr: "u@two", Port: 22, RemoteDir: "a"},
		{Name: "dev", Addr: "u@two", Port: 2200, RemoteDir: "a"},
		{Name: "dev", Addr: "u@two", Port: 2200, RemoteDir: "b"},
		{Name: "dev", Addr: "u@one", Port: 22, RemoteDir: "a"},
	} {
		if err := r.Add(h); err != nil {
			t.Fatal(err)
		}
		resolved, err := r.Resolve("dev")
		if err != nil {
			t.Fatal(err)
		}
		if resolved.Generation != uint64(i+1) {
			t.Fatalf("step %d generation=%d", i, resolved.Generation)
		}
	}
}

func TestHostFingerprintCanonicalizesRemoteDirCompatibilitySpelling(t *testing.T) {
	r := NewRegistry()
	if err := r.Add(transport.Host{Name: "dev", Addr: "u@one", RemoteDir: ".cache/rdev"}); err != nil {
		t.Fatal(err)
	}
	before, _ := r.Resolve("dev")
	if err := r.Add(transport.Host{Name: "dev", Addr: "u@one", RemoteDir: "~/.cache/rdev"}); err != nil {
		t.Fatal(err)
	}
	after, _ := r.Resolve("dev")
	if before.Generation != after.Generation || before.Fingerprint != after.Fingerprint {
		t.Fatalf("equivalent RemoteDir changed identity: before=%+v after=%+v", before, after)
	}
}

func TestForceAgentUploadPolicyDoesNotReplaceCredentialIdentity(t *testing.T) {
	r := NewRegistry()
	if err := r.Add(transport.Host{Name: "dev", Addr: "u@one"}); err != nil {
		t.Fatal(err)
	}
	r.Update("dev", func(st *State) {
		st.Env = map[string]string{"TOKEN": "secret:tok"}
	})
	before, _ := r.Resolve("dev")
	if err := r.Add(transport.Host{Name: "dev", Addr: "u@one", ForceAgentUpload: true}); err != nil {
		t.Fatal(err)
	}
	after, _ := r.Resolve("dev")
	if before.Generation != after.Generation || before.Fingerprint != after.Fingerprint {
		t.Fatalf("bootstrap policy replaced credential identity: before=%+v after=%+v", before, after)
	}
	if before.ConnectionFingerprint == after.ConnectionFingerprint {
		t.Fatal("bootstrap policy did not invalidate the transport connection")
	}
	if got := r.State("dev").Env["TOKEN"]; got != "secret:tok" {
		t.Fatalf("bootstrap policy cleared sticky credential reference: %q", got)
	}
}

func TestHostRedefinitionClearsStickyEnvAndSecretDeclarations(t *testing.T) {
	r := NewRegistry()
	if err := r.Add(transport.Host{Name: "dev", Addr: "u@old", Port: 22}); err != nil {
		t.Fatal(err)
	}
	r.Update("dev", func(st *State) {
		st.Cwd = "~/old"
		st.Env = map[string]string{"TOKEN": "secret:tok", "PLAIN": "old"}
		st.Secrets = map[string]string{"tok": "~/old-token"}
		st.LoginShell = false
	})
	before, _ := r.Resolve("dev")
	if err := r.Add(transport.Host{Name: "dev", Addr: "u@new", Port: 22}); err != nil {
		t.Fatal(err)
	}
	after, _ := r.Resolve("dev")
	if after.Generation == before.Generation {
		t.Fatal("identity generation did not advance")
	}
	st := r.State("dev")
	if st.Cwd != "" || len(st.Env) != 0 || len(st.Secrets) != 0 || !st.LoginShell {
		t.Fatalf("old sticky state survived host redefinition: %+v", st)
	}
}

func TestScopeChangeAdvancesIdentityAndClearsStickyCredentials(t *testing.T) {
	r := NewRegistry()
	if err := r.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
		t.Fatal(err)
	}
	r.Update("dev", func(st *State) {
		st.Env = map[string]string{"TOKEN": "secret:tok"}
		st.Secrets = map[string]string{"tok": "~/token"}
	})
	before, _ := r.Resolve("dev")
	r.SetScope("dev", ScopeProject)
	after, _ := r.Resolve("dev")
	if after.Scope != ScopeProject || after.Generation == before.Generation || after.Fingerprint == before.Fingerprint {
		t.Fatalf("scope change did not replace identity: before=%+v after=%+v", before, after)
	}
	if st := r.State("dev"); len(st.Env) != 0 || len(st.Secrets) != 0 {
		t.Fatalf("scope change inherited credentials: %+v", st)
	}
}

func TestConfigOverrideReplacesRatherThanMergesOldState(t *testing.T) {
	r := NewRegistry()
	global := []byte(`{"hosts":[{"name":"dev","addr":"u@global","env":{"OLD":"secret:old"},"secrets":{"old":"~/old"}}]}`)
	project := []byte(`{"hosts":[{"name":"dev","addr":"u@project","env":{"NEW":"literal"},"secrets":{"new":"~/new"}}]}`)
	if err := r.loadBytes("global", global, ScopeGlobal); err != nil {
		t.Fatal(err)
	}
	if err := r.loadBytes("project", project, ScopeProject); err != nil {
		t.Fatal(err)
	}
	st := r.State("dev")
	if _, ok := st.Env["OLD"]; ok || st.Env["NEW"] != "literal" {
		t.Fatalf("project env merged old identity state: %+v", st.Env)
	}
	if _, ok := st.Secrets["old"]; ok || st.Secrets["new"] != "~/new" {
		t.Fatalf("project secrets merged old identity state: %+v", st.Secrets)
	}
}

func TestSecretDeclarationChangeAdvancesIdentityGeneration(t *testing.T) {
	r := NewRegistry()
	var invalidated []string
	r.SetHostChangeHook(func(name string, _ uint64) {
		invalidated = append(invalidated, name)
	})
	first := []byte(`{"hosts":[{"name":"dev","addr":"u@same","secrets":{"old":"~/old"}}]}`)
	second := []byte(`{"hosts":[{"name":"dev","addr":"u@same","secrets":{"new":"~/new"}}]}`)
	if err := r.loadBytes("global", first, ScopeGlobal); err != nil {
		t.Fatal(err)
	}
	before, err := r.Resolve("dev")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.loadBytes("global", second, ScopeGlobal); err != nil {
		t.Fatal(err)
	}
	after, err := r.Resolve("dev")
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation == before.Generation {
		t.Fatalf("secret declaration change reused generation %d", after.Generation)
	}
	st := r.State("dev")
	if _, ok := st.Secrets["old"]; ok || st.Secrets["new"] != "~/new" {
		t.Fatalf("old secret declaration survived replacement: %+v", st.Secrets)
	}
	if len(invalidated) != 2 || invalidated[0] != "dev" || invalidated[1] != "dev" {
		t.Fatalf("host change hook calls = %v", invalidated)
	}
}

func TestConfigRejectsInvalidSecretDeclarationBeforePublication(t *testing.T) {
	r := NewRegistry()
	bad := []byte(`{"hosts":[{"name":"dev","addr":"u@h","secrets":{"tok":""}}]}`)
	if err := r.loadBytes("global", bad, ScopeGlobal); err == nil || !strings.Contains(err.Error(), "nonempty name and path") {
		t.Fatalf("invalid declaration error = %v", err)
	}
	if names := r.Names(); len(names) != 0 {
		t.Fatalf("invalid config published hosts: %v", names)
	}
}
