// Package session holds per-host state and the host registry.
//
// State exists so a caller does not repeat cwd and env on every request: set
// cwd once for a host and later calls inherit it. Hosts come from
// ~/.rdev/hosts.json when present, and can also be registered at runtime.
package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/CIPFZ/rdev/internal/transport"
)

// State is the sticky context applied to a host's requests.
type State struct {
	Cwd string            `json:"cwd,omitempty"`
	Env map[string]string `json:"env,omitempty"`
	// LoginShell is the default for exec and jobs on this host.
	//
	// Defaults to true: a non-login ssh command misses ~/.bashrc, so tools in
	// ~/.local/bin (uv, pipx, cargo) are invisible. Paying a small startup cost
	// beats "command not found" on a tool that is plainly installed.
	LoginShell bool `json:"login_shell"`
	// Secrets maps a secret name to a file path on this host, read on first
	// connect. Paths only; values never touch local disk.
	Secrets map[string]string `json:"secrets,omitempty"`
}

// Scope identifies which config file a host is defined in.
type Scope string

const (
	// ScopeGlobal hosts live in ~/.rdev/hosts.json and are visible everywhere.
	ScopeGlobal Scope = "global"
	// ScopeProject hosts live in <project>/.rdev/hosts.json and are visible only
	// while working in that directory. Use this for machines that belong to one
	// codebase, so an alias like "dev" cannot be reached from an unrelated repo.
	ScopeProject Scope = "project"
)

// Registry tracks known hosts and their state. Safe for concurrent use.
type Registry struct {
	mu     sync.RWMutex
	hosts  map[string]transport.Host
	state  map[string]*State
	scopes map[string]Scope
}

func NewRegistry() *Registry {
	return &Registry{
		hosts:  make(map[string]transport.Host),
		state:  make(map[string]*State),
		scopes: make(map[string]Scope),
	}
}

// hostFile is the on-disk registry format.
type hostFile struct {
	Hosts []hostEntry `json:"hosts"`
}

type hostEntry struct {
	Name      string `json:"name"`
	Addr      string `json:"addr"`
	Port      int    `json:"port,omitempty"`
	RemoteDir string `json:"remote_dir,omitempty"`
	Cwd       string `json:"cwd,omitempty"`
	// ForceAgentUpload installs the local agent even when the installed one was
	// built later. Persisted per host because the situation that calls for it --
	// a shared box where agents are stamped from another branch -- is a property
	// of that host, not of one command.
	ForceAgentUpload bool `json:"force_agent_upload,omitempty"`
	// Env is the host's sticky environment. Persisted so a caller that sets it
	// once does not have to re-set it every session.
	//
	// Values are stored verbatim, including any "secret:NAME" references, which
	// resolve at request time. Never put a plaintext credential here: this file
	// is 0600 but it is still a file on disk.
	Env map[string]string `json:"env,omitempty"`
	// LoginShell is a pointer so an explicit false survives a round trip. With a
	// plain bool, "omitempty" would drop it and Load would restore the true
	// default, silently discarding the caller's choice.
	LoginShell *bool `json:"login_shell,omitempty"`
	// Secrets names credential files on this host to register for redaction on
	// first connect, as {"gftoken": "~/.nexus/auth/gongfeng/key"}.
	//
	// Only the path is stored. The value is read over the agent connection when
	// the host is first used, so no plaintext reaches local disk -- which is the
	// whole reason the store is in-memory. Without this, every new session had to
	// re-register by hand, and a credential nobody remembered to register is a
	// credential that leaks into a transcript verbatim.
	Secrets map[string]string `json:"secrets,omitempty"`
}

// Load reads the global registry and then the project one, so a project can
// define hosts that exist only while you work in that directory.
//
// Order matters: the project file is read last and wins on name collisions,
// letting a repo pin "dev" to its own machine even if a global alias exists.
// A missing file at either level is not an error; hosts can also be registered
// at runtime.
func (r *Registry) Load() error {
	global, err := ConfigPath()
	if err != nil {
		return err
	}
	if err := r.loadFile(global, ScopeGlobal); err != nil {
		return err
	}

	// The MCP server inherits the project directory as its cwd, which is what
	// makes a project-scoped host file work without extra configuration.
	if project, err := ProjectConfigPath(); err == nil {
		if err := r.loadFile(project, ScopeProject); err != nil {
			return err
		}
	}
	return nil
}

// loadFile merges one host file into the registry, tagging each entry's scope.
func (r *Registry) loadFile(path string, scope Scope) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var hf hostFile
	if err := json.Unmarshal(b, &hf); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	for _, e := range hf.Hosts {
		if e.Name == "" || e.Addr == "" {
			continue
		}
		r.Add(transport.Host{
			Name:             e.Name,
			Addr:             e.Addr,
			Port:             e.Port,
			RemoteDir:        e.RemoteDir,
			ForceAgentUpload: e.ForceAgentUpload,
		})
		r.mu.Lock()
		r.scopes[e.Name] = scope
		r.mu.Unlock()
		// Apply persisted state in one update so a host loaded from disk starts
		// with exactly the context it was saved with.
		if e.Cwd != "" || len(e.Env) > 0 || e.LoginShell != nil || len(e.Secrets) > 0 {
			r.Update(e.Name, func(s *State) {
				if e.Cwd != "" {
					s.Cwd = e.Cwd
				}
				if len(e.Env) > 0 {
					s.Env = MergeEnv(s.Env, e.Env)
				}
				if e.LoginShell != nil {
					s.LoginShell = *e.LoginShell
				}
				if len(e.Secrets) > 0 {
					s.Secrets = MergeEnv(s.Secrets, e.Secrets)
				}
			})
		}
	}
	return nil
}

// Save persists hosts to the given scope's file.
//
// Only hosts belonging to that scope are written, so saving the global file does
// not silently absorb a project's private hosts (and vice versa).
func (r *Registry) Save(scope Scope) error {
	var path string
	var err error
	switch scope {
	case ScopeProject:
		path, err = ProjectConfigPath()
	default:
		path, err = ConfigPath()
	}
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	r.mu.RLock()
	hf := hostFile{}
	for name, h := range r.hosts {
		if r.scopes[name] != scope {
			continue
		}
		e := hostEntry{
			Name: name, Addr: h.Addr, Port: h.Port, RemoteDir: h.RemoteDir,
			ForceAgentUpload: h.ForceAgentUpload,
		}
		if st, ok := r.state[name]; ok {
			e.Cwd = st.Cwd
			if len(st.Env) > 0 {
				e.Env = make(map[string]string, len(st.Env))
				for k, v := range st.Env {
					e.Env[k] = v
				}
			}
			// Only record a non-default value: writing login_shell:true into
			// every entry is noise, since true is what an absent field means.
			if !st.LoginShell {
				no := false
				e.LoginShell = &no
			}
			if len(st.Secrets) > 0 {
				e.Secrets = make(map[string]string, len(st.Secrets))
				for k, v := range st.Secrets {
					e.Secrets[k] = v
				}
			}
		}
		hf.Hosts = append(hf.Hosts, e)
	}
	r.mu.RUnlock()

	sort.Slice(hf.Hosts, func(i, j int) bool { return hf.Hosts[i].Name < hf.Hosts[j].Name })
	b, err := json.MarshalIndent(hf, "", "  ")
	if err != nil {
		return err
	}
	// Written 0600: it records hostnames and usernames, not secrets, but there
	// is no reason to make it world-readable.
	return os.WriteFile(path, append(b, '\n'), 0o600)
}

// SetScope records which config file a host belongs to.
func (r *Registry) SetScope(name string, scope Scope) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scopes[name] = scope
}

// ScopeOf reports which config file a host came from.
func (r *Registry) ScopeOf(name string) Scope {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if s, ok := r.scopes[name]; ok {
		return s
	}
	return ScopeGlobal
}

// Add registers or replaces a host, initializing its state.
func (r *Registry) Add(h transport.Host) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h.Name == "" {
		h.Name = h.Addr
	}
	r.hosts[h.Name] = h
	if _, ok := r.state[h.Name]; !ok {
		r.state[h.Name] = &State{LoginShell: true}
	}
}

// Host resolves a host by name.
//
// An unknown name that looks like an ssh destination ("user@host",
// "host:port") is accepted and auto-registered, so a one-off machine works
// without editing a config file first.
func (r *Registry) Host(name string) (transport.Host, error) {
	r.mu.RLock()
	h, ok := r.hosts[name]
	r.mu.RUnlock()
	if ok {
		return h, nil
	}

	if parsed, err := parseDestination(name); err == nil {
		r.Add(parsed)
		return parsed, nil
	}
	return transport.Host{}, fmt.Errorf("unknown host %q (known: %s)", name, strings.Join(r.Names(), ", "))
}

// parseDestination interprets "user@host", "user@host:port", or "host:port".
func parseDestination(s string) (transport.Host, error) {
	if s == "" {
		return transport.Host{}, errors.New("empty destination")
	}
	addr, portStr, hasPort := strings.Cut(s, ":")
	h := transport.Host{Name: s, Addr: addr}
	if hasPort {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return transport.Host{}, fmt.Errorf("invalid port %q", portStr)
		}
		h.Port = port
	}
	// Require an "@" or an explicit port, so a bare typo is reported as an
	// unknown host rather than silently treated as a hostname.
	if !strings.Contains(addr, "@") && !hasPort {
		return transport.Host{}, fmt.Errorf("%q is not a host alias or ssh destination", s)
	}
	return h, nil
}

// Names lists registered host names, sorted.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.hosts))
	for k := range r.hosts {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// State returns a copy of a host's state, so callers cannot mutate it directly.
func (r *Registry) State(name string) State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.state[name]
	if !ok {
		return State{LoginShell: true}
	}
	cp := State{Cwd: st.Cwd, LoginShell: st.LoginShell}
	if len(st.Env) > 0 {
		cp.Env = make(map[string]string, len(st.Env))
		for k, v := range st.Env {
			cp.Env[k] = v
		}
	}
	if len(st.Secrets) > 0 {
		cp.Secrets = make(map[string]string, len(st.Secrets))
		for k, v := range st.Secrets {
			cp.Secrets[k] = v
		}
	}
	return cp
}

// Update mutates a host's state under lock.
func (r *Registry) Update(name string, fn func(*State)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.state[name]
	if !ok {
		st = &State{LoginShell: true}
		r.state[name] = st
	}
	fn(st)
}

// MergeEnv layers per-call env over the host's sticky env, with the per-call
// values winning.
func MergeEnv(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

// ConfigPath returns the path to the global host registry file.
func ConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".rdev", "hosts.json"), nil
}

// ProjectConfigPath returns the path to the project-local host registry.
//
// It resolves against the process working directory, which for the MCP server is
// the project Claude Code was started in. That is what scopes a host to one
// codebase without any extra plumbing.
func ProjectConfigPath() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(cwd, ".rdev", "hosts.json"), nil
}
