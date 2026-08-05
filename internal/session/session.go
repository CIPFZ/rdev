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

	"github.com/tonynyyan/rdev/internal/transport"
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
}

// Registry tracks known hosts and their state. Safe for concurrent use.
type Registry struct {
	mu    sync.RWMutex
	hosts map[string]transport.Host
	state map[string]*State
}

func NewRegistry() *Registry {
	return &Registry{
		hosts: make(map[string]transport.Host),
		state: make(map[string]*State),
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
}

// Load reads ~/.rdev/hosts.json. A missing file is not an error: hosts can be
// registered at runtime instead.
func (r *Registry) Load() error {
	path, err := ConfigPath()
	if err != nil {
		return err
	}
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
			Name:      e.Name,
			Addr:      e.Addr,
			Port:      e.Port,
			RemoteDir: e.RemoteDir,
		})
		if e.Cwd != "" {
			r.Update(e.Name, func(s *State) { s.Cwd = e.Cwd })
		}
	}
	return nil
}

// Save persists the current hosts and their cwd to ~/.rdev/hosts.json.
func (r *Registry) Save() error {
	path, err := ConfigPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	r.mu.RLock()
	hf := hostFile{}
	for name, h := range r.hosts {
		e := hostEntry{Name: name, Addr: h.Addr, Port: h.Port, RemoteDir: h.RemoteDir}
		if st, ok := r.state[name]; ok {
			e.Cwd = st.Cwd
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

// ConfigPath returns the path to the host registry file.
func ConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".rdev", "hosts.json"), nil
}
