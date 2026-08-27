// Package session holds per-host state and the host registry.
//
// State exists so a caller does not repeat cwd and env on every request: set
// cwd once for a host and later calls inherit it. Hosts come from
// ~/.rdev/hosts.json when present, and can also be registered at runtime.
package session

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/CIPFZ/rdev/internal/observe"
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
	// Lock order for identity-changing writes is: per-alias lease(s), tx, mu.
	// A sink holds only one alias lease for its lifetime, so a slow operation on
	// one machine never serializes unrelated hosts. Multi-alias publications
	// sort names before locking to avoid lock-order cycles.
	leaseMu      sync.Mutex
	identity     map[string]*sync.RWMutex
	approvalMu   sync.Mutex
	tx           sync.Mutex
	mu           sync.RWMutex
	hosts        map[string]transport.Host
	state        map[string]*State
	scopes       map[string]Scope
	generations  map[string]uint64
	nextGen      uint64
	projectTrust ProjectTrust
	fatal        error
	observe      *observe.Registry
	onHostChange func(string, uint64)
	io           registryIO
}

type registryIO struct {
	read    func(string) ([]byte, error)
	marshal func(any, string, string) ([]byte, error)
	write   func(string, []byte) error
}

func NewRegistry() *Registry {
	return &Registry{
		hosts:       make(map[string]transport.Host),
		state:       make(map[string]*State),
		scopes:      make(map[string]Scope),
		generations: make(map[string]uint64),
		identity:    make(map[string]*sync.RWMutex),
		observe:     observe.New(nil),
		io:          registryIO{read: readConfigFile, marshal: json.MarshalIndent, write: atomicWriteConfigFile},
	}
}

func (r *Registry) identityLease(name string) *sync.RWMutex {
	r.leaseMu.Lock()
	defer r.leaseMu.Unlock()
	lease := r.identity[name]
	if lease == nil {
		lease = &sync.RWMutex{}
		r.identity[name] = lease
	}
	return lease
}

func candidateNames(candidates []hostCandidate) []string {
	names := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		name := candidate.host.Name
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (r *Registry) lockIdentityWrites(names []string) func() {
	leases := make([]*sync.RWMutex, 0, len(names))
	for _, name := range names {
		lease := r.identityLease(name)
		lease.Lock()
		leases = append(leases, lease)
	}
	return func() {
		for i := len(leases) - 1; i >= 0; i-- {
			leases[i].Unlock()
		}
	}
}

// ResolvedHost is an immutable connection identity. Generation prevents an old
// connection from becoming valid again if an alias is changed and later reverted.
type ResolvedHost struct {
	Host        transport.Host
	Fingerprint string
	Generation  uint64
}

func hostFingerprint(h transport.Host) string {
	remoteDir, _ := transport.ValidateRemoteDir(h.RemoteDir)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%t", h.Addr, h.Port, remoteDir, h.ForceAgentUpload)))
	return fmt.Sprintf("%x", sum[:])
}

// SetHostChangeHook installs the connection-pool invalidation hook. Registry
// publication is already complete when the hook runs.
func (r *Registry) SetHostChangeHook(hook func(string, uint64)) {
	r.mu.Lock()
	r.onHostChange = hook
	r.mu.Unlock()
}

// SetObserver installs the process-local security metrics and structured event
// sink. It is primarily a stable seam for the future status/doctor exporter.
func (r *Registry) SetObserver(registry *observe.Registry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if registry == nil {
		registry = observe.New(nil)
	}
	r.observe = registry
}

func (r *Registry) SecuritySnapshot() observe.Snapshot {
	r.mu.RLock()
	registry := r.observe
	r.mu.RUnlock()
	return registry.Snapshot()
}

func (r *Registry) fatalError() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.fatal == nil {
		return nil
	}
	return fmt.Errorf("registry stopped after ambiguous authorization-state write: %w", r.fatal)
}

func (r *Registry) stopOnAmbiguousWrite(err error) {
	if !configWriteAmbiguous(err) {
		return
	}
	r.mu.Lock()
	if r.fatal == nil {
		r.fatal = err
	}
	r.mu.Unlock()
}

func (r *Registry) reject(reason observe.SecurityReason, target string) {
	r.mu.RLock()
	registry := r.observe
	r.mu.RUnlock()
	registry.Reject(reason, target)
}

func configReadReason(err error) observe.SecurityReason {
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "symlink") || strings.Contains(message, "no-follow") ||
		strings.Contains(message, "too many levels of symbolic links") {
		return observe.ReasonConfigSymlink
	}
	return observe.ReasonConfigInvalid
}

// hostFile is the on-disk registry format.
type hostFile struct {
	Hosts []hostEntry `json:"hosts"`
}

const trustFileVersion = 1

type trustFile struct {
	Version  int              `json:"version"`
	Projects []trustedProject `json:"projects"`
}

type trustedProject struct {
	Path   string `json:"path"`
	Digest string `json:"digest"`
}

// ProjectTrust is the reviewable trust decision for the current project file.
// Approval binds both its absolute path and the SHA-256 of its exact bytes.
type ProjectTrust struct {
	Path     string `json:"path,omitempty"`
	Digest   string `json:"digest,omitempty"`
	Approved bool   `json:"approved"`
}

// UntrustedProjectError means a project file was deliberately not loaded.
// Global hosts remain available; only the unapproved repository data is skipped.
type UntrustedProjectError struct {
	Trust ProjectTrust
}

func (e *UntrustedProjectError) Error() string {
	return fmt.Sprintf(
		"project host config is not approved: %s (sha256:%s); review it, then run `rdev hosts approve-project %s`",
		e.Trust.Path, e.Trust.Digest, e.Trust.Digest)
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

type hostCandidate struct {
	host  transport.Host
	entry hostEntry
}

type registrySnapshot struct {
	hosts       map[string]transport.Host
	state       map[string]*State
	scopes      map[string]Scope
	generations map[string]uint64
	nextGen     uint64
	trust       ProjectTrust
}

func cloneState(st *State) *State {
	if st == nil {
		return &State{LoginShell: true}
	}
	return &State{
		Cwd: st.Cwd, LoginShell: st.LoginShell,
		Env: MergeEnv(nil, st.Env), Secrets: MergeEnv(nil, st.Secrets),
	}
}

func (r *Registry) snapshotLocked() registrySnapshot {
	s := registrySnapshot{
		hosts:       make(map[string]transport.Host, len(r.hosts)),
		state:       make(map[string]*State, len(r.state)),
		scopes:      make(map[string]Scope, len(r.scopes)),
		generations: make(map[string]uint64, len(r.generations)),
		nextGen:     r.nextGen, trust: r.projectTrust,
	}
	for k, v := range r.hosts {
		s.hosts[k] = v
	}
	for k, v := range r.state {
		s.state[k] = cloneState(v)
	}
	for k, v := range r.scopes {
		s.scopes[k] = v
	}
	for k, v := range r.generations {
		s.generations[k] = v
	}
	return s
}

func applyCandidates(s *registrySnapshot, candidates []hostCandidate, scope Scope) []string {
	changed := make([]string, 0, len(candidates))
	for _, c := range candidates {
		h, e := c.host, c.entry
		old, exists := s.hosts[h.Name]
		if !exists || hostFingerprint(old) != hostFingerprint(h) {
			s.nextGen++
			s.generations[h.Name] = s.nextGen
			changed = append(changed, h.Name)
		}
		s.hosts[h.Name] = h
		s.scopes[h.Name] = scope
		st, ok := s.state[h.Name]
		if !ok {
			st = &State{LoginShell: true}
			s.state[h.Name] = st
		}
		if e.Cwd != "" {
			st.Cwd = e.Cwd
		}
		if len(e.Env) > 0 {
			st.Env = MergeEnv(st.Env, e.Env)
		}
		if e.LoginShell != nil {
			st.LoginShell = *e.LoginShell
		}
		if len(e.Secrets) > 0 {
			st.Secrets = MergeEnv(st.Secrets, e.Secrets)
		}
	}
	return changed
}

func (r *Registry) publishLocked(s registrySnapshot) (func(string, uint64), map[string]uint64) {
	r.hosts, r.state, r.scopes = s.hosts, s.state, s.scopes
	r.generations, r.nextGen, r.projectTrust = s.generations, s.nextGen, s.trust
	gens := make(map[string]uint64, len(s.generations))
	for k, v := range s.generations {
		gens[k] = v
	}
	return r.onHostChange, gens
}

func (r *Registry) parseCandidates(path string, b []byte) ([]hostCandidate, error) {
	var hf hostFile
	if err := json.Unmarshal(b, &hf); err != nil {
		r.reject(observe.ReasonConfigInvalid, path)
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	candidates := make([]hostCandidate, 0, len(hf.Hosts))
	for _, e := range hf.Hosts {
		if e.Name == "" || e.Addr == "" {
			r.reject(observe.ReasonConfigInvalid, path)
			return nil, fmt.Errorf("parse %s: every host requires non-empty name and addr", path)
		}
		h := transport.Host{Name: e.Name, Addr: e.Addr, Port: e.Port, RemoteDir: e.RemoteDir, ForceAgentUpload: e.ForceAgentUpload}
		if err := transport.ValidateHost(h); err != nil {
			reason := observe.ReasonRemoteDir
			if transport.ValidateDestination(h.Addr, h.Port) != nil {
				reason = observe.ReasonDestination
			}
			r.reject(reason, e.Name)
			return nil, fmt.Errorf("parse %s host %q: %w", path, e.Name, err)
		}
		candidates = append(candidates, hostCandidate{host: h, entry: e})
	}
	return candidates, nil
}

// Load reads the global registry and then the project one, so a project can
// define hosts that exist only while you work in that directory.
//
// Order matters: an approved project file is read last and wins on name
// collisions, letting a repo pin "dev" to its own machine without allowing an
// unreviewed checkout to replace a global destination.
// A missing file at either level is not an error; hosts can also be registered
// at runtime.
func (r *Registry) Load() error {
	if err := r.fatalError(); err != nil {
		return err
	}
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
		b, err := r.io.read(project)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			r.reject(configReadReason(err), project)
			return err
		}
		trust := ProjectTrust{Path: filepath.Clean(project), Digest: digestBytes(b)}
		approved, err := r.projectDigestApproved(trust.Path, trust.Digest)
		if err != nil {
			return fmt.Errorf("read project trust store: %w", err)
		}
		trust.Approved = approved
		r.mu.Lock()
		r.projectTrust = trust
		r.mu.Unlock()
		if !approved {
			r.reject(observe.ReasonProjectUntrusted, trust.Path)
			return &UntrustedProjectError{Trust: trust}
		}
		if err := r.loadBytes(project, b, ScopeProject); err != nil {
			return err
		}
	}
	return nil
}

// loadFile merges one host file into the registry, tagging each entry's scope.
func (r *Registry) loadFile(path string, scope Scope) error {
	b, err := r.io.read(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		r.reject(configReadReason(err), path)
		return err
	}

	return r.loadBytes(path, b, scope)
}

func (r *Registry) loadBytes(path string, b []byte, scope Scope) error {
	candidates, err := r.parseCandidates(path, b)
	if err != nil {
		return err
	}
	releaseIdentities := r.lockIdentityWrites(candidateNames(candidates))
	defer releaseIdentities()
	r.tx.Lock()
	r.mu.Lock()
	s := r.snapshotLocked()
	changed := applyCandidates(&s, candidates, scope)
	hook, generations := r.publishLocked(s)
	r.mu.Unlock()
	r.tx.Unlock()
	if hook != nil {
		for _, name := range changed {
			hook(name, generations[name])
		}
	}
	return nil
}

// Save persists hosts to the given scope's file.
//
// Only hosts belonging to that scope are written, so saving the global file does
// not silently absorb a project's private hosts (and vice versa).
func (r *Registry) Save(scope Scope) error {
	if err := r.fatalError(); err != nil {
		return err
	}
	r.tx.Lock()
	defer r.tx.Unlock()
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
	b, err := r.io.marshal(hf, "", "  ")
	if err != nil {
		return err
	}
	// Written 0600: it records hostnames and usernames, not secrets, but there
	// is no reason to make it world-readable.
	err = r.io.write(path, append(b, '\n'))
	r.stopOnAmbiguousWrite(err)
	return err
}

func digestBytes(b []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func trustPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".rdev", "trusted-projects.json"), nil
}

func (r *Registry) loadTrustFile() (trustFile, error) {
	p, err := trustPath()
	if err != nil {
		return trustFile{}, err
	}
	b, err := r.io.read(p)
	if err != nil {
		if os.IsNotExist(err) {
			return trustFile{Version: trustFileVersion}, nil
		}
		return trustFile{}, err
	}
	var tf trustFile
	if err := json.Unmarshal(b, &tf); err != nil {
		return trustFile{}, fmt.Errorf("parse %s: %w", p, err)
	}
	if tf.Version != trustFileVersion {
		return trustFile{}, fmt.Errorf("unsupported trust store version %d", tf.Version)
	}
	return tf, nil
}

func (r *Registry) projectDigestApproved(projectPath, digest string) (bool, error) {
	tf, err := r.loadTrustFile()
	if err != nil {
		return false, err
	}
	for _, project := range tf.Projects {
		if project.Path == projectPath && project.Digest == digest {
			return true, nil
		}
	}
	return false, nil
}

// ProjectTrustStatus returns the current project's path, digest, and approval.
func (r *Registry) ProjectTrustStatus() ProjectTrust {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.projectTrust
}

// ApproveProject loads the current project config only after the caller repeats
// its exact digest. The approval is persisted outside the repository.
func (r *Registry) ApproveProject(digest string) (ProjectTrust, error) {
	if err := r.fatalError(); err != nil {
		return ProjectTrust{}, err
	}
	// Approval persistence is serialized separately from live registry writes.
	// In particular, never hold tx while waiting for an alias lease: otherwise
	// an active command on host A would also block an update to host B.
	r.approvalMu.Lock()
	defer r.approvalMu.Unlock()

	projectPath, err := ProjectConfigPath()
	if err != nil {
		return ProjectTrust{}, err
	}
	b, err := r.io.read(projectPath)
	if err != nil {
		return ProjectTrust{}, err
	}
	trust := ProjectTrust{Path: filepath.Clean(projectPath), Digest: digestBytes(b)}
	if digest == "" || !strings.EqualFold(digest, trust.Digest) {
		return trust, fmt.Errorf("project config digest mismatch: got %q, current sha256 is %s", digest, trust.Digest)
	}
	candidates, err := r.parseCandidates(projectPath, b)
	if err != nil {
		return trust, err
	}

	tf, err := r.loadTrustFile()
	if err != nil {
		return trust, err
	}
	projects := make([]trustedProject, 0, len(tf.Projects)+1)
	for _, project := range tf.Projects {
		if project.Path != trust.Path {
			projects = append(projects, project)
		}
	}
	projects = append(projects, trustedProject{Path: trust.Path, Digest: trust.Digest})
	sort.Slice(projects, func(i, j int) bool { return projects[i].Path < projects[j].Path })
	tf.Version = trustFileVersion
	tf.Projects = projects
	out, err := r.io.marshal(tf, "", "  ")
	if err != nil {
		return trust, err
	}
	p, err := trustPath()
	if err != nil {
		return trust, err
	}
	writeErr := r.io.write(p, append(out, '\n'))
	if writeErr != nil && !configWriteCommitted(writeErr) {
		r.stopOnAmbiguousWrite(writeErr)
		return trust, writeErr
	}

	// The trust record is durable. Pin every affected alias before taking the
	// current live snapshot so publication is atomic with respect to active
	// sinks while unrelated aliases remain fully independent.
	releaseIdentities := r.lockIdentityWrites(candidateNames(candidates))
	defer releaseIdentities()
	r.tx.Lock()
	trust.Approved = true
	r.mu.Lock()
	staged := r.snapshotLocked()
	changed := applyCandidates(&staged, candidates, ScopeProject)
	staged.trust = trust
	hook, generations := r.publishLocked(staged)
	registry := r.observe
	r.mu.Unlock()
	r.tx.Unlock()
	if hook != nil {
		for _, name := range changed {
			hook(name, generations[name])
		}
	}
	registry.ProjectApproved(trust.Path)
	return trust, writeErr
}

// SetScope records which config file a host belongs to.
func (r *Registry) SetScope(name string, scope Scope) {
	r.tx.Lock()
	defer r.tx.Unlock()
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

// Add registers or replaces a validated host, initializing its state.
func (r *Registry) Add(h transport.Host) error {
	if err := r.fatalError(); err != nil {
		return err
	}
	if err := transport.ValidateHost(h); err != nil {
		reason := observe.ReasonConfigInvalid
		if destErr := transport.ValidateDestination(h.Addr, h.Port); destErr != nil {
			reason = observe.ReasonDestination
		} else {
			reason = observe.ReasonRemoteDir
		}
		r.reject(reason, h.Name)
		return err
	}
	if h.Name == "" {
		h.Name = h.Addr
	}
	releaseIdentity := r.lockIdentityWrites([]string{h.Name})
	defer releaseIdentity()
	r.tx.Lock()
	r.mu.Lock()
	old, exists := r.hosts[h.Name]
	r.hosts[h.Name] = h
	if _, ok := r.state[h.Name]; !ok {
		r.state[h.Name] = &State{LoginShell: true}
	}
	var generation uint64
	changed := !exists || hostFingerprint(old) != hostFingerprint(h)
	if changed {
		r.nextGen++
		generation = r.nextGen
		r.generations[h.Name] = generation
	}
	hook := r.onHostChange
	r.mu.Unlock()
	r.tx.Unlock()
	if changed && hook != nil {
		hook(h.Name, generation)
	}
	return nil
}

// Host resolves a host by name.
//
// An unknown name that looks like an ssh destination ("user@host",
// "host:port") is accepted and auto-registered, so a one-off machine works
// without editing a config file first.
func (r *Registry) Host(name string) (transport.Host, error) {
	resolved, err := r.Resolve(name)
	return resolved.Host, err
}

// Resolve returns a host together with the immutable identity used by the
// connection pool.
func (r *Registry) Resolve(name string) (ResolvedHost, error) {
	if err := r.fatalError(); err != nil {
		return ResolvedHost{}, err
	}
	r.mu.RLock()
	h, ok := r.hosts[name]
	generation := r.generations[name]
	r.mu.RUnlock()
	if ok {
		return ResolvedHost{Host: h, Fingerprint: hostFingerprint(h), Generation: generation}, nil
	}

	if parsed, err := parseDestination(name); err == nil {
		if err := r.Add(parsed); err != nil {
			return ResolvedHost{}, err
		}
		r.mu.RLock()
		h, generation = r.hosts[parsed.Name], r.generations[parsed.Name]
		r.mu.RUnlock()
		return ResolvedHost{Host: h, Fingerprint: hostFingerprint(h), Generation: generation}, nil
	}
	return ResolvedHost{}, fmt.Errorf("unknown host %q (known: %s)", name, strings.Join(r.Names(), ", "))
}

// AcquireIdentity pins one resolved identity until release. An update to this
// alias cannot publish while a sink using the old identity is in progress;
// updates to all other aliases remain independent.
func (r *Registry) AcquireIdentity(name string, generation uint64, fingerprint string) (func(), bool) {
	if r.fatalError() != nil {
		return nil, false
	}
	lease := r.identityLease(name)
	lease.RLock()
	r.mu.RLock()
	h, ok := r.hosts[name]
	if !ok || r.generations[name] != generation || hostFingerprint(h) != fingerprint {
		r.mu.RUnlock()
		lease.RUnlock()
		return nil, false
	}
	r.mu.RUnlock()
	return lease.RUnlock, true
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
		if err != nil || port < 1 || port > 65535 {
			return transport.Host{}, fmt.Errorf("invalid port %q", portStr)
		}
		h.Port = port
	}
	// Require an "@" or an explicit port, so a bare typo is reported as an
	// unknown host rather than silently treated as a hostname.
	if !strings.Contains(addr, "@") && !hasPort {
		return transport.Host{}, fmt.Errorf("%q is not a host alias or ssh destination", s)
	}
	if err := transport.ValidateDestination(h.Addr, h.Port); err != nil {
		return transport.Host{}, err
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
	r.tx.Lock()
	defer r.tx.Unlock()
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
	// Bind approvals to one physical project identity rather than allowing the
	// same checkout to accumulate separate decisions through symlink aliases.
	if resolved, err := filepath.EvalSymlinks(cwd); err == nil {
		cwd = resolved
	}
	return filepath.Join(cwd, ".rdev", "hosts.json"), nil
}
