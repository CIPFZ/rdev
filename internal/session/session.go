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
	"github.com/CIPFZ/rdev/internal/secrets"
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

// HostUpdate is one atomic patch to a registry host. Front ends populate the
// fields they received and call ApplyHostUpdate once; validation, optional
// persistence, live publication, generation advancement, and invalidation all
// happen at this boundary.
type HostUpdate struct {
	Name string
	// Host replaces the complete transport definition when non-nil. Its Name
	// must either be empty or match Name.
	Host *transport.Host
	// RemoteDir patches only the state directory while retaining the existing
	// destination. It is ignored when Host is supplied.
	RemoteDir *string

	Scope        Scope
	SetScope     bool
	DefaultScope Scope

	Cwd        *string
	Env        map[string]string
	LoginShell *bool
	Secrets    map[string]string
	Persist    bool
}

// HostUpdateResult describes the single committed registry publication.
type HostUpdateResult struct {
	Scope      Scope
	Generation uint64
	SavedTo    string
}

// HostSnapshot is an atomic host/scope/state observation.
type HostSnapshot struct {
	ResolvedHost
	State State
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
	leaseMu    sync.Mutex
	identity   map[string]*sync.RWMutex
	approvalMu sync.Mutex
	tx         sync.Mutex
	mu         sync.RWMutex
	hosts      map[string]transport.Host
	state      map[string]*State
	scopes     map[string]Scope
	// persistence is the per-alias scope migration state machine. It keeps the
	// durable locations and an in-progress live-only move in one value so every
	// load, rewrite, and live scope transition is published together.
	persistence  map[string]scopePersistence
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
		persistence: make(map[string]scopePersistence),
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
	Host                  transport.Host
	Scope                 Scope
	Fingerprint           string
	ConnectionFingerprint string
	Generation            uint64
}

func hostFingerprint(h transport.Host, scope Scope) string {
	remoteDir, _ := transport.ValidateRemoteDir(h.RemoteDir)
	// ForceAgentUpload controls the next bootstrap but does not change the
	// credential identity. Treating it as identity would purge exact scoped
	// secrets even though address, user, port, namespace, and scope were stable.
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s", h.Addr, h.Port, remoteDir, scope)))
	return fmt.Sprintf("%x", sum[:])
}

func connectionFingerprint(h transport.Host, scope Scope) string {
	remoteDir, _ := transport.ValidateRemoteDir(h.RemoteDir)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s\x00%t\x00%s", h.Addr, h.Port, remoteDir, h.ForceAgentUpload, scope)))
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

func (r *Registry) RecordSecretLoadFailure(reason observe.SecretReason, target string) {
	r.mu.RLock()
	registry := r.observe
	r.mu.RUnlock()
	registry.SecretLoadFailed(reason, target)
}

func (r *Registry) RecordSecretRejection(reason observe.SecretReason, target string) {
	r.mu.RLock()
	registry := r.observe
	r.mu.RUnlock()
	registry.SecretRejected(reason, target)
}

func (r *Registry) RecordConnectionSecurityState(state observe.ConnectionSecurityState, target string) {
	r.mu.RLock()
	registry := r.observe
	r.mu.RUnlock()
	registry.ConnectionSecurityStateChanged(state, target)
}

func (r *Registry) RecordRedactionHit(count uint64) {
	r.mu.RLock()
	registry := r.observe
	r.mu.RUnlock()
	registry.RedactionHit(count)
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
	persistence map[string]scopePersistence
	generations map[string]uint64
	nextGen     uint64
	trust       ProjectTrust
}

// scopePersistence is an explicit two-state migration ledger:
//
//   - stable: movingFrom is empty; persisted is the exact set of files that
//     contain the alias.
//   - live-moved: movingFrom names a still-persisted source while scopes holds
//     the unsaved destination. Persisting that destination is rejected until
//     an authoritative rewrite removes the source.
//
// Rewriting the source transitions live-moved back to stable. A later reverse
// move therefore starts a new migration instead of being mistaken for a
// cancellation of stale state from the previous completed migration.
type scopePersistence struct {
	persisted  uint8
	movingFrom Scope
}

func (p scopePersistence) load(scope Scope) scopePersistence {
	p.persisted |= scopeBit(scope)
	p.movingFrom = ""
	return p
}

func (p scopePersistence) move(from, to Scope) scopePersistence {
	if from == to {
		return p
	}
	if p.movingFrom == "" {
		p.movingFrom = from
	} else if to == p.movingFrom {
		p.movingFrom = ""
	}
	return p.normalize()
}

func (p scopePersistence) rewrite(scope, liveScope Scope) scopePersistence {
	p.persisted &^= scopeBit(scope)
	if liveScope == scope {
		p.persisted |= scopeBit(scope)
	}
	return p.normalize()
}

func (p scopePersistence) normalize() scopePersistence {
	if p.movingFrom != "" && p.persisted&scopeBit(p.movingFrom) == 0 {
		p.movingFrom = ""
	}
	return p
}

func (p scopePersistence) empty() bool {
	return p.persisted == 0 && p.movingFrom == ""
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

func cloneStateValue(st *State) State {
	return *cloneState(st)
}

func (r *Registry) snapshotLocked() registrySnapshot {
	s := registrySnapshot{
		hosts:       make(map[string]transport.Host, len(r.hosts)),
		state:       make(map[string]*State, len(r.state)),
		scopes:      make(map[string]Scope, len(r.scopes)),
		persistence: make(map[string]scopePersistence, len(r.persistence)),
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
	for k, v := range r.persistence {
		s.persistence[k] = v
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
		oldScope := s.scopes[h.Name]
		oldState := s.state[h.Name]
		declarationsChanged := oldState == nil || !equalStringMap(oldState.Secrets, e.Secrets)
		securityChanged := !exists || hostFingerprint(old, oldScope) != hostFingerprint(h, scope) || declarationsChanged
		connectionChanged := !exists || connectionFingerprint(old, oldScope) != connectionFingerprint(h, scope)
		if securityChanged {
			s.nextGen++
			s.generations[h.Name] = s.nextGen
		}
		if securityChanged || connectionChanged {
			changed = append(changed, h.Name)
		}
		s.hosts[h.Name] = h
		s.scopes[h.Name] = scope
		s.persistence[h.Name] = s.persistence[h.Name].load(scope)
		// A config entry is an authoritative snapshot, not a patch. Reusing the
		// prior map would retain env or secret declarations removed from disk.
		// Identity changes additionally advance the generation above.
		st := &State{LoginShell: true}
		s.state[h.Name] = st
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
	r.persistence = s.persistence
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
		if err := secrets.ValidateDeclarations(e.Secrets); err != nil {
			r.reject(observe.ReasonConfigInvalid, path)
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
	if !validScope(scope) {
		return fmt.Errorf("scope must be project or global, got %q", scope)
	}
	r.tx.Lock()
	defer r.tx.Unlock()
	r.mu.RLock()
	s := r.snapshotLocked()
	r.mu.RUnlock()
	if err := rejectImplicitScopeMigration(s, scope); err != nil {
		return err
	}
	path, b, err := r.marshalScopeSnapshot(s, scope)
	if err != nil {
		return err
	}
	// Written 0600: it records hostnames and usernames, not secrets, but there
	// is no reason to make it world-readable.
	err = r.io.write(path, b)
	if err != nil && !configWriteCommitted(err) {
		r.stopOnAmbiguousWrite(err)
		return err
	}
	r.mu.Lock()
	recordScopeRewrite(&s, scope)
	r.persistence = s.persistence
	r.mu.Unlock()
	return err
}

func validScope(scope Scope) bool {
	return scope == ScopeGlobal || scope == ScopeProject
}

func scopeBit(scope Scope) uint8 {
	if scope == ScopeGlobal {
		return 1
	}
	if scope == ScopeProject {
		return 2
	}
	return 0
}

// rejectImplicitScopeMigration prevents a single scope rewrite from silently
// becoming half of a two-file migration. The storage layer provides atomic
// replacement for one hosts.json, but has no crash-recovery journal spanning
// the global and project files. Callers must first Save the old scope (which
// removes the live-moved alias there), then retry the destination write.
func rejectImplicitScopeMigration(s registrySnapshot, scope Scope) error {
	for name := range s.hosts {
		state := s.persistence[name]
		if s.scopes[name] == scope && state.movingFrom != "" {
			from := state.movingFrom
			return fmt.Errorf("cannot persist host %q from %s to %s atomically: registry storage has no crash-consistent cross-file transaction; explicitly save/remove it from the %s scope first, then persist the %s scope", name, from, scope, from, scope)
		}
	}
	return nil
}

// recordScopeRewrite updates durable-location bookkeeping after a committed
// authoritative rewrite of one config file.
func recordScopeRewrite(s *registrySnapshot, scope Scope) {
	for name, state := range s.persistence {
		state = state.rewrite(scope, s.scopes[name])
		if state.empty() {
			delete(s.persistence, name)
		} else {
			s.persistence[name] = state
		}
	}
	for name := range s.hosts {
		if s.scopes[name] == scope {
			state := s.persistence[name].rewrite(scope, scope)
			s.persistence[name] = state
		}
	}
}

func configPathForScope(scope Scope) (string, error) {
	if scope == ScopeProject {
		return ProjectConfigPath()
	}
	return ConfigPath()
}

func (r *Registry) marshalScopeSnapshot(s registrySnapshot, scope Scope) (string, []byte, error) {
	path, err := configPathForScope(scope)
	if err != nil {
		return "", nil, err
	}
	hf := hostFile{}
	for name, h := range s.hosts {
		if s.scopes[name] != scope {
			continue
		}
		e := hostEntry{
			Name: name, Addr: h.Addr, Port: h.Port, RemoteDir: h.RemoteDir,
			ForceAgentUpload: h.ForceAgentUpload,
		}
		if st, ok := s.state[name]; ok {
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
	sort.Slice(hf.Hosts, func(i, j int) bool { return hf.Hosts[i].Name < hf.Hosts[j].Name })
	b, err := r.io.marshal(hf, "", "  ")
	if err != nil {
		return "", nil, err
	}
	return path, append(b, '\n'), nil
}

// ApplyHostUpdate validates and stages a complete host patch, optionally writes
// that staged snapshot durably, and only then publishes it to readers. No live
// registry state or invalidation hook changes on a pre-commit failure.
func (r *Registry) ApplyHostUpdate(update HostUpdate) (HostUpdateResult, error) {
	if err := r.fatalError(); err != nil {
		return HostUpdateResult{}, err
	}
	if strings.TrimSpace(update.Name) == "" {
		return HostUpdateResult{}, errors.New("host name required")
	}
	if update.SetScope && !validScope(update.Scope) {
		return HostUpdateResult{}, fmt.Errorf("scope must be project or global, got %q", update.Scope)
	}
	if update.DefaultScope != "" && !validScope(update.DefaultScope) {
		return HostUpdateResult{}, fmt.Errorf("default scope must be project or global, got %q", update.DefaultScope)
	}
	if err := secrets.ValidateDeclarations(update.Secrets); err != nil {
		return HostUpdateResult{}, err
	}
	var replacement *transport.Host
	if update.Host != nil {
		h := *update.Host
		if h.Name == "" {
			h.Name = update.Name
		}
		if h.Name != update.Name {
			return HostUpdateResult{}, fmt.Errorf("host update name %q does not match transport name %q", update.Name, h.Name)
		}
		if err := transport.ValidateHost(h); err != nil {
			return HostUpdateResult{}, fmt.Errorf("invalid host: %w", err)
		}
		replacement = &h
	} else if update.RemoteDir != nil {
		if _, err := transport.ValidateRemoteDir(*update.RemoteDir); err != nil {
			return HostUpdateResult{}, fmt.Errorf("invalid host: %w", err)
		}
	}

	releaseIdentity := r.lockIdentityWrites([]string{update.Name})
	defer releaseIdentity()
	r.tx.Lock()
	txLocked := true
	defer func() {
		if txLocked {
			r.tx.Unlock()
		}
	}()
	if err := r.fatalError(); err != nil {
		return HostUpdateResult{}, err
	}

	r.mu.RLock()
	staged := r.snapshotLocked()
	r.mu.RUnlock()
	oldHost, exists := staged.hosts[update.Name]
	oldScope := staged.scopes[update.Name]
	oldState := staged.state[update.Name]
	if !exists && replacement == nil {
		return HostUpdateResult{}, fmt.Errorf("unknown host %q", update.Name)
	}

	newHost := oldHost
	if replacement != nil {
		newHost = *replacement
	} else if update.RemoteDir != nil {
		newHost.RemoteDir = *update.RemoteDir
	}
	if err := transport.ValidateHost(newHost); err != nil {
		return HostUpdateResult{}, fmt.Errorf("invalid host: %w", err)
	}

	newScope := oldScope
	if !exists {
		newScope = update.DefaultScope
		if newScope == "" {
			newScope = ScopeGlobal
		}
	}
	if update.SetScope {
		newScope = update.Scope
	}
	if !validScope(newScope) {
		return HostUpdateResult{}, fmt.Errorf("scope must be project or global, got %q", newScope)
	}

	identityChanged := !exists || hostFingerprint(oldHost, oldScope) != hostFingerprint(newHost, newScope)
	newState := cloneState(oldState)
	if identityChanged {
		newState = &State{LoginShell: true}
	}
	if update.Cwd != nil {
		newState.Cwd = *update.Cwd
	}
	if len(update.Env) > 0 {
		newState.Env = MergeEnv(newState.Env, update.Env)
	}
	if update.LoginShell != nil {
		newState.LoginShell = *update.LoginShell
	}
	if len(update.Secrets) > 0 {
		newState.Secrets = MergeEnv(newState.Secrets, update.Secrets)
	}
	if err := secrets.ValidateDeclarations(newState.Secrets); err != nil {
		return HostUpdateResult{}, err
	}

	declarationsChanged := !equalStringMap(cloneState(oldState).Secrets, newState.Secrets)
	securityChanged := identityChanged || declarationsChanged
	connectionChanged := !exists || connectionFingerprint(oldHost, oldScope) != connectionFingerprint(newHost, newScope)
	staged.hosts[update.Name] = newHost
	staged.scopes[update.Name] = newScope
	if exists && oldScope != newScope {
		state := staged.persistence[update.Name].move(oldScope, newScope)
		if state.empty() {
			delete(staged.persistence, update.Name)
		} else {
			staged.persistence[update.Name] = state
		}
	}
	staged.state[update.Name] = newState
	generation := staged.generations[update.Name]
	if securityChanged {
		staged.nextGen++
		generation = staged.nextGen
		staged.generations[update.Name] = generation
	}

	result := HostUpdateResult{Scope: newScope, Generation: generation}
	var writeErr error
	if update.Persist {
		if err := rejectImplicitScopeMigration(staged, newScope); err != nil {
			return HostUpdateResult{}, err
		}
		path, data, err := r.marshalScopeSnapshot(staged, newScope)
		if err != nil {
			return HostUpdateResult{}, err
		}
		writeErr = r.io.write(path, data)
		if writeErr != nil && !configWriteCommitted(writeErr) {
			r.stopOnAmbiguousWrite(writeErr)
			return HostUpdateResult{}, writeErr
		}
		recordScopeRewrite(&staged, newScope)
		result.SavedTo = path
	}

	r.mu.Lock()
	hook, _ := r.publishLocked(staged)
	r.mu.Unlock()
	r.tx.Unlock()
	txLocked = false
	if (securityChanged || connectionChanged) && hook != nil {
		hook(update.Name, generation)
	}
	return result, writeErr
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

// SetScope records which config file a host belongs to. Scope is part of the
// immutable secret identity, so changing it advances the generation, clears
// sticky state, and invalidates the old connection and credentials.
func (r *Registry) SetScope(name string, scope Scope) {
	releaseIdentity := r.lockIdentityWrites([]string{name})
	defer releaseIdentity()
	r.tx.Lock()
	r.mu.Lock()
	old := r.scopes[name]
	if old == scope {
		r.mu.Unlock()
		r.tx.Unlock()
		return
	}
	r.scopes[name] = scope
	if _, exists := r.hosts[name]; !exists {
		r.mu.Unlock()
		r.tx.Unlock()
		return
	}
	state := r.persistence[name].move(old, scope)
	if state.empty() {
		delete(r.persistence, name)
	} else {
		r.persistence[name] = state
	}
	r.state[name] = &State{LoginShell: true}
	r.nextGen++
	generation := r.nextGen
	r.generations[name] = generation
	hook := r.onHostChange
	r.mu.Unlock()
	r.tx.Unlock()
	if hook != nil {
		hook(name, generation)
	}
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
	scope := r.scopes[h.Name]
	if scope == "" {
		scope = ScopeGlobal
		r.scopes[h.Name] = scope
	}
	r.hosts[h.Name] = h
	changed := !exists || hostFingerprint(old, scope) != hostFingerprint(h, scope)
	connectionChanged := !exists || connectionFingerprint(old, scope) != connectionFingerprint(h, scope)
	credentialsChanged := !exists || hostFingerprint(old, scope) != hostFingerprint(h, scope)
	if _, ok := r.state[h.Name]; !ok || credentialsChanged {
		r.state[h.Name] = &State{LoginShell: true}
	}
	generation := r.generations[h.Name]
	if changed {
		r.nextGen++
		generation = r.nextGen
		r.generations[h.Name] = generation
	}
	hook := r.onHostChange
	r.mu.Unlock()
	r.tx.Unlock()
	if (changed || connectionChanged) && hook != nil {
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
	scope := r.scopes[name]
	r.mu.RUnlock()
	if ok {
		return ResolvedHost{Host: h, Scope: scope, Fingerprint: hostFingerprint(h, scope), ConnectionFingerprint: connectionFingerprint(h, scope), Generation: generation}, nil
	}

	if parsed, err := parseDestination(name); err == nil {
		if err := r.Add(parsed); err != nil {
			return ResolvedHost{}, err
		}
		r.mu.RLock()
		h, generation, scope = r.hosts[parsed.Name], r.generations[parsed.Name], r.scopes[parsed.Name]
		r.mu.RUnlock()
		return ResolvedHost{Host: h, Scope: scope, Fingerprint: hostFingerprint(h, scope), ConnectionFingerprint: connectionFingerprint(h, scope), Generation: generation}, nil
	}
	return ResolvedHost{}, fmt.Errorf("unknown host %q (known: %s)", name, strings.Join(r.Names(), ", "))
}

// Inspect returns host identity, scope, generation, and sticky state from one
// read lock, so status/list callers cannot combine fields from two publications.
// Unlike Resolve it never auto-registers ssh-shaped names.
func (r *Registry) Inspect(name string) (HostSnapshot, error) {
	if err := r.fatalError(); err != nil {
		return HostSnapshot{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.hosts[name]
	if !ok {
		return HostSnapshot{}, fmt.Errorf("unknown host %q", name)
	}
	scope := r.scopes[name]
	return HostSnapshot{
		ResolvedHost: ResolvedHost{
			Host: h, Scope: scope, Fingerprint: hostFingerprint(h, scope),
			ConnectionFingerprint: connectionFingerprint(h, scope), Generation: r.generations[name],
		},
		State: cloneStateValue(r.state[name]),
	}, nil
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
	scope := r.scopes[name]
	if !ok || r.generations[name] != generation || hostFingerprint(h, scope) != fingerprint {
		r.mu.RUnlock()
		lease.RUnlock()
		return nil, false
	}
	r.mu.RUnlock()
	return lease.RUnlock, true
}

// AcquireIdentityWrite excludes every operation using this alias while a
// host-scoped secret is replaced or deleted. Without it, rotating a value could
// remove the old redactor before an in-flight response is scrubbed.
func (r *Registry) AcquireIdentityWrite(name string, generation uint64, fingerprint string) (func(), bool) {
	if r.fatalError() != nil {
		return nil, false
	}
	lease := r.identityLease(name)
	lease.Lock()
	r.mu.RLock()
	h, ok := r.hosts[name]
	scope := r.scopes[name]
	if !ok || r.generations[name] != generation || hostFingerprint(h, scope) != fingerprint {
		r.mu.RUnlock()
		lease.Unlock()
		return nil, false
	}
	r.mu.RUnlock()
	return lease.Unlock, true
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

// Update mutates a host's state under lock. Secret declaration changes advance
// the host generation so a warm connection cannot continue without loading the
// new complete declaration set first.
func (r *Registry) Update(name string, fn func(*State)) {
	releaseIdentity := r.lockIdentityWrites([]string{name})
	defer releaseIdentity()
	r.tx.Lock()
	r.mu.Lock()
	st, ok := r.state[name]
	if !ok {
		st = &State{LoginShell: true}
		r.state[name] = st
	}
	beforeSecrets := MergeEnv(nil, st.Secrets)
	fn(st)
	changed := !equalStringMap(beforeSecrets, st.Secrets)
	var generation uint64
	if changed {
		r.nextGen++
		generation = r.nextGen
		r.generations[name] = generation
	}
	hook := r.onHostChange
	r.mu.Unlock()
	r.tx.Unlock()
	if changed && hook != nil {
		hook(name, generation)
	}
}

func equalStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
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
