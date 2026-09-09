package broker

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
)

const maxSecretValue = 64 << 10
const maxSecretState = 20 << 20
const maxSecretVersions = 4096
const maxOwnerSecretVersions = 256
const maxSecretBytes = 16 << 20
const maxOwnerSecretBytes = 1 << 20

var errSecretUnavailable = errors.New("secret unavailable for this principal and host")
var errSecretStorage = errors.New("secret storage unavailable; restart required")

type SecretParams struct {
	Path  string `json:"path,omitempty"`
	Name  string `json:"name,omitempty"`
	Value string `json:"value,omitempty"`
}

type SecretDescriptor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type secretRecord struct {
	Owner   string `json:"owner"`
	Host    string `json:"host"`
	Target  string `json:"target"`
	Name    string `json:"name"`
	Version string `json:"version"`
	Value   string `json:"value"`
	Active  bool   `json:"active"`
}
type secretState struct {
	Schema    int            `json:"schema"`
	DigestKey string         `json:"digest_key"`
	Records   []secretRecord `json:"records"`
}

// SecretRegistry keeps exact principal/host credentials and all accepted prior
// versions. Deletion retires injection authority, not output protection: detached
// jobs can still emit a previous value after rotation, removal or daemon restart.
// Bounds reject new versions instead of silently evicting a live redactor.
type SecretRegistry struct {
	updateMu sync.Mutex
	mu       sync.RWMutex
	state    secretState
	path     string
	failed   bool
	redactor *secrets.Store
	persist  func(string, secretState) error
}

func NewSecretRegistry(redactor *secrets.Store) *SecretRegistry {
	return &SecretRegistry{redactor: redactor, state: secretState{Schema: SecretSchemaVersion, Records: []secretRecord{}}, persist: saveSecretState}
}
func validSecretName(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}
func validateSecretParams(op string, p *SecretParams) error {
	if op != "secret.set" && op != "secret.delete" && op != "secret.list" && op != "secret.set_from_file" {
		return errors.New("unsupported secret operation")
	}
	if p == nil {
		return errors.New("secret parameters required")
	}
	if op == "secret.list" {
		if p.Name != "" || p.Value != "" || p.Path != "" {
			return errors.New("secret list does not accept a name or value")
		}
		return nil
	}
	if !validSecretName(p.Name) {
		return errors.New("invalid secret name")
	}
	if op == "secret.set_from_file" {
		if len(p.Path) == 0 || len(p.Path) > 4096 || strings.ContainsRune(p.Path, 0) {
			return errors.New("invalid remote secret path")
		}
	} else if p.Path != "" {
		return errors.New("unexpected secret source path")
	}
	if op == "secret.set" || op == "secret.set_from_file" {
		if len(p.Value) < secrets.MinValueBytes || len(p.Value) > maxSecretValue || strings.ContainsRune(p.Value, 0) || !utf8.ValidString(p.Value) {
			return errors.New("secret value must contain 6 to 65536 bytes without NUL")
		}
	} else if p.Value != "" {
		return errors.New("secret delete does not accept a value")
	}
	return nil
}
func validateSecretState(st secretState) error {
	if st.Schema != SecretSchemaVersion || !validDigest(st.DigestKey) || st.Records == nil || len(st.Records) > maxSecretVersions {
		return errSecretStorage
	}
	versions := map[string]bool{}
	active := map[[4]string]bool{}
	counts := map[string]int{}
	sizes := map[string]int{}
	total := 0
	encodedBytes := len(`{"schema":1,"digest_key":"","records":[]}`) + len(st.DigestKey) + 1
	if len(st.Records) > 0 {
		encodedBytes += len(st.Records) - 1
	}
	for _, r := range st.Records {
		c, p := splitOwner(r.Owner)
		if (Owner{ClientID: c, ProjectID: p}).Validate() != nil || r.Host == "" || len(r.Host) > 512 || strings.ContainsAny(r.Host, "\x00\r\n") || !validDigest(r.Target) || !validDigest(r.Version) || !validSecretName(r.Name) || len(r.Value) < secrets.MinValueBytes || len(r.Value) > maxSecretValue || strings.ContainsRune(r.Value, 0) || !utf8.ValidString(r.Value) || versions[r.Version] {
			return errSecretStorage
		}
		encoded, err := json.Marshal(r)
		if err != nil {
			return errSecretStorage
		}
		encodedBytes += len(encoded)
		if encodedBytes > maxSecretState {
			return errors.New("secret retention capacity reached")
		}
		versions[r.Version] = true
		if r.Active {
			k := [4]string{r.Owner, r.Host, r.Target, r.Name}
			if active[k] {
				return errSecretStorage
			}
			active[k] = true
		}
		counts[r.Owner]++
		sizes[r.Owner] += len(r.Value)
		total += len(r.Value)
		if counts[r.Owner] > maxOwnerSecretVersions || sizes[r.Owner] > maxOwnerSecretBytes || total > maxSecretBytes {
			return errors.New("secret retention capacity reached")
		}
	}
	return nil
}
func (r *SecretRegistry) ConfigurePersistence(path string) error {
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	st := secretState{Schema: SecretSchemaVersion, Records: []secretRecord{}}
	data, err := ReadPrivateFile(path, maxSecretState)
	if err != nil && !os.IsNotExist(err) {
		return errSecretStorage
	}
	if err == nil {
		d := json.NewDecoder(bytes.NewReader(data))
		if rejectDuplicateJSON(d) != nil {
			return errSecretStorage
		}
		if _, err := d.Token(); err != io.EOF {
			return errSecretStorage
		}
		d = json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if d.Decode(&st) != nil {
			return errSecretStorage
		}
	} else {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return errSecretStorage
		}
		st.DigestKey = hex.EncodeToString(key)
	}
	if err := validateSecretState(st); err != nil {
		return err
	}
	if err := r.persist(path, st); err != nil {
		return errSecretStorage
	}
	for _, item := range st.Records {
		if err := r.protect(item); err != nil {
			return errSecretStorage
		}
	}
	r.state, r.path, r.failed = st, path, false
	return nil
}
func (r *SecretRegistry) protect(item secretRecord) error {
	// Only a random version appears in replacement markers, never another
	// principal's secret name or identity. This output key is never injectable.
	return r.redactor.Set(secrets.OutputKey("broker_"+item.Version), item.Value)
}
func (r *SecretRegistry) current(owner, host, target, name string) (secretRecord, bool) {
	for i := len(r.state.Records) - 1; i >= 0; i-- {
		s := r.state.Records[i]
		if s.Active && s.Owner == owner && s.Host == host && s.Target == target && s.Name == name {
			return s, true
		}
	}
	return secretRecord{}, false
}
func (r *SecretRegistry) List(owner, host, target string) ([]SecretDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.failed {
		return nil, errSecretStorage
	}
	out := []SecretDescriptor{}
	for _, s := range r.state.Records {
		if s.Active && s.Owner == owner && s.Host == host && s.Target == target {
			out = append(out, SecretDescriptor{s.Name, s.Version})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (r *SecretRegistry) digestLocked(v any) (string, error) {
	if r.failed || !validDigest(r.state.DigestKey) {
		return "", errSecretStorage
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", errSecretStorage
	}
	key, _ := hex.DecodeString(r.state.DigestKey)
	m := hmac.New(sha256.New, key)
	m.Write(b)
	return hex.EncodeToString(m.Sum(nil)), nil
}
func (r *SecretRegistry) mutationDigestLocked(owner, host, target, operation string, p *SecretParams) (string, error) {
	old, _ := r.current(owner, host, target, p.Name)
	return r.digestLocked([]any{owner, host, target, operation, p, old.Version})
}
func (r *SecretRegistry) Plan(owner, host, target, operation string, p *SecretParams) (string, error) {
	if err := validateSecretParams(operation, p); err != nil {
		return "", err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.mutationDigestLocked(owner, host, target, operation, p)
}
func (r *SecretRegistry) Apply(owner, host, target, operation string, p *SecretParams, digest string) error {
	if err := validateSecretParams(operation, p); err != nil {
		return err
	}
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.mu.RLock()
	current, err := r.mutationDigestLocked(owner, host, target, operation, p)
	if err != nil {
		r.mu.RUnlock()
		return err
	}
	if current != digest {
		r.mu.RUnlock()
		return errors.New("approved secret version changed")
	}
	next := r.state
	next.Records = append([]secretRecord{}, r.state.Records...)
	path := r.path
	r.mu.RUnlock()
	for i, s := range next.Records {
		if s.Active && s.Owner == owner && s.Host == host && s.Name == p.Name {
			next.Records[i].Active = false
		}
	}
	if operation == "secret.set" || operation == "secret.set_from_file" {
		version := make([]byte, 32)
		if _, err := rand.Read(version); err != nil {
			return errSecretStorage
		}
		next.Records = append(next.Records, secretRecord{Owner: owner, Host: host, Target: target, Name: p.Name, Value: p.Value, Version: hex.EncodeToString(version), Active: true})
	}
	if err := validateSecretState(next); err != nil {
		return err
	}
	fail := func() error { r.mu.Lock(); r.failed = true; r.mu.Unlock(); return errSecretStorage }
	// Readers keep the old committed version while a storage write is pending.
	// An uncertain commit disables further resolution until restart.
	if err := r.persist(path, next); err != nil {
		return fail()
	}
	if operation == "secret.set" || operation == "secret.set_from_file" {
		if err := r.protect(next.Records[len(next.Records)-1]); err != nil {
			return fail()
		}
	}
	r.mu.Lock()
	r.state = next
	r.mu.Unlock()
	return nil
}

// Resolve copies the request and freezes only this owner's exact-host values.
// The HMAC binds the submitted reference request and random version IDs without
// publishing a dictionary-testable hash of a low-entropy credential.
func (r *SecretRegistry) Resolve(owner, host, target string, wire *proto.Request) (*proto.Request, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	copy := *wire
	revisions := map[string]string{}
	expandedBytes := 0
	expand := func(p *proto.ExecParams) (*proto.ExecParams, error) {
		out := *p
		out.Env = make(map[string]string, len(p.Env))
		for k, v := range p.Env {
			if name, ok := strings.CutPrefix(v, "secret:"); ok {
				value, found := r.current(owner, host, target, name)
				if !found || r.failed {
					return nil, errSecretUnavailable
				}
				expandedBytes += len(value.Value)
				if expandedBytes > 1<<20 {
					return nil, errors.New("expanded secret request exceeds limit")
				}
				revisions[name] = value.Version
				out.Env[k] = value.Value
			} else {
				out.Env[k] = v
			}
		}
		return &out, nil
	}
	var err error
	if copy.Exec != nil {
		copy.Exec, err = expand(copy.Exec)
		if err != nil {
			return nil, "", err
		}
	}
	if copy.Job != nil && copy.Job.Spec != nil {
		j := *copy.Job
		j.Spec, err = expand(j.Spec)
		if err != nil {
			return nil, "", err
		}
		copy.Job = &j
	}
	if len(revisions) == 0 {
		return &copy, "", nil
	}
	encoded, err := json.Marshal(&copy)
	if err != nil || len(encoded) > 1<<20 {
		return nil, "", errors.New("expanded secret request exceeds limit")
	}
	canonical := *wire
	canonical.ID, canonical.OperationID = "", ""
	canonical.Replay = false
	canonical.StreamWindowBytes = 0
	digest, err := r.digestLocked([]any{owner, host, target, &canonical, revisions})
	return &copy, digest, err
}
func wireUsesSecrets(w *proto.Request) bool {
	if w == nil {
		return false
	}
	has := func(p *proto.ExecParams) bool {
		if p != nil {
			for _, v := range p.Env {
				if strings.HasPrefix(v, "secret:") {
					return true
				}
			}
		}
		return false
	}
	return has(w.Exec) || (w.Job != nil && has(w.Job.Spec))
}

func saveSecretState(path string, st secretState) error {
	if path == "" {
		return errors.New("secret persistence is required")
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".secrets-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		err = json.NewEncoder(f).Encode(st)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	info, err := os.Lstat(f.Name())
	if err != nil {
		return err
	}
	if info.Size() > maxSecretState {
		return errors.New("secret snapshot exceeds size limit")
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Retire bindings that no longer match the startup registry before READY. A
// later return to an earlier host definition cannot reactivate their authority.
func (s *Service) ConfigureSecrets(path string) error {
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		for _, intent := range s.Mutations.Snapshot() {
			if isSecretMutation(intent.Operation) {
				return errSecretStorage
			}
		}
	}
	if err := s.Secrets.ConfigurePersistence(path); err != nil {
		return err
	}
	r := s.Secrets
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.state
	next.Records = append([]secretRecord{}, r.state.Records...)
	changed := false
	for i, item := range next.Records {
		if item.Active {
			target, err := s.client.ProtocolTargetIdentity(item.Host)
			if err != nil || target != item.Target {
				next.Records[i].Active = false
				changed = true
			}
		}
	}
	if changed {
		if err := r.persist(path, next); err != nil {
			r.failed = true
			return errSecretStorage
		}
		r.state = next
	}
	return nil
}
