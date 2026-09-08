package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const maxMutationIntents = 8192
const maxOwnerMutationIntents = 1024
const maxMutationBytes = 8 << 20

var ErrMutationRecorded = errors.New("mutation identity already recorded; query mutation.status; request was not replayed")
var ErrMutationStorage = errors.New("mutation state durability uncertain; restart required")

// MutationIntent contains identity and outcome metadata only. Request payloads,
// outputs, environment values and approval tokens must never enter this file.
type MutationIntent struct {
	OperationID   string    `json:"operation_id"`
	Owner         string    `json:"owner"`
	Host          string    `json:"host"`
	Operation     string    `json:"operation"`
	RequestDigest string    `json:"request_digest"`
	TargetDigest  string    `json:"target_digest"`
	PolicyDigest  string    `json:"policy_digest"`
	ApprovalID    string    `json:"approval_id"`
	JobID         string    `json:"job_id,omitempty"`
	JobDigest     string    `json:"job_digest,omitempty"`
	State         string    `json:"state"`
	RemoteOK      bool      `json:"remote_ok,omitempty"`
	Updated       time.Time `json:"updated"`
}

type mutationKey struct{ owner, id string }
type mutationSnapshot struct {
	Schema  int              `json:"schema"`
	Records []MutationIntent `json:"records"`
}
type mutationCommitUncertain struct{ cause error }

func (e *mutationCommitUncertain) Error() string { return ErrMutationStorage.Error() }
func (e *mutationCommitUncertain) Unwrap() error { return e.cause }

type MutationRegistry struct {
	updateMu sync.Mutex
	mu       sync.RWMutex
	records  map[mutationKey]MutationIntent
	path     string
	failed   bool
	persist  func(string, []MutationIntent) error
}

func NewMutationRegistry() *MutationRegistry {
	return &MutationRegistry{records: make(map[mutationKey]MutationIntent), persist: saveMutations}
}
func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func validateMutation(m MutationIntent) error {
	client, project, ok := strings.Cut(m.Owner, "\x00")
	if !ok || (Owner{ClientID: client, ProjectID: project}).Validate() != nil || proto.ValidateOperationID(m.OperationID) != nil {
		return errors.New("invalid mutation principal or identity")
	}
	d, ok := proto.LookupOperation(m.Operation)
	if !ok || d.Class != proto.ClassMutating {
		return errors.New("invalid mutation operation")
	}
	if m.Host == "" || len(m.Host) > 512 || strings.ContainsAny(m.Host, "\x00\r\n") || !validDigest(m.RequestDigest) || !validDigest(m.TargetDigest) || !validDigest(m.PolicyDigest) || !validDigest(m.ApprovalID) || m.Updated.IsZero() {
		return errors.New("invalid mutation binding")
	}
	if m.JobID != "" {
		if m.Operation != proto.OpJobStart || !validDigest(m.JobDigest) || validateJobRef(JobRef{ID: m.JobID, Host: m.Host, Owner: m.Owner}) != nil {
			return errors.New("invalid mutation job reference")
		}
		expected, _ := proto.JobIDForOperation(proto.PrincipalID(client, project), m.OperationID)
		if m.JobID != expected {
			return errors.New("mutation job identity binding mismatch")
		}
	} else if m.Operation == proto.OpJobStart || m.JobDigest != "" {
		return errors.New("mutation job identity missing")
	}
	switch m.State {
	case "prepared", "dispatched", "completed", "not_sent", "ambiguous":
	default:
		return errors.New("invalid mutation state")
	}
	if m.RemoteOK && m.State != "completed" {
		return errors.New("nonterminal mutation cannot report remote success")
	}
	return nil
}

func sameMutationBinding(a, b MutationIntent) bool {
	return a.Owner == b.Owner && a.OperationID == b.OperationID && a.Host == b.Host && a.Operation == b.Operation && a.RequestDigest == b.RequestDigest && a.TargetDigest == b.TargetDigest && a.JobID == b.JobID && a.JobDigest == b.JobDigest
}
func sortedMutations(records map[mutationKey]MutationIntent) []MutationIntent {
	out := make([]MutationIntent, 0, len(records))
	for _, m := range records {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Owner != out[j].Owner {
			return out[i].Owner < out[j].Owner
		}
		return out[i].OperationID < out[j].OperationID
	})
	return out
}
func (r *MutationRegistry) update(change func(map[mutationKey]MutationIntent) error) error {
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.mu.RLock()
	if r.failed {
		r.mu.RUnlock()
		return ErrMutationStorage
	}
	next := make(map[mutationKey]MutationIntent, len(r.records))
	for k, m := range r.records {
		next[k] = m
	}
	path := r.path
	r.mu.RUnlock()
	if err := change(next); err != nil {
		return err
	}
	if path != "" {
		if err := r.persist(path, sortedMutations(next)); err != nil {
			var uncertain *mutationCommitUncertain
			if errors.As(err, &uncertain) {
				r.mu.Lock()
				r.failed = true
				r.mu.Unlock()
			}
			return err
		}
	}
	r.mu.Lock()
	r.records = next
	r.mu.Unlock()
	return nil
}
func (r *MutationRegistry) Prepare(m MutationIntent) error {
	m.State = "prepared"
	m.Updated = time.Now().UTC()
	if err := validateMutation(m); err != nil {
		return err
	}
	return r.update(func(next map[mutationKey]MutationIntent) error {
		key := mutationKey{m.Owner, m.OperationID}
		if old, ok := next[key]; ok {
			if !sameMutationBinding(old, m) {
				return proto.NewError(proto.CodeOperationIDConflict, m.OperationID, proto.StateNotSent)
			}
			return ErrMutationRecorded
		}
		count := 0
		for k := range next {
			if k.owner == m.Owner {
				count++
			}
		}
		if len(next) >= maxMutationIntents || count >= maxOwnerMutationIntents {
			return errors.New("mutation identity retention limit reached; no mutation dispatched")
		}
		next[key] = m
		return nil
	})
}
func (r *MutationRegistry) Transition(owner, id, state string, remoteOK bool) error {
	return r.update(func(next map[mutationKey]MutationIntent) error {
		key := mutationKey{owner, id}
		m, ok := next[key]
		if !ok {
			return errors.New("unknown mutation identity")
		}
		allowed := m.State == "prepared" && (state == "dispatched" || state == "not_sent") || m.State == "dispatched" && (state == "completed" || state == "ambiguous" || state == "not_sent") || m.State == "ambiguous" && state == "completed"
		if !allowed {
			return errors.New("invalid mutation transition")
		}
		m.State, m.RemoteOK, m.Updated = state, remoteOK, time.Now().UTC()
		next[key] = m
		return nil
	})
}
func (r *MutationRegistry) Get(owner, id string) (MutationIntent, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.failed {
		return MutationIntent{}, ErrMutationStorage
	}
	m, ok := r.records[mutationKey{owner, id}]
	if !ok {
		return MutationIntent{}, errors.New("mutation unknown for principal")
	}
	return m, nil
}
func (r *MutationRegistry) Snapshot() []MutationIntent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sortedMutations(r.records)
}

func saveMutations(path string, records []MutationIntent) error {
	data, err := json.Marshal(mutationSnapshot{Schema: 1, Records: records})
	if err != nil {
		return err
	}
	if len(data) > maxMutationBytes {
		return errors.New("mutation snapshot exceeds size limit")
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".rdev-mutations-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return &mutationCommitUncertain{err}
	}
	err = d.Sync()
	closeErr := d.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return &mutationCommitUncertain{err}
	}
	return nil
}

// rejectDuplicateJSON checks the whole tree before typed decoding. JSON null
// and case-folded duplicate keys cannot silently select a different identity.
func rejectDuplicateJSON(dec *json.Decoder) error {
	return rejectDuplicateJSONDepth(dec, 0)
}

func rejectDuplicateJSONDepth(dec *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("state nesting exceeds limit")
	}
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t == nil {
		return errors.New("null state is invalid")
	}
	d, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch d {
	case '{':
		seen := make(map[string]bool)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return err
			}
			s, ok := key.(string)
			if !ok {
				return errors.New("invalid state key")
			}
			s = strings.ToLower(s)
			if seen[s] {
				return errors.New("duplicate state key")
			}
			seen[s] = true
			if err := rejectDuplicateJSONDepth(dec, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := rejectDuplicateJSONDepth(dec, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid state delimiter")
	}
	_, err = dec.Token()
	return err
}
func (r *MutationRegistry) ConfigurePersistence(path string) error {
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	data, err := ReadPrivateFile(path, maxMutationBytes)
	next := make(map[mutationKey]MutationIntent)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		dec := json.NewDecoder(bytes.NewReader(data))
		if err := rejectDuplicateJSON(dec); err != nil {
			return err
		}
		if _, err := dec.Token(); err != io.EOF {
			return errors.New("mutation state requires one document")
		}
		var snapshot mutationSnapshot
		dec = json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&snapshot); err != nil {
			return err
		}
		if snapshot.Schema != 1 || snapshot.Records == nil || len(snapshot.Records) > maxMutationIntents {
			return errors.New("invalid mutation snapshot schema or size")
		}
		owners := make(map[string]int)
		for _, m := range snapshot.Records {
			if err := validateMutation(m); err != nil {
				return err
			}
			key := mutationKey{m.Owner, m.OperationID}
			if _, ok := next[key]; ok {
				return errors.New("duplicate mutation identity")
			}
			owners[m.Owner]++
			if owners[m.Owner] > maxOwnerMutationIntents {
				return errors.New("mutation owner retention limit exceeded")
			}
			// No process survives broker restart to finish a prepared dispatch.
			if m.State == "prepared" {
				m.State = "not_sent"
			}
			if m.State == "dispatched" {
				m.State = "ambiguous"
			}
			next[key] = m
		}
	}
	if err := r.persist(path, sortedMutations(next)); err != nil {
		return err
	}
	r.mu.Lock()
	r.records, r.path, r.failed = next, path, false
	r.mu.Unlock()
	return nil
}
