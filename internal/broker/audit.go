package broker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

type AuditEvent struct {
	RequestDigest string    `json:"request_digest,omitempty"`
	TargetDigest  string    `json:"target_digest,omitempty"`
	ApprovalID    string    `json:"approval_id,omitempty"`
	PolicyDigest  string    `json:"policy_digest,omitempty"`
	Schema        int       `json:"schema,omitempty"`
	At            time.Time `json:"at"`
	Owner         string    `json:"owner,omitempty"`
	Operation     string    `json:"operation,omitempty"`
	Decision      string    `json:"decision,omitempty"`
	Result        string    `json:"result,omitempty"`
}
type AuditLog struct {
	mu       sync.RWMutex
	max      int
	events   []AuditEvent
	file     *os.File
	path     string
	maxBytes int64
}

func NewAuditLog(max int) *AuditLog {
	if max < 1 {
		max = 256
	}
	return &AuditLog{max: max}
}

func (a *AuditLog) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	return err
}

// ConfigureFile enables append-only JSONL persistence with bounded rotation.
func (a *AuditLog) ConfigureFile(path string, maxBytes int64) error {
	if path == "" || maxBytes < 1 {
		return os.ErrInvalid
	}
	// Restore the rotated segment before the active segment so a broker restart
	// can answer queries spanning the rotation boundary.
	rotated, _ := os.ReadFile(path + ".1")
	prior, _ := os.ReadFile(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	loaded := make([]AuditEvent, 0)
	for _, line := range bytes.Split(append(append([]byte{}, rotated...), prior...), []byte{'\n'}) {
		var event AuditEvent
		if len(line) > 0 && json.Unmarshal(line, &event) == nil {
			loaded = append(loaded, event)
		}
	}
	a.mu.Lock()
	if len(loaded) > a.max {
		loaded = loaded[len(loaded)-a.max:]
	}
	a.events = append(a.events, loaded...)
	if a.file != nil {
		_ = a.file.Close()
	}
	a.file, a.path, a.maxBytes = f, path, maxBytes
	a.mu.Unlock()
	return nil
}
func (a *AuditLog) Append(e AuditEvent) {
	// Owner.Key contains NUL as an unambiguous separator. Sanitizing that key
	// both broke queries and conflated distinct principal/project pairs. Keep a
	// stable hash of the original bytes; never authorize by a display string.
	e.Schema = 1
	for _, field := range []*string{&e.PolicyDigest, &e.RequestDigest, &e.TargetDigest, &e.ApprovalID} {
		if digest, err := hex.DecodeString(*field); err != nil || len(digest) != sha256.Size {
			*field = ""
		}
	}
	e.Owner = AuditOwnerID(e.Owner)
	e.Operation = auditOperation(e.Operation)
	e.Decision = auditCode(e.Decision)
	e.Result = auditCode(e.Result)
	if e.At.IsZero() {
		e.At = time.Now()
	}
	e.At = e.At.UTC()
	a.mu.Lock()
	a.events = append(a.events, e)
	if len(a.events) > a.max {
		a.events = a.events[len(a.events)-a.max:]
	}
	if a.file != nil {
		if data, err := json.Marshal(e); err == nil {
			data = append(data, '\n')
			if st, err := a.file.Stat(); err == nil && st.Size()+int64(len(data)) > a.maxBytes {
				_ = a.file.Close()
				_ = os.Rename(a.path, a.path+".1")
				if f, openErr := os.OpenFile(a.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600); openErr == nil {
					a.file = f
				}
			}
			if a.file != nil {
				_, _ = a.file.Write(data)
				_ = a.file.Sync()
			}
		}
	}
	a.mu.Unlock()
}

// AuditOwnerID is an opaque correlation identity, not a credential.
func AuditOwnerID(owner string) string {
	sum := sha256.Sum256([]byte(owner))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func auditOperation(operation string) string {
	if _, err := proto.RequireOperation(operation); err == nil {
		return operation
	}
	switch operation {
	case "status", "doctor", "audit_query", "policy.grant", "approval.create", "sync.push", "sync.pull", "sync.delete", "secret.set", "secret.delete", "secret.use", "fleet.plan", "fleet.execute", "fleet.approve":
		return operation
	default:
		return "unknown"
	}
}

func auditCode(code string) string {
	switch code {
	case "", "allow", "deny", "granted", "denied", "denied by default", "capability mismatch", "policy storage unavailable", "approval_denied", "approval_required", "approval_issued", "approval_used", "approval_invalid", "accepted", "completed", "dispatch_error", "quota_rejected", "policy_updated", "policy_update_failed", "admitted", "recovery_missing", "recovery_unreachable", "state_persist_failed":
		return code
	default:
		return "unknown"
	}
}

func (a *AuditLog) Query(since time.Time) []AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]AuditEvent, 0)
	for _, e := range a.events {
		if e.At.After(since) {
			out = append(out, e)
		}
	}
	return out
}

func (a *AuditLog) QueryOwner(since time.Time, owner string) []AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]AuditEvent, 0)
	identity := AuditOwnerID(owner)
	for _, event := range a.events {
		// Legacy records used lossy display identities. They remain on disk for
		// administrator inspection but cannot safely be assigned to a principal.
		if event.Schema == 1 && event.Owner == identity && event.At.After(since) {
			out = append(out, event)
		}
	}
	return out
}

// Legacy display identities cannot be reversed into exact owner keys. Expose
// an omission marker without returning another principal's content or counts.
func (a *AuditLog) HasLegacyRecords() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, event := range a.events {
		if event.Schema != 1 {
			return true
		}
	}
	return false
}
