package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

type AuditEvent struct {
	RequestRef    string    `json:"request_ref,omitempty"`
	OperationRef  string    `json:"operation_ref,omitempty"`
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
	mu     sync.RWMutex
	max    int
	events []AuditEvent
	sink   *auditSink
	closed bool
}

func NewAuditLog(max int) *AuditLog {
	if max < 1 {
		max = 256
	}
	return &AuditLog{max: max}
}

func (a *AuditLog) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.CloseContext(ctx, true)
}

// complete is false if broker work failed to drain; the marker must retain that
// uncertainty even if the audit queue itself finishes before this deadline.
func (a *AuditLog) CloseContext(ctx context.Context, complete bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if !complete && a.sink != nil {
		a.sink.requireIncomplete.Store(true)
	}
	if !a.closed {
		a.closed = true
		if a.sink != nil {
			close(a.sink.queue)
		}
	}
	sink := a.sink
	a.mu.Unlock()
	if sink == nil {
		return nil
	}
	select {
	case <-sink.done:
		return sink.err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush waits for an explicit audit durability barrier, not a request-path
// requirement. Ordinary Append calls only update the bounded memory/queue.
func (a *AuditLog) Flush(ctx context.Context) error {
	ack := make(chan error, 1)
	for {
		a.mu.RLock()
		if a.closed {
			a.mu.RUnlock()
			return ErrClosed
		}
		sink := a.sink
		if sink == nil {
			a.mu.RUnlock()
			return nil
		}
		sent := false
		select {
		case sink.queue <- auditWrite{ack: ack}:
			sent = true
		default:
		}
		a.mu.RUnlock()
		if sent {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (a *AuditLog) SinkStatus() AuditSinkStatus {
	a.mu.RLock()
	sink := a.sink
	a.mu.RUnlock()
	if sink == nil {
		return AuditSinkStatus{State: "disabled"}
	}
	return sink.status()
}

// ConfigureFile validates bounded private segments and recovers an interrupted
// final record before starting the asynchronous writer. Reconfiguration is not
// supported while a sink is running; daemon reload retains the existing sink.
func (a *AuditLog) ConfigureFile(path string, maxBytes int64) error {
	a.mu.Lock()
	if a.sink != nil || a.closed {
		a.mu.Unlock()
		return ErrClosed
	}
	sink, events, err := newAuditSink(path, maxBytes, a.max)
	if err != nil {
		a.mu.Unlock()
		return err
	}
	a.events = append(a.events, events...)
	if len(a.events) > a.max {
		a.events = a.events[len(a.events)-a.max:]
	}
	a.sink = sink
	a.mu.Unlock()
	go sink.run()
	return nil
}
func (a *AuditLog) Append(e AuditEvent) {
	// Owner.Key contains NUL as an unambiguous separator. Sanitizing that key
	// both broke queries and conflated distinct principal/project pairs. Keep a
	// stable hash of the original bytes; never authorize by a display string.
	e.Schema = 1
	for _, field := range []*string{&e.RequestRef, &e.PolicyDigest, &e.RequestDigest, &e.TargetDigest, &e.ApprovalID, &e.OperationRef} {
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
	if a.sink != nil {
		if a.closed {
			a.sink.dropped.Add(1)
		} else {
			a.sink.pending.Add(1)
			select {
			case a.sink.queue <- auditWrite{event: e}:
				a.sink.accepted.Add(1)
			default:
				a.sink.pending.Add(-1)
				a.sink.dropped.Add(1)
			}
		}
	}
	a.mu.Unlock()
}

// NewRequestReference identifies one broker decision/approval/result chain even
// when a client reuses its request ID. It never hashes caller-supplied text.
func NewRequestReference() (string, error) {
	id, err := proto.NewOperationID()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:]), nil
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
	case "job.events", "status", "doctor", "audit_query", "audit.health", "pool.health", "mutation.status", "policy.grant", "approval.create", "sync.push", "sync.pull", "sync.delete", "secret.set", "secret.delete", "secret.list", "secret.use", "secret.set_from_file", "fleet.plan", "fleet.execute", "fleet.approve":
		return operation
	default:
		return "unknown"
	}
}

func auditCode(code string) string {
	switch code {
	case "", "allow", "deny", "granted", "denied", "denied by default", "capability mismatch", "policy storage unavailable", "approval_denied", "approval_required", "approval_issued", "approval_used", "approval_invalid", "accepted", "completed", "dispatch_error", "request_rejected", "route_rejected", "quota_rejected", "policy_updated", "policy_update_failed", "admitted", "recovery_missing", "recovery_unreachable", "recovery_found", "state_persist_failed":
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
