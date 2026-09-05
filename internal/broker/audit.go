package broker

import (
	"sync"
	"time"
)

type AuditEvent struct {
	At                                 time.Time `json:"at"`
	Owner, Operation, Decision, Result string    `json:"owner,omitempty"`
}
type AuditLog struct {
	mu     sync.RWMutex
	max    int
	events []AuditEvent
}

func NewAuditLog(max int) *AuditLog {
	if max < 1 {
		max = 256
	}
	return &AuditLog{max: max}
}
func (a *AuditLog) Append(e AuditEvent) {
	e.At = e.At.UTC()
	a.mu.Lock()
	a.events = append(a.events, e)
	if len(a.events) > a.max {
		a.events = a.events[len(a.events)-a.max:]
	}
	a.mu.Unlock()
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
