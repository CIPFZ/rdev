package broker

import (
	"sync"
	"time"
)

type Lease struct {
	mu                sync.Mutex
	clients, inflight int
	detachedAt        time.Time
	grace             time.Duration
}

func NewLease(grace time.Duration) *Lease { return &Lease{grace: grace} }
func (l *Lease) Attach()                  { l.mu.Lock(); l.clients++; l.detachedAt = time.Time{}; l.mu.Unlock() }
func (l *Lease) Detach() {
	l.mu.Lock()
	if l.clients > 0 {
		l.clients--
	}
	if l.clients == 0 {
		l.detachedAt = time.Now()
	}
	l.mu.Unlock()
}
func (l *Lease) Begin() { l.mu.Lock(); l.inflight++; l.mu.Unlock() }
func (l *Lease) End() {
	l.mu.Lock()
	if l.inflight > 0 {
		l.inflight--
	}
	l.mu.Unlock()
}
func (l *Lease) Reapable(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.clients == 0 && l.inflight == 0 && !l.detachedAt.IsZero() && now.Sub(l.detachedAt) >= l.grace
}
