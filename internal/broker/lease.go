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

func NewLease(grace time.Duration) *Lease     { return &Lease{grace: grace, detachedAt: time.Now()} }
func (l *Lease) SetGrace(grace time.Duration) { l.mu.Lock(); l.grace = grace; l.mu.Unlock() }
func (l *Lease) Attach()                      { l.mu.Lock(); l.clients++; l.detachedAt = time.Time{}; l.mu.Unlock() }
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
	return l.reapable(now)
}

func (l *Lease) reapable(now time.Time) bool {
	return l.clients == 0 && l.inflight == 0 && !l.detachedAt.IsZero() && now.Sub(l.detachedAt) >= l.grace
}

// Reap makes the idle check and pool detachment atomic with Attach and Begin.
// detach must only remove the pool snapshot; blocking transport cleanup runs
// after admission is unlocked, so a new caller need not wait for old SSH exits.
func (l *Lease) Reap(now time.Time, detach func() func()) bool {
	l.mu.Lock()
	if !l.reapable(now) {
		l.mu.Unlock()
		return false
	}
	cleanup := detach()
	l.detachedAt = time.Time{}
	l.mu.Unlock()
	cleanup()
	return true
}
