// Package connmgr owns the lifecycle of reusable remote transports.
package connmgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type State string

const (
	Cold     State = "COLD"
	Dialing  State = "DIALING"
	Warm     State = "WARM"
	Backoff  State = "BACKOFF"
	EVICTING State = "EVICTING"
	Draining State = "DRAINING"
	// Evicting is the idiomatic spelling; EVICTING is retained for callers
	// that mirror the lifecycle diagram's uppercase state names.
	Evicting = EVICTING
)

var (
	ErrDraining  = errors.New("connection is draining")
	ErrNotFound  = errors.New("connection not found")
	ErrQueueFull = errors.New("connection queue is full")
)

// Connection is the small lifecycle surface required by the manager.
type Connection interface{ Close() error }

type Config struct{ MaxQueue int }

type Manager struct {
	mu       sync.Mutex
	entries  map[string]*entry
	maxQueue int
}

type entry struct {
	key                     string
	state                   State
	conn                    Connection
	lease, inflight, queued int
	lastActivity            time.Time
	dialDone                chan struct{}
	lastErr                 error
}

type Lease struct {
	m    *Manager
	key  string
	conn Connection
	once sync.Once
}

func (l *Lease) Conn() Connection {
	if l == nil {
		return nil
	}
	return l.conn
}
func (l *Lease) Release() {
	if l == nil || l.m == nil {
		return
	}
	l.once.Do(func() { l.m.releaseLease(l.key) })
}

type Snapshot struct {
	Key                     string
	State                   State
	Lease, Inflight, Queued int
	LastActivity            time.Time
	LastError               string
}

func New(cfg Config) *Manager {
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 256
	}
	return &Manager{entries: make(map[string]*entry), maxQueue: cfg.MaxQueue}
}

// Acquire coalesces concurrent dials for one canonical key. The dial function
// runs outside the manager lock; exactly one caller owns it and all followers
// observe the same result.
func (m *Manager) Acquire(ctx context.Context, key string, dial func(context.Context) (Connection, error)) (*Lease, error) {
	if m == nil || key == "" || dial == nil {
		return nil, errors.New("invalid connection acquire")
	}
	for {
		m.mu.Lock()
		e := m.entries[key]
		if e == nil {
			e = &entry{key: key, state: Cold}
			m.entries[key] = e
		}
		if e.state == Warm && e.conn != nil {
			e.lease++
			e.lastActivity = time.Now()
			c := e.conn
			m.mu.Unlock()
			return &Lease{m: m, key: key, conn: c}, nil
		}
		if e.state == Draining {
			e.state = Warm
			e.lease++
			e.lastActivity = time.Now()
			c := e.conn
			m.mu.Unlock()
			return &Lease{m: m, key: key, conn: c}, nil
		}
		if e.state == Dialing {
			done := e.dialDone
			m.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		e.state = Dialing
		e.dialDone = make(chan struct{})
		done := e.dialDone
		m.mu.Unlock()
		conn, err := dial(ctx)
		m.mu.Lock()
		if err != nil {
			e.state = Backoff
			e.lastErr = err
			close(done)
			e.dialDone = nil
			m.mu.Unlock()
			return nil, err
		}
		e.conn, e.state, e.lastErr, e.lease, e.lastActivity = conn, Warm, nil, 1, time.Now()
		close(done)
		e.dialDone = nil
		m.mu.Unlock()
		return &Lease{m: m, key: key, conn: conn}, nil
	}
}

func (m *Manager) releaseLease(key string) {
	m.mu.Lock()
	if e := m.entries[key]; e != nil && e.lease > 0 {
		e.lease--
		e.lastActivity = time.Now()
	}
	m.mu.Unlock()
}

// Queue reserves one bounded request slot. Call the returned function exactly
// once when the request leaves the queue (whether it starts or is canceled).
func (m *Manager) Queue(key string) (func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key]
	if e == nil || e.conn == nil {
		return nil, ErrNotFound
	}
	if e.state == EVICTING || e.state == Draining {
		return nil, ErrDraining
	}
	if e.queued >= m.maxQueue {
		return nil, ErrQueueFull
	}
	e.queued++
	e.lastActivity = time.Now()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			if e.queued > 0 {
				e.queued--
			}
			e.lastActivity = time.Now()
			m.mu.Unlock()
		})
	}, nil
}

func (m *Manager) Begin(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key]
	if e == nil {
		return ErrNotFound
	}
	if e.queued > 0 {
		e.queued--
	}
	if e.state == EVICTING {
		return ErrDraining
	}
	e.inflight++
	e.lastActivity = time.Now()
	return nil
}
func (m *Manager) End(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key]
	if e == nil {
		return ErrNotFound
	}
	if e.inflight > 0 {
		e.inflight--
	}
	e.lastActivity = time.Now()
	return nil
}

// Evict atomically enters DRAINING and closes only an idle connection. Busy
// connections remain warm; a subsequent retry can safely race with eviction.
func (m *Manager) Evict(key, reason string) bool {
	m.mu.Lock()
	e := m.entries[key]
	if e == nil || e.conn == nil || (e.state != Warm && e.state != Draining) {
		m.mu.Unlock()
		return false
	}
	if e.lease != 0 || e.inflight != 0 || e.queued != 0 {
		e.state = Draining
		m.mu.Unlock()
		return false
	}
	e.state = EVICTING
	c := e.conn
	e.conn = nil
	m.mu.Unlock()
	_ = c.Close()
	m.mu.Lock()
	if cur := m.entries[key]; cur == e {
		e.state = Cold
		e.lastErr = fmt.Errorf("evicted: %s", reason)
		delete(m.entries, key)
	}
	m.mu.Unlock()
	return true
}

func (m *Manager) Snapshot(key string) Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key]
	if e == nil {
		return Snapshot{Key: key, State: Cold}
	}
	s := Snapshot{Key: key, State: e.state, Lease: e.lease, Inflight: e.inflight, Queued: e.queued, LastActivity: e.lastActivity}
	if e.lastErr != nil {
		s.LastError = e.lastErr.Error()
	}
	return s
}
func (m *Manager) Snapshots() []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Snapshot, 0, len(m.entries))
	for _, e := range m.entries {
		s := Snapshot{Key: e.key, State: e.state, Lease: e.lease, Inflight: e.inflight, Queued: e.queued, LastActivity: e.lastActivity}
		if e.lastErr != nil {
			s.LastError = e.lastErr.Error()
		}
		out = append(out, s)
	}
	return out
}
