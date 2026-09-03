// Package connmgr owns the lifecycle of reusable remote transports.
package connmgr

import (
	"context"
	"errors"
	"fmt"
	"sort"
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
	Evicting       = EVICTING
	Draining State = "DRAINING"
)

var (
	ErrDraining  = errors.New("connection is draining")
	ErrNotFound  = errors.New("connection not found")
	ErrQueueFull = errors.New("connection queue is full")
	ErrWarmLimit = errors.New("connection warm-host limit reached")
)

type Connection interface{ Close() error }
type GracefulConnection interface{ GracefulClose(context.Context) error }
type ControlMasterCloser interface{ CloseControlMaster(context.Context) error }

type Config struct {
	MaxQueue        int
	MaxWarmHosts    int
	IdleTTL         time.Duration
	MaxIdleTTL      time.Duration
	LastClientGrace time.Duration
	DrainTimeout    time.Duration
	Now             func() time.Time
}

// Validate checks explicit policy values before a manager is constructed.
func (c Config) Validate() error {
	if c.MaxQueue < 0 || c.MaxWarmHosts < 0 {
		return errors.New("connection limits must not be negative")
	}
	if c.IdleTTL < 0 || c.MaxIdleTTL < 0 || c.LastClientGrace < 0 || c.DrainTimeout < 0 {
		return errors.New("connection durations must not be negative")
	}
	if c.IdleTTL > 0 && c.MaxIdleTTL > 0 && c.MaxIdleTTL < c.IdleTTL {
		return errors.New("max idle TTL must be at least idle TTL")
	}
	return nil
}

type Manager struct {
	mu                                                 sync.Mutex
	entries                                            map[string]*entry
	maxQueue, maxWarmHosts                             int
	idleTTL, maxIdleTTL, lastClientGrace, drainTimeout time.Duration
	now                                                func() time.Time
}

type entry struct {
	key                              string
	state                            State
	conn                             Connection
	lease, inflight, queued          int
	lastActivity, lastClientDetached time.Time
	dialDone                         chan struct{}
	evictDone                        chan struct{}
	lastErr                          error
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
	LastClientDetached      time.Time
	LastError               string
}

func New(cfg Config) *Manager {
	// New is kept infallible for compatibility. Invalid negative values are
	// normalized to safe defaults; callers needing strict validation can call
	// Config.Validate before constructing the manager.
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 256
	}
	if cfg.MaxWarmHosts < 0 {
		cfg.MaxWarmHosts = 0
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 3 * time.Minute
	}
	if cfg.MaxIdleTTL <= 0 {
		cfg.MaxIdleTTL = 10 * time.Minute
	}
	if cfg.MaxIdleTTL < cfg.IdleTTL {
		cfg.MaxIdleTTL = cfg.IdleTTL
	}
	if cfg.LastClientGrace <= 0 {
		cfg.LastClientGrace = 30 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 2 * time.Second
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Manager{entries: make(map[string]*entry), maxQueue: cfg.MaxQueue,
		maxWarmHosts: cfg.MaxWarmHosts, idleTTL: cfg.IdleTTL, maxIdleTTL: cfg.MaxIdleTTL,
		lastClientGrace: cfg.LastClientGrace, drainTimeout: cfg.DrainTimeout, now: now}
}
func (m *Manager) timestamp() time.Time { return m.now() }
func (m *Manager) warmCountLocked() int {
	n := 0
	for _, e := range m.entries {
		if e.conn != nil && (e.state == Warm || e.state == Draining || e.state == Dialing) {
			n++
		}
	}
	return n
}
func (m *Manager) lruIdleLocked(exclude string) *entry {
	var victim *entry
	for _, e := range m.entries {
		if e.key == exclude || e.conn == nil || e.state != Warm || e.lease != 0 || e.inflight != 0 || e.queued != 0 {
			continue
		}
		if victim == nil || e.lastActivity.Before(victim.lastActivity) ||
			(e.lastActivity.Equal(victim.lastActivity) && e.key < victim.key) {
			victim = e
		}
	}
	return victim
}
func (m *Manager) beginEvictionLocked(e *entry) {
	e.state = EVICTING
	e.evictDone = make(chan struct{})
}
func (m *Manager) closeConnection(c Connection) {
	if c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.drainTimeout)
	defer cancel()
	if graceful, ok := c.(GracefulConnection); ok {
		_ = graceful.GracefulClose(ctx)
	}
	_ = c.Close()
	if master, ok := c.(ControlMasterCloser); ok {
		_ = master.CloseControlMaster(ctx)
	}
}
func (m *Manager) finishEviction(e *entry, done chan struct{}, reason string) {
	m.mu.Lock()
	if cur := m.entries[e.key]; cur == e {
		e.state = Cold
		e.lastErr = fmt.Errorf("evicted: %s", reason)
	}
	close(done)
	m.mu.Unlock()
}

// Acquire coalesces concurrent dials and enforces MaxWarmHosts using an idle
// LRU victim. Dial and close callbacks never run under the manager mutex.
func (m *Manager) Acquire(ctx context.Context, key string, dial func(context.Context) (Connection, error)) (*Lease, error) {
	if m == nil || key == "" || dial == nil || ctx == nil {
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
			e.lastActivity = m.timestamp()
			c := e.conn
			m.mu.Unlock()
			return &Lease{m: m, key: key, conn: c}, nil
		}
		if e.state == Draining && e.conn != nil {
			e.state = Warm
			e.lease++
			e.lastClientDetached = time.Time{}
			e.lastActivity = m.timestamp()
			c := e.conn
			m.mu.Unlock()
			return &Lease{m: m, key: key, conn: c}, nil
		}
		if e.state == EVICTING {
			done := e.evictDone
			m.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
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
		if m.maxWarmHosts > 0 && m.warmCountLocked() >= m.maxWarmHosts {
			victim := m.lruIdleLocked(key)
			if victim == nil {
				m.mu.Unlock()
				return nil, ErrWarmLimit
			}
			m.beginEvictionLocked(victim)
			done := victim.evictDone
			c := victim.conn
			victim.conn = nil
			m.mu.Unlock()
			m.closeConnection(c)
			m.finishEviction(victim, done, "LRU")
			continue
		}
		e.state = Dialing
		e.dialDone = make(chan struct{})
		done := e.dialDone
		m.mu.Unlock()
		conn, err := dial(ctx)
		m.mu.Lock()
		if err != nil || conn == nil {
			if err == nil {
				err = errors.New("dial returned a nil connection")
			}
			e.state = Backoff
			e.lastErr = err
			close(done)
			e.dialDone = nil
			m.mu.Unlock()
			return nil, err
		}
		e.conn, e.state, e.lastErr, e.lease, e.lastActivity = conn, Warm, nil, 1, m.timestamp()
		e.lastClientDetached = time.Time{}
		close(done)
		e.dialDone = nil
		m.mu.Unlock()
		return &Lease{m: m, key: key, conn: conn}, nil
	}
}

func (m *Manager) releaseLease(key string) {
	var e *entry
	m.mu.Lock()
	e = m.entries[key]
	if e != nil && e.lease > 0 {
		e.lease--
		e.lastActivity = m.timestamp()
		if e.lease == 0 {
			e.lastClientDetached = m.timestamp()
		}
	}
	closeNow := e != nil && e.state == Draining && e.lease == 0 && e.inflight == 0 && e.queued == 0
	var done chan struct{}
	var c Connection
	if closeNow {
		m.beginEvictionLocked(e)
		done = e.evictDone
		c = e.conn
		e.conn = nil
	}
	m.mu.Unlock()
	if closeNow {
		m.closeConnection(c)
		m.finishEviction(e, done, "drain complete")
	}
}
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
	e.lastActivity = m.timestamp()
	var once sync.Once
	return func() {
		once.Do(func() {
			var c Connection
			var done chan struct{}
			m.mu.Lock()
			if e.queued > 0 {
				e.queued--
			}
			e.lastActivity = m.timestamp()
			if e.state == Draining && e.lease == 0 && e.inflight == 0 && e.queued == 0 {
				m.beginEvictionLocked(e)
				done, c = e.evictDone, e.conn
				e.conn = nil
			}
			m.mu.Unlock()
			if done != nil {
				m.closeConnection(c)
				m.finishEviction(e, done, "drain complete")
			}
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
	if e.state == EVICTING || e.state == Draining {
		return ErrDraining
	}
	e.inflight++
	e.lastActivity = m.timestamp()
	return nil
}
func (m *Manager) End(key string) error {
	var e *entry
	m.mu.Lock()
	e = m.entries[key]
	if e == nil {
		m.mu.Unlock()
		return ErrNotFound
	}
	if e.inflight > 0 {
		e.inflight--
	}
	e.lastActivity = m.timestamp()
	closeNow := e.state == Draining && e.lease == 0 && e.inflight == 0 && e.queued == 0
	var done chan struct{}
	var c Connection
	if closeNow {
		m.beginEvictionLocked(e)
		done = e.evictDone
		c = e.conn
		e.conn = nil
	}
	m.mu.Unlock()
	if closeNow {
		m.closeConnection(c)
		m.finishEviction(e, done, "drain complete")
	}
	return nil
}
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
	m.beginEvictionLocked(e)
	done := e.evictDone
	c := e.conn
	e.conn = nil
	m.mu.Unlock()
	m.closeConnection(c)
	m.mu.Lock()
	if cur := m.entries[key]; cur == e {
		e.state = Cold
		e.lastErr = fmt.Errorf("evicted: %s", reason)
	}
	close(done)
	m.mu.Unlock()
	return true
}

// Reap closes idle entries by TTL or by the shorter last-client grace period.
func (m *Manager) Reap(at time.Time) int {
	if at.IsZero() {
		at = m.timestamp()
	}
	var victims []*entry
	m.mu.Lock()
	for _, e := range m.entries {
		if e.conn == nil || e.state != Warm || e.lease != 0 || e.inflight != 0 || e.queued != 0 {
			continue
		}
		if at.Sub(e.lastActivity) >= m.idleTTL || (!e.lastClientDetached.IsZero() && at.Sub(e.lastClientDetached) >= m.lastClientGrace) {
			m.beginEvictionLocked(e)
			victims = append(victims, e)
		}
	}
	m.mu.Unlock()
	for _, e := range victims {
		m.mu.Lock()
		done := e.evictDone
		c := e.conn
		e.conn = nil
		m.mu.Unlock()
		m.closeConnection(c)
		m.finishEviction(e, done, "idle TTL")
	}
	return len(victims)
}
func (m *Manager) Snapshot(key string) Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key]
	if e == nil {
		return Snapshot{Key: key, State: Cold}
	}
	s := Snapshot{Key: key, State: e.state, Lease: e.lease, Inflight: e.inflight, Queued: e.queued, LastActivity: e.lastActivity, LastClientDetached: e.lastClientDetached}
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
		s := Snapshot{Key: e.key, State: e.state, Lease: e.lease, Inflight: e.inflight, Queued: e.queued, LastActivity: e.lastActivity, LastClientDetached: e.lastClientDetached}
		if e.lastErr != nil {
			s.LastError = e.lastErr.Error()
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
