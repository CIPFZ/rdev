// Package connmgr owns the lifecycle of reusable remote transports.
package connmgr

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	// MaxConcurrentDials bounds calls to dial across all hosts. A non-positive
	// value selects the conservative default of six.
	MaxConcurrentDials int
	// BaseBackoff and MaxBackoff bound reconnect attempts after a failed dial.
	// Jitter applies a deterministic +/-50% spread using Random.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Jitter      bool
	Random      func() float64
	// Rand is an alias retained for callers that use the shorter name.
	Rand func() float64
	// OnEvent receives low-cardinality lifecycle events. Key is an opaque
	// canonical key and must not be treated as a user-facing label.
	OnEvent  func(Event)
	Observer interface{ RecordConnection(string, string) }
}

// Validate checks explicit policy values before a manager is constructed.
func (c Config) Validate() error {
	if c.MaxQueue < 0 || c.MaxWarmHosts < 0 || c.MaxConcurrentDials < 0 {
		return errors.New("connection limits must not be negative")
	}
	if c.IdleTTL < 0 || c.MaxIdleTTL < 0 || c.LastClientGrace < 0 || c.DrainTimeout < 0 || c.BaseBackoff < 0 || c.MaxBackoff < 0 {
		return errors.New("connection durations must not be negative")
	}
	if c.IdleTTL > 0 && c.MaxIdleTTL > 0 && c.MaxIdleTTL < c.IdleTTL {
		return errors.New("max idle TTL must be at least idle TTL")
	}
	if c.BaseBackoff > 0 && c.MaxBackoff > 0 && c.MaxBackoff < c.BaseBackoff {
		return errors.New("max backoff must be at least base backoff")
	}
	return nil
}

type Manager struct {
	mu                                                 sync.Mutex
	entries                                            map[string]*entry
	maxQueue, maxWarmHosts                             int
	idleTTL, maxIdleTTL, lastClientGrace, drainTimeout time.Duration
	now                                                func() time.Time
	dialSem                                            *dialSemaphore
	baseBackoff, maxBackoff                            time.Duration
	jitter                                             bool
	random                                             func() float64
	onEvent                                            func(Event)
	observer                                           interface{ RecordConnection(string, string) }
}

// dialSemaphore is a FIFO, context-aware semaphore. A buffered channel is
// sufficient to cap concurrency, but does not provide a fairness guarantee:
// a newly arriving host can repeatedly win a send race over an older waiter.
// Keeping the waiter queue explicit also lets cancellation remove a waiter
// without leaving a token or goroutine behind.
type dialSemaphore struct {
	mu      sync.Mutex
	limit   int
	inUse   int
	waiters []*dialWaiter
}

type dialWaiter struct {
	ready   chan struct{}
	granted bool
}

func newDialSemaphore(limit int) *dialSemaphore {
	if limit <= 0 {
		limit = 1
	}
	return &dialSemaphore{limit: limit}
}

func (s *dialSemaphore) acquire(ctx context.Context) error {
	w := &dialWaiter{ready: make(chan struct{})}
	s.mu.Lock()
	if s.inUse < s.limit && len(s.waiters) == 0 {
		s.inUse++
		s.mu.Unlock()
		return nil
	}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		if w.granted {
			// The grant may race with cancellation. This caller will not use the
			// token, so return it and wake the next FIFO waiter.
			s.inUse--
			s.grantNextLocked()
			s.mu.Unlock()
			return ctx.Err()
		}
		for i, queued := range s.waiters {
			if queued == w {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

func (s *dialSemaphore) grantNextLocked() {
	if s.inUse >= s.limit || len(s.waiters) == 0 {
		return
	}
	w := s.waiters[0]
	s.waiters = s.waiters[1:]
	w.granted = true
	s.inUse++
	close(w.ready)
}

func (s *dialSemaphore) release() {
	s.mu.Lock()
	if s.inUse > 0 {
		s.inUse--
	}
	s.grantNextLocked()
	s.mu.Unlock()
}

type entry struct {
	key                              string
	state                            State
	conn                             Connection
	lease, inflight, queued          int
	lastActivity, lastClientDetached time.Time
	dialDone                         chan struct{}
	lastDialToken                    chan struct{}
	lastDialErr                      error
	evictDone                        chan struct{}
	lastErr                          error
	nextAttempt                      time.Time
	failures                         int
	backoffDone                      chan struct{}
}

// Event is a stable connection lifecycle observation. Values intentionally
// use a closed vocabulary; no remote path, argv, or error text is included.
type Event struct {
	Key    string
	Name   string
	Reason string
}

// FailureClass is a bounded classification suitable for retry policy and
// metrics. It deliberately avoids exposing arbitrary error strings.
type FailureClass string

const (
	FailureCanceled FailureClass = "canceled"
	FailureTimeout  FailureClass = "timeout"
	FailureAuth     FailureClass = "auth"
	FailureNetwork  FailureClass = "network"
	FailureResource FailureClass = "resource"
	FailureUnknown  FailureClass = "unknown"
)

// ClassifyDialFailure maps common dial errors to a stable low-cardinality
// class. Callers may wrap errors; errors.Is/As still work through wrapping.
func ClassifyDialFailure(err error) FailureClass {
	if err == nil {
		return FailureUnknown
	}
	if errors.Is(err, context.Canceled) {
		return FailureCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return FailureTimeout
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "permission denied") || strings.Contains(msg, "authentication") || strings.Contains(msg, "publickey") {
		return FailureAuth
	}
	if strings.Contains(msg, "too many open files") || strings.Contains(msg, "resource temporarily unavailable") {
		return FailureResource
	}
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection reset") || strings.Contains(msg, "no route") || strings.Contains(msg, "network") {
		return FailureNetwork
	}
	return FailureUnknown
}

// classifyDialFailure is kept as a small internal alias for event emission.
func classifyDialFailure(err error) string { return string(ClassifyDialFailure(err)) }

const (
	EventDialStarted   = "connection.dial_started"
	EventDialSucceeded = "connection.dial_succeeded"
	EventDialFailed    = "connection.dial_failed"
	EventBackoff       = "connection.backoff"
	EventEvicted       = "connection.evicted"
	EventDisconnected  = "connection.disconnected"
)

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
	RetryAt                 time.Time
	FailureCount            int
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
	if cfg.MaxConcurrentDials <= 0 {
		cfg.MaxConcurrentDials = 6
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.MaxBackoff < cfg.BaseBackoff {
		cfg.MaxBackoff = cfg.BaseBackoff
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	random := cfg.Random
	if random == nil {
		random = cfg.Rand
	}
	if random == nil {
		random = func() float64 { return float64(time.Now().UnixNano()%1_000_000) / 1_000_000 }
	}
	return &Manager{entries: make(map[string]*entry), maxQueue: cfg.MaxQueue,
		maxWarmHosts: cfg.MaxWarmHosts, idleTTL: cfg.IdleTTL, maxIdleTTL: cfg.MaxIdleTTL,
		lastClientGrace: cfg.LastClientGrace, drainTimeout: cfg.DrainTimeout, now: now,
		dialSem: newDialSemaphore(cfg.MaxConcurrentDials), baseBackoff: cfg.BaseBackoff,
		maxBackoff: cfg.MaxBackoff, jitter: cfg.Jitter, random: random, onEvent: cfg.OnEvent, observer: cfg.Observer}
}
func (m *Manager) timestamp() time.Time { return m.now() }
func (m *Manager) emit(key, name, reason string) {
	if m.onEvent != nil {
		m.onEvent(Event{Key: key, Name: name, Reason: reason})
	}
	if m.observer != nil {
		m.observer.RecordConnection(name, reason)
	}
}

func leaveBackoffLocked(e *entry) {
	if e.backoffDone != nil {
		close(e.backoffDone)
		e.backoffDone = nil
	}
	e.nextAttempt = time.Time{}
}

func (m *Manager) waitBackoff(ctx context.Context, e *entry) error {
	for {
		m.mu.Lock()
		until := e.nextAttempt
		if e.state != Backoff || until.IsZero() || !m.timestamp().Before(until) {
			if e.state == Backoff {
				e.state = Cold
				leaveBackoffLocked(e)
			}
			m.mu.Unlock()
			return nil
		}
		d := until.Sub(m.timestamp())
		done := e.backoffDone
		m.mu.Unlock()
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-done:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
		case <-ctx.Done():
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
}

func (m *Manager) backoffDuration(failures int) time.Duration {
	d := m.baseBackoff
	for i := 1; i < failures && d < m.maxBackoff; i++ {
		if d > m.maxBackoff/2 {
			d = m.maxBackoff
			break
		}
		d *= 2
	}
	if d > m.maxBackoff {
		d = m.maxBackoff
	}
	if !m.jitter || d <= 0 {
		return d
	}
	r := m.random()
	if r < 0 {
		r = 0
	}
	if r > 1 {
		r = 1
	}
	// Apply a +/-50% spread. Clamp to MaxBackoff so jitter never defeats the
	// configured circuit-open bound.
	j := time.Duration(float64(d) * (0.5 + r))
	if j > m.maxBackoff {
		j = m.maxBackoff
	}
	return j
}
func (m *Manager) warmCountLocked() int {
	n := 0
	for _, e := range m.entries {
		// Keep EVICTING connections counted until Close has returned.  The
		// underlying transport is still live during a slow close; dropping it
		// from the count before then would let a full pool temporarily exceed
		// MaxWarmHosts.
		if e.state == Dialing || (e.conn != nil && (e.state == Warm || e.state == Draining || e.state == EVICTING)) {
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
		e.conn = nil
		e.state = Cold
		e.lastErr = fmt.Errorf("evicted: %s", reason)
	}
	close(done)
	m.mu.Unlock()
	m.emit(e.key, EventEvicted, reason)
	m.emit(e.key, EventDisconnected, reason)
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
		if e.state == Backoff {
			if err := ctx.Err(); err != nil {
				m.mu.Unlock()
				return nil, err
			}
			// Circuit-open callers fail fast during the retry window. This is
			// important for a hundred-host recovery: callers do not each sleep
			// and then stampede the first instant the timer expires. A later
			// caller (or an explicit retry loop) can try again after RetryAt.
			err := e.lastErr
			until := e.nextAttempt
			now := m.timestamp()
			m.mu.Unlock()
			if !until.IsZero() && now.Before(until) {
				if err != nil {
					return nil, err
				}
				return nil, errors.New("connection is backing off")
			}
			m.mu.Lock()
			if e.state == Backoff && !m.timestamp().Before(e.nextAttempt) {
				e.state = Cold
				leaveBackoffLocked(e)
			}
			m.mu.Unlock()
			continue
		}
		if e.state == Dialing {
			done := e.dialDone
			m.mu.Unlock()
			select {
			case <-done:
				m.mu.Lock()
				err := error(nil)
				if e.lastDialToken == done {
					err = e.lastDialErr
				}
				m.mu.Unlock()
				if err != nil {
					// A canceled leader must not poison callers that are still
					// willing to dial. The next loop iteration can take over the
					// cold entry once the global semaphore is available.
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						continue
					}
					return nil, err
				}
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
			m.mu.Unlock()
			m.closeConnection(c)
			m.finishEviction(victim, done, "LRU")
			continue
		}
		e.state = Dialing
		e.dialDone = make(chan struct{})
		done := e.dialDone
		m.mu.Unlock()
		// Admission is context-aware. No goroutine or token is retained when a
		// caller cancels while waiting for the global dial budget.
		if err := m.dialSem.acquire(ctx); err != nil {
			m.mu.Lock()
			if e.state == Dialing && e.dialDone == done {
				e.state = Cold
				e.lastDialToken, e.lastDialErr = done, err
				close(done)
				e.dialDone = nil
			}
			m.mu.Unlock()
			return nil, err
		}
		m.emit(key, EventDialStarted, "")
		conn, err := dial(ctx)
		m.dialSem.release()
		m.mu.Lock()
		if err != nil || conn == nil {
			if err == nil {
				err = errors.New("dial returned a nil connection")
			}
			e.lastErr = err
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				// A caller withdrawing its own context is not a transport health
				// failure and must not open the circuit for other callers.
				e.state = Cold
				leaveBackoffLocked(e)
			} else {
				e.state = Backoff
				e.failures++
				delay := m.backoffDuration(e.failures)
				e.nextAttempt = m.timestamp().Add(delay)
				e.backoffDone = make(chan struct{})
			}
			e.lastDialToken, e.lastDialErr = done, err
			wasBackoff := e.state == Backoff
			close(done)
			e.dialDone = nil
			m.mu.Unlock()
			m.emit(key, EventDialFailed, classifyDialFailure(err))
			if wasBackoff {
				m.emit(key, EventBackoff, classifyDialFailure(err))
			}
			return nil, err
		}
		e.conn, e.state, e.lastErr, e.lease, e.lastActivity = conn, Warm, nil, 1, m.timestamp()
		e.lastDialToken, e.lastDialErr = done, nil
		e.failures = 0
		leaveBackoffLocked(e)
		e.lastClientDetached = time.Time{}
		close(done)
		e.dialDone = nil
		m.mu.Unlock()
		m.emit(key, EventDialSucceeded, "")
		return &Lease{m: m, key: key, conn: conn}, nil
	}
}

func (m *Manager) releaseLease(key string) {
	var e *entry
	m.mu.Lock()
	e = m.entries[key]
	released := false
	if e != nil && e.lease > 0 {
		released = true
		e.lease--
		e.lastActivity = m.timestamp()
		if e.lease == 0 {
			e.lastClientDetached = m.timestamp()
		}
	}
	closeNow := released && e.state == Draining && e.lease == 0 && e.inflight == 0 && e.queued == 0
	var done chan struct{}
	var c Connection
	if closeNow {
		m.beginEvictionLocked(e)
		done = e.evictDone
		c = e.conn
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
	if e.state == EVICTING || e.state == Draining {
		return ErrDraining
	}
	if e.queued > 0 {
		e.queued--
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
	if e.inflight == 0 {
		m.mu.Unlock()
		return nil
	}
	e.inflight--
	e.lastActivity = m.timestamp()
	closeNow := e.state == Draining && e.lease == 0 && e.inflight == 0 && e.queued == 0
	var done chan struct{}
	var c Connection
	if closeNow {
		m.beginEvictionLocked(e)
		done = e.evictDone
		c = e.conn
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
	m.mu.Unlock()
	m.closeConnection(c)
	m.finishEviction(e, done, reason)
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
	s := Snapshot{Key: key, State: e.state, Lease: e.lease, Inflight: e.inflight, Queued: e.queued, LastActivity: e.lastActivity, LastClientDetached: e.lastClientDetached, RetryAt: e.nextAttempt, FailureCount: e.failures}
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
		s := Snapshot{Key: e.key, State: e.state, Lease: e.lease, Inflight: e.inflight, Queued: e.queued, LastActivity: e.lastActivity, LastClientDetached: e.lastClientDetached, RetryAt: e.nextAttempt, FailureCount: e.failures}
		if e.lastErr != nil {
			s.LastError = e.lastErr.Error()
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
