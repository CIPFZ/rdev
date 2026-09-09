package broker

import (
	"context"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/client"
)

// HostPool bounds reserved host slots, including setup and asynchronous cleanup.
// All broker transport dispatch must hold a slot through the entire retry and
// response-redaction path. Scheduled work reserves a slot before a lane worker;
// startup/internal dispatch uses the separately bounded cancellable wait queue.
type HostPool struct {
	wake        chan struct{}
	demand      map[string]int
	blockedCold map[string]bool
	mu          sync.Mutex
	limit       int
	entries     map[string]*hostSlot
	queue       []*hostWaiter
	detach      func(string) func()
	changed     chan struct{}
	closed      bool
	evictions   map[string]EvictionStat
}
type hostSlot struct {
	controls    int
	bulkClosing bool
	cleaned     bool
	host        string
	active      int
	closing     bool
	born, idle  time.Time
}
type hostWaiter struct {
	lane Lane
	host string
	ctx  context.Context
	done chan struct{}
	slot *hostSlot
	err  error
}

// EvictionStat uses fixed reason keys, without host, owner or request labels.
type EvictionStat struct {
	Count          uint64 `json:"count"`
	LastIdleMS     int64  `json:"last_idle_ms"`
	LastLifetimeMS int64  `json:"last_lifetime_ms"`
	LastDrainMS    int64  `json:"last_drain_ms"`
}

// PoolHealth is global administrative information; ordinary status never
// includes it. ReservedHosts includes transports still being closed.
type PoolHealth struct {
	DialAdmission             client.DialAdmissionSnapshot `json:"dial_admission"`
	Goroutines                int                          `json:"goroutines"`
	ObserverMetricSeries      int                          `json:"observer_metric_series"`
	ObserverMetricSeriesBound int                          `json:"observer_metric_series_bound"`
	ClosingBulk               int                          `json:"closing_bulk"`
	Limit                     int                          `json:"limit"`
	ReservedHosts             int                          `json:"reserved_hosts"`
	ActiveHosts               int                          `json:"active_hosts"`
	ActiveLeases              int                          `json:"active_leases"`
	ClosingHosts              int                          `json:"closing_hosts"`
	Queued                    int                          `json:"queued"`
	BaseTransports            int                          `json:"base_transports"`
	BulkTransports            int                          `json:"bulk_transports"`
	Evictions                 map[string]EvictionStat      `json:"evictions"`
}

func NewHostPool(limit int, detach func(string) func()) *HostPool {
	return &HostPool{wake: make(chan struct{}, 1), demand: make(map[string]int), blockedCold: make(map[string]bool), limit: limit, entries: make(map[string]*hostSlot), detach: detach, changed: make(chan struct{}), evictions: make(map[string]EvictionStat)}
}
func (p *HostPool) notifyLocked() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
	close(p.changed)
	p.changed = make(chan struct{})
}
func (p *HostPool) grantLocked(w *hostWaiter, slot *hostSlot) {
	slot.active++
	if w.lane == LaneControl {
		slot.controls++
	}
	slot.idle = time.Time{}
	w.slot = slot
	close(w.done)
}
func (p *HostPool) retireLocked(slot *hostSlot, reason string, now time.Time) {
	if slot.closing {
		return
	}
	slot.closing = true
	// Detach exactly this generation before permitting any other admission.
	cleanup := p.detach(slot.host)
	stat := p.evictions[reason]
	stat.Count++
	if !slot.idle.IsZero() {
		stat.LastIdleMS = max(0, now.Sub(slot.idle).Milliseconds())
	} else {
		stat.LastIdleMS = 0
	}
	stat.LastLifetimeMS = max(0, now.Sub(slot.born).Milliseconds())
	p.evictions[reason] = stat
	// At most one closer per reserved slot; the slot is not free until it exits.
	go func() {
		start := time.Now()
		cleanup()
		p.mu.Lock()
		defer p.mu.Unlock()
		slot.cleaned = true
		if !slot.bulkClosing {
			delete(p.entries, slot.host)
		}
		stat := p.evictions[reason]
		stat.LastDrainMS = time.Since(start).Milliseconds()
		p.evictions[reason] = stat
		p.pumpLocked()
		p.notifyLocked()
	}()
}
func (p *HostPool) oldestIdleLocked() *hostSlot {
	var oldest *hostSlot
	for _, slot := range p.entries {
		if !slot.closing && !slot.bulkClosing && slot.active == 0 && (oldest == nil || slot.idle.Before(oldest.idle)) {
			oldest = slot
		}
	}
	return oldest
}
func (p *HostPool) trimLocked() {
	live := 0
	for _, slot := range p.entries {
		if !slot.closing {
			live++
		}
	}
	for live > p.limit {
		slot := p.oldestIdleLocked()
		if slot == nil {
			return
		}
		p.retireLocked(slot, "capacity_reload", time.Now())
		live--
	}
}
func (p *HostPool) pumpLocked() {
	if p.closed {
		return
	}
	p.trimLocked()
	for len(p.queue) > 0 {
		w := p.queue[0]
		if err := w.ctx.Err(); err != nil {
			p.queue = p.queue[1:]
			w.err = err
			close(w.done)
			continue
		}
		slot := p.entries[w.host]
		if slot != nil && slot.closing {
			return
		}
		if len(p.entries) > p.limit {
			return
		}
		if slot == nil {
			if len(p.entries) >= p.limit {
				// Stop new non-control warm work behind this cold waiter, so an active
				// host can become idle. Warm control remains available to stop jobs.
				if oldest := p.oldestIdleLocked(); oldest != nil {
					p.retireLocked(oldest, "capacity_lru", time.Now())
				}
				return
			}
			slot = &hostSlot{host: w.host, born: time.Now()}
			p.entries[w.host] = slot
		}
		p.queue = p.queue[1:]
		p.grantLocked(w, slot)
	}
}
func (p *HostPool) Acquire(ctx context.Context, host string, lane Lane) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	w := &hostWaiter{host: host, lane: lane, ctx: ctx, done: make(chan struct{})}
	if slot := p.entries[host]; lane == LaneControl && slot != nil && !slot.closing {
		p.grantLocked(w, slot)
	} else {
		// Dispatch normally reaches here after bounded scheduler admission. Keep
		// startup/internal users bounded as well.
		if len(p.queue) >= 1024 {
			p.mu.Unlock()
			return nil, ErrQueueFull
		}
		p.queue = append(p.queue, w)
		p.pumpLocked()
	}
	p.mu.Unlock()
	select {
	case <-w.done:
	case <-ctx.Done():
		p.mu.Lock()
		if w.slot == nil && w.err == nil {
			for i, queued := range p.queue {
				if queued == w {
					p.queue = append(p.queue[:i], p.queue[i+1:]...)
					w.err = ctx.Err()
					close(w.done)
					break
				}
			}
			p.pumpLocked()
		}
		p.mu.Unlock()
		<-w.done
	}
	if w.err != nil {
		return nil, w.err
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			w.slot.active--
			if w.lane == LaneControl {
				w.slot.controls--
			}
			if w.slot.active == 0 {
				w.slot.idle = time.Now()
			}
			p.pumpLocked()
			p.notifyLocked()
		})
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
func (p *HostPool) Configure(limit int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.limit = limit
	p.pumpLocked()
	p.notifyLocked()
}
func (p *HostPool) Reap(now time.Time, ttl time.Duration, reason string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, slot := range p.entries {
		if !slot.closing && !slot.bulkClosing && slot.active == 0 && now.Sub(slot.idle) >= ttl {
			p.retireLocked(slot, reason, now)
			n++
		}
	}
	return n
}

// ReapBulk allows at most one detached bulk closer per host. Its base slot
// cannot be evicted while that closer runs, even if replacement bulk work has
// already completed. Shutdown waits for both base and detached bulk cleanup.
func (p *HostPool) ReapBulk(now time.Time, ttl time.Duration, detach func(string, time.Time, time.Duration) func()) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, slot := range p.entries {
		if slot.closing || slot.bulkClosing {
			continue
		}
		cleanup := detach(slot.host, now, ttl)
		if cleanup == nil {
			continue
		}
		slot.bulkClosing = true
		n++
		go func() {
			cleanup()
			p.mu.Lock()
			defer p.mu.Unlock()
			slot.bulkClosing = false
			if slot.cleaned {
				delete(p.entries, slot.host)
			}
			p.pumpLocked()
			p.notifyLocked()
		}()
	}
	return n
}

func (p *HostPool) Snapshot() PoolHealth {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := PoolHealth{Limit: p.limit, ReservedHosts: len(p.entries), Queued: len(p.queue), Evictions: make(map[string]EvictionStat)}
	for _, count := range p.demand {
		h.Queued += count
	}
	for _, slot := range p.entries {
		if slot.active > 0 {
			h.ActiveHosts++
			h.ActiveLeases += slot.active
		}
		if slot.bulkClosing {
			h.ClosingBulk++
		}
		if slot.closing {
			h.ClosingHosts++
		}
	}
	for reason, stat := range p.evictions {
		h.Evictions[reason] = stat
	}
	return h
}
func (p *HostPool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	for _, w := range p.queue {
		w.err = ErrClosed
		close(w.done)
	}
	p.queue = nil
	for _, slot := range p.entries {
		p.retireLocked(slot, "shutdown", time.Now())
	}
	for len(p.entries) > 0 {
		changed := p.changed
		p.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		p.mu.Lock()
	}
	p.mu.Unlock()
	return nil
}

// Scheduler admission reserves a host before occupying any lane worker. Cold
// waits stay in the weighted owner queue, leaving warm control executable even
// for an owner that already has a queued cold control request.
type hostPoolLeaseKey struct{}
type hostPoolBinding struct {
	pool *HostPool
	host string
}

func (p *HostPool) dispatchLease(ctx context.Context, host string, lane Lane) (func(), error) {
	if b, ok := ctx.Value(hostPoolLeaseKey{}).(hostPoolBinding); ok && b.pool == p && b.host == host {
		return func() {}, nil
	}
	return p.Acquire(ctx, host, lane)
}
func (p *HostPool) Demand(host string, delta int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.demand[host] += delta
	if p.demand[host] <= 0 {
		delete(p.demand, host)
		delete(p.blockedCold, host)
	}
}
func (p *HostPool) TryAcquire(host string, lane Lane) (func(), bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false
	}
	p.trimLocked()
	slot := p.entries[host]
	if slot != nil && slot.closing {
		return nil, false
	}
	if slot == nil {
		if len(p.entries) >= p.limit {
			p.blockedCold[host] = true
			if oldest := p.oldestIdleLocked(); oldest != nil {
				p.retireLocked(oldest, "capacity_lru", time.Now())
			}
			return nil, false
		}
		slot = &hostSlot{host: host, born: time.Now()}
		p.entries[host] = slot
		delete(p.blockedCold, host)
	} else {
		if lane != LaneControl && len(p.entries) > p.limit {
			return nil, false
		}
		if len(p.entries) >= p.limit {
			for cold := range p.blockedCold {
				if p.entries[cold] == nil {
					// Let active work retain one control request (e.g. job_stop), while
					// preventing overlapping hot pings from keeping the host busy forever.
					if lane != LaneControl || slot.active == 0 || slot.controls > 0 {
						return nil, false
					}
				}
			}
		}
	}
	slot.active++
	if lane == LaneControl {
		slot.controls++
	}
	slot.idle = time.Time{}
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			slot.active--
			if lane == LaneControl {
				slot.controls--
			}
			if slot.active == 0 {
				slot.idle = time.Now()
			}
			p.pumpLocked()
			p.notifyLocked()
		})
	}, true
}
