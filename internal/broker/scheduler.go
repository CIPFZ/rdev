package broker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

var ErrQueueFull = errors.New("broker request queue is full")

// QoSConfig limits execution and pending work independently. Zero fields use
// defaults. Two control slots are reserved globally and per host; one is
// reserved per owner. Queue reservations keep control admissible under bulk
// or exec saturation. Weights govern dispatch counts, not execution time.
type QoSConfig struct {
	BulkBytesPerSecond int64 `json:"bulk_bytes_per_second,omitempty"`
	MaxActive          int   `json:"max_active,omitempty"`
	PerHost            int   `json:"per_host,omitempty"`
	PerOwner           int   `json:"per_owner,omitempty"`
	MaxQueued          int   `json:"max_queued,omitempty"`
	PerOwnerQueued     int   `json:"per_owner_queued,omitempty"`
}

func (c QoSConfig) effective() QoSConfig {
	if c.BulkBytesPerSecond == 0 {
		c.BulkBytesPerSecond = 8 << 20
	}
	if c.MaxActive == 0 {
		c.MaxActive = 12
	}
	if c.PerHost == 0 {
		c.PerHost = 11
	}
	if c.PerOwner == 0 {
		c.PerOwner = 4
	}
	if c.MaxQueued == 0 {
		c.MaxQueued = 256
	}
	if c.PerOwnerQueued == 0 {
		c.PerOwnerQueued = 32
	}
	return c
}
func (c QoSConfig) validate() error {
	c = c.effective()
	if c.BulkBytesPerSecond < 1<<20 || c.BulkBytesPerSecond > 1<<30 {
		return errors.New("bulk_bytes_per_second must be between 1048576 and 1073741824")
	}
	if c.MaxActive < 3 || c.MaxActive > 1024 || c.PerHost < 3 || c.PerHost > c.MaxActive || c.PerOwner < 2 || c.PerOwner >= c.PerHost || c.MaxQueued < 8 || c.MaxQueued > 4096 || c.PerOwnerQueued < 2 || c.PerOwnerQueued >= c.MaxQueued {
		return errors.New("invalid qos limits: require 3 <= per_host <= max_active <= 1024, 2 <= per_owner < per_host, 8 <= max_queued <= 4096, 2 <= per_owner_queued < max_queued")
	}
	return nil
}

type WorkCount struct {
	Active int `json:"active"`
	Queued int `json:"queued"`
}
type schedulerCount struct {
	WorkCount
	nonControlActive, nonControlQueued int
}

type SchedulerSnapshot struct {
	BulkPayloadBytes uint64    `json:"bulk_payload_bytes"`
	Limits           QoSConfig `json:"limits"`
	// All counts and durations are scoped to the authenticated owner. Do not
	// expose other owners, host aliases or workload timing through status.
	WorkCount
	Lanes          map[Lane]WorkCount `json:"lanes"`
	Started        uint64             `json:"started"`
	Rejected       uint64             `json:"rejected"`
	Canceled       uint64             `json:"canceled"`
	QueueWaitNS    int64              `json:"queue_wait_ns"`
	MaxQueueWaitNS int64              `json:"max_queue_wait_ns"`
}

type scheduledItem struct {
	ctx         context.Context
	cancel      context.CancelFunc
	owner, host string
	lane        Lane
	fn          func(context.Context) (*proto.Response, error)
	result      chan dispatchResult
	enqueued    time.Time
}
type dispatchResult struct {
	resp *proto.Response
	err  error
}
type ownerStats struct {
	bulkBytes                   uint64
	started, rejected, canceled uint64
	wait, maxWait               int64
}

// Scheduler selects only eligible work and reserves quota atomically with
// dequeue. No worker is occupied waiting for someone else's quota or lane.
// Every state change schedules directly under mu: there are no wake tokens to
// lose or to deliver to an ineligible waiter.
type Scheduler struct {
	bulkReady  time.Time
	bulkTimer  *time.Timer
	mu         sync.Mutex
	limits     QoSConfig
	maxHosts   int
	weights    map[string]int
	queues     map[Lane]*FairQueue
	lanes      map[Lane]schedulerCount
	owners     map[string]schedulerCount
	hosts      map[string]schedulerCount
	ownerLanes map[string]map[Lane]WorkCount
	total      schedulerCount
	running    map[*scheduledItem]bool
	stats      map[string]ownerStats
	closed     bool
	done       chan struct{}
}

var schedulerLanes = []Lane{LaneControl, LaneExec, LaneBulk}
var laneLimits = map[Lane]int{LaneControl: 2, LaneExec: 8, LaneBulk: 1}

func NewScheduler(c QoSConfig, maxHosts int) *Scheduler {
	return &Scheduler{limits: c.effective(), maxHosts: maxHosts, weights: make(map[string]int), queues: map[Lane]*FairQueue{LaneControl: NewFairQueue(), LaneExec: NewFairQueue(), LaneBulk: NewFairQueue()}, lanes: make(map[Lane]schedulerCount), owners: make(map[string]schedulerCount), hosts: make(map[string]schedulerCount), ownerLanes: make(map[string]map[Lane]WorkCount), running: make(map[*scheduledItem]bool), stats: make(map[string]ownerStats), done: make(chan struct{})}
}
func (s *Scheduler) Configure(c QoSConfig, maxHosts int, weights map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits, s.maxHosts = c.effective(), maxHosts
	s.weights = make(map[string]int, len(weights))
	for owner, w := range weights {
		s.weights[owner] = w
	}
	for _, q := range s.queues {
		q.SetWeights(s.weights)
	}
	s.scheduleLocked()
}
func (s *Scheduler) SetOwnerWeight(owner string, weight int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if weight < 1 {
		weight = 1
	}
	if weight > 100 {
		weight = 100
	}
	s.weights[owner] = weight
	for _, q := range s.queues {
		q.SetWeights(s.weights)
	}
	s.scheduleLocked()
}
func (s *Scheduler) Snapshot(owner string) SchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := SchedulerSnapshot{Limits: s.limits, WorkCount: s.owners[owner].WorkCount, Lanes: make(map[Lane]WorkCount)}
	for lane, c := range s.ownerLanes[owner] {
		out.Lanes[lane] = c
	}
	st := s.stats[owner]
	out.BulkPayloadBytes = st.bulkBytes
	out.Started, out.Rejected, out.Canceled, out.QueueWaitNS, out.MaxQueueWaitNS = st.started, st.rejected, st.canceled, st.wait, st.maxWait
	return out
}

// Runtime statistics are bounded, and never keep owners alive in the queues.
// When the history budget fills, retain only owners with active/pending work.
func (s *Scheduler) statLocked(owner string) ownerStats {
	if len(s.stats) >= 1024 {
		for key := range s.stats {
			if c := s.owners[key]; c.Active+c.Queued == 0 {
				delete(s.stats, key)
			}
		}
	}
	return s.stats[owner]
}
func (s *Scheduler) Do(ctx context.Context, host, owner string, lane Lane, fn func(context.Context) (*proto.Response, error)) (*proto.Response, error) {
	if owner == "" || s.queues[lane] == nil {
		return nil, errors.New("owner and valid lane required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	item := &scheduledItem{ctx: runCtx, cancel: cancel, owner: owner, host: host, lane: lane, fn: fn, result: make(chan dispatchResult, 1), enqueued: time.Now()}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		cancel()
		return nil, ErrClosed
	}
	c := s.owners[owner]
	blocked := s.total.Queued >= s.limits.MaxQueued || c.Queued >= s.limits.PerOwnerQueued
	if lane != LaneControl {
		blocked = blocked || s.total.nonControlQueued >= s.limits.MaxQueued-s.limits.MaxQueued/8 || c.nonControlQueued >= s.limits.PerOwnerQueued-max(1, s.limits.PerOwnerQueued/8)
	}
	if blocked {
		st := s.statLocked(owner)
		st.rejected++
		s.stats[owner] = st
		s.mu.Unlock()
		cancel()
		return nil, ErrQueueFull
	}
	s.changeLocked(item, 0, 1)
	s.queues[lane].Enqueue(owner, item, s.weights[owner])
	s.scheduleLocked()
	s.mu.Unlock()
	select {
	case out := <-item.result:
		return out.resp, out.err
	case <-ctx.Done():
		s.mu.Lock()
		if s.queues[lane].RemoveIf(func(v any) bool { return v == item }) != 0 {
			s.changeLocked(item, 0, -1)
			st := s.statLocked(owner)
			st.canceled++
			s.stats[owner] = st
		}
		s.scheduleLocked()
		s.mu.Unlock()
		cancel()
		return nil, ctx.Err()
	}
}
func adjustCount(c schedulerCount, item *scheduledItem, active, queued int) schedulerCount {
	c.Active += active
	c.Queued += queued
	if item.lane != LaneControl {
		c.nonControlActive += active
		c.nonControlQueued += queued
	}
	return c
}
func (s *Scheduler) changeLocked(item *scheduledItem, active, queued int) {
	s.total = adjustCount(s.total, item, active, queued)
	s.lanes[item.lane] = adjustCount(s.lanes[item.lane], item, active, queued)
	s.owners[item.owner] = adjustCount(s.owners[item.owner], item, active, queued)
	s.hosts[item.host] = adjustCount(s.hosts[item.host], item, active, queued)
	if s.ownerLanes[item.owner] == nil {
		s.ownerLanes[item.owner] = make(map[Lane]WorkCount)
	}
	c := s.ownerLanes[item.owner][item.lane]
	c.Active += active
	c.Queued += queued
	s.ownerLanes[item.owner][item.lane] = c
	if c := s.owners[item.owner]; c.Active+c.Queued == 0 {
		delete(s.owners, item.owner)
		delete(s.ownerLanes, item.owner)
	}
	if c := s.hosts[item.host]; c.Active+c.Queued == 0 {
		delete(s.hosts, item.host)
	}
}
func (s *Scheduler) eligibleLocked(item *scheduledItem) bool {
	owner, host := s.owners[item.owner], s.hosts[item.host]
	if s.total.Active >= s.limits.MaxActive || host.Active >= s.limits.PerHost || owner.Active >= s.limits.PerOwner {
		return false
	}
	// max_hosts bounds hosts executing broker work. Warm-pool eviction is a
	// separate connection-manager concern; do not use it as a handler limit.
	if host.Active == 0 {
		activeHosts := 0
		for _, c := range s.hosts {
			if c.Active > 0 {
				activeHosts++
			}
		}
		if activeHosts >= s.maxHosts {
			return false
		}
	}
	if item.lane == LaneControl {
		// A single owner cannot monopolize both reserved control workers.
		return s.ownerLanes[item.owner][LaneControl].Active < 1
	}
	return s.total.nonControlActive < s.limits.MaxActive-2 && host.nonControlActive < s.limits.PerHost-2 && owner.nonControlActive < s.limits.PerOwner-1
}
func (s *Scheduler) scheduleLocked() {
	if s.closed {
		return
	}
	for _, lane := range schedulerLanes {
		q := s.queues[lane]
		q.RemoveIf(func(v any) bool {
			item := v.(*scheduledItem)
			if err := item.ctx.Err(); err != nil {
				s.changeLocked(item, 0, -1)
				st := s.statLocked(item.owner)
				st.canceled++
				s.stats[item.owner] = st
				item.cancel()
				item.result <- dispatchResult{err: err}
				return true
			}
			return false
		})
		if lane == LaneBulk && time.Now().Before(s.bulkReady) {
			if s.bulkTimer == nil {
				s.bulkTimer = time.AfterFunc(time.Until(s.bulkReady), func() { s.mu.Lock(); s.bulkTimer = nil; s.scheduleLocked(); s.mu.Unlock() })
			}
			continue
		}
		for s.lanes[lane].Active < laneLimits[lane] {
			value, ok := q.NextEligible(func(v any) bool { return s.eligibleLocked(v.(*scheduledItem)) })
			if !ok {
				break
			}
			item := value.(*scheduledItem)
			s.changeLocked(item, 1, -1)
			s.running[item] = true
			wait := time.Since(item.enqueued).Nanoseconds()
			st := s.statLocked(item.owner)
			st.started++
			st.wait += wait
			st.maxWait = max(st.maxWait, wait)
			s.stats[item.owner] = st
			go s.run(item)
		}
	}
}
func (s *Scheduler) run(item *scheduledItem) {
	started := time.Now()
	var resp *proto.Response
	err := item.ctx.Err()
	if err == nil {
		resp, err = item.fn(item.ctx)
	}
	item.cancel()
	s.mu.Lock()
	if item.lane == LaneBulk && resp != nil {
		var payload uint64
		if resp.Read != nil {
			payload += uint64(len(resp.Read.Content))
		}
		if resp.Cat != nil && resp.Cat.BytesWritten > 0 {
			payload += uint64(resp.Cat.BytesWritten)
		}
		st := s.statLocked(item.owner)
		st.bulkBytes += payload
		s.stats[item.owner] = st
		// Charge actual application payload. One bounded response may burst; delay
		// the next bulk admission without holding a worker or a transport lease.
		s.bulkReady = started.Add(time.Duration(payload) * time.Second / time.Duration(s.limits.BulkBytesPerSecond))
	}
	delete(s.running, item)
	s.changeLocked(item, -1, 0)
	s.scheduleLocked()
	if s.closed && len(s.running) == 0 {
		close(s.done)
	}
	s.mu.Unlock()
	item.result <- dispatchResult{resp: resp, err: err}
}
func (s *Scheduler) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		if s.bulkTimer != nil {
			s.bulkTimer.Stop()
		}
		for _, q := range s.queues {
			q.RemoveIf(func(v any) bool {
				item := v.(*scheduledItem)
				s.changeLocked(item, 0, -1)
				item.cancel()
				item.result <- dispatchResult{err: ErrClosed}
				return true
			})
		}
		for item := range s.running {
			item.cancel()
		}
		if len(s.running) == 0 {
			close(s.done)
		}
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
