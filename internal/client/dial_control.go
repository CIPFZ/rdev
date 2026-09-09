package client

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

const maxConcurrentDials = 6
const maxDialStates = 1024
const minimumDialBackoff = 500 * time.Millisecond
const maximumDialBackoff = 30 * time.Second

// Every Client in this process shares the SSH setup budget. The authoritative
// broker normally owns one Client; standalone and embedded callers cannot each
// silently multiply that process budget.
var processDialSlots = make(chan struct{}, maxConcurrentDials)
var errDialIdentityChanged = errors.New("connection identity changed while waiting for dial admission")

type dialState struct {
	epoch    uint64
	refs     int
	busy     chan struct{}
	failures uint8
	retryAt  time.Time
	used     uint64
}

type dialControl struct {
	metrics  dialWaitMetrics
	activity observe.ConnectionActivity
	mu       sync.Mutex
	states   map[string]*dialState
	slots    chan struct{}
	changed  chan struct{}
	seq      uint64
	now      func() time.Time
	jitter   func(time.Duration, time.Duration) time.Duration
	wait     func(context.Context, time.Duration, <-chan struct{}) error
	// Tests observe admission barriers without relying on scheduler sleeps.
	testBeforeSlotWait     func()
	testBeforeBulkLockWait func()
	testWaitStarted        func(bool)
}

func newDialControl(slots chan struct{}) *dialControl {
	return &dialControl{
		states: make(map[string]*dialState), slots: slots, changed: make(chan struct{}), now: time.Now,
		jitter: func(lo, hi time.Duration) time.Duration { return lo + time.Duration(rand.Int64N(int64(hi-lo)+1)) },
		wait:   waitDialBackoff,
	}
}

func waitDialBackoff(ctx context.Context, delay time.Duration, changed <-chan struct{}) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return errDialIdentityChanged
	case <-timer.C:
		return nil
	}
}

func (d *dialControl) changes() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.changed
}

func (d *dialControl) identityChanged(host *transport.Host, reset bool) {
	key := ""
	if host != nil && reset {
		key, _ = canonicalDialKey(*host)
	}
	d.mu.Lock()
	if entry := d.states[key]; entry != nil {
		entry.epoch++
		entry.failures, entry.retryAt = 0, time.Time{}
	}
	close(d.changed)
	d.changed = make(chan struct{})
	d.mu.Unlock()
}

// The key excludes alias and lane. It retains SSH user, port and namespace;
// distinct SSH-config aliases cannot be assumed to resolve to the same machine.
// Hashing keeps internal bookkeeping bounded independently of display names.
func canonicalDialKey(host transport.Host) (string, error) {
	host, err := transport.NormalizeHost(host)
	if err != nil {
		return "", err
	}
	dir, err := transport.ValidateRemoteDir(host.RemoteDir)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", host.Addr, host.Port, dir)))), nil
}

type dialPermit struct {
	control *dialControl
	state   *dialState
	epoch   uint64
	slot    bool
}

// reserve waits before the caller acquires a registry identity lease. A host
// update can therefore wake backoff/queue waiters without waiting for 30 seconds
// of stale connection setup. Actual SSH admission always rechecks that identity.
func (d *dialControl) reserve(ctx context.Context, host transport.Host, changed <-chan struct{}) (*dialPermit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := canonicalDialKey(host)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	entry := d.states[key]
	if entry == nil {
		if len(d.states) >= maxDialStates {
			var oldest string
			var used uint64
			for key, candidate := range d.states {
				// Never discard an active failure window to admit a new target.
				if candidate.refs == 0 && candidate.busy == nil && !d.now().Before(candidate.retryAt) && (oldest == "" || candidate.used < used) {
					oldest, used = key, candidate.used
				}
			}
			if oldest == "" {
				d.mu.Unlock()
				return nil, proto.NewError(proto.CodeQueueFull, "", proto.StateNotSent)
			}
			delete(d.states, oldest)
		}
		entry = &dialState{epoch: 1}
		d.states[key] = entry
	}
	entry.refs++
	d.seq++
	entry.used = d.seq
	d.mu.Unlock()
	owned := false
	defer func() {
		if !owned {
			d.mu.Lock()
			entry.refs--
			d.mu.Unlock()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		select {
		case <-changed:
			return nil, errDialIdentityChanged
		default:
		}
		d.mu.Lock()
		if busy := entry.busy; busy != nil {
			d.mu.Unlock()
			finished := d.beginWait(false)
			select {
			case <-ctx.Done():
				finished()
				return nil, ctx.Err()
			case <-changed:
				finished()
				return nil, errDialIdentityChanged
			case <-busy:
				finished()
				continue
			}
		}
		if delay := entry.retryAt.Sub(d.now()); delay > 0 {
			d.mu.Unlock()
			finished := d.beginWait(true)
			err := d.wait(ctx, delay, changed)
			finished()
			if err != nil {
				return nil, err
			}
			continue
		}
		entry.busy = make(chan struct{})
		epoch := entry.epoch
		d.mu.Unlock()
		permit := &dialPermit{control: d, state: entry, epoch: epoch}
		owned = true
		select {
		case d.slots <- struct{}{}:
			permit.slot = true
		default:
			finished := d.beginWait(false)
			if d.testBeforeSlotWait != nil {
				d.testBeforeSlotWait()
			}
			select {
			case <-ctx.Done():
				finished()
				permit.finish(false, nil)
				return nil, ctx.Err()
			case <-changed:
				finished()
				permit.finish(false, nil)
				return nil, errDialIdentityChanged
			case d.slots <- struct{}{}:
				permit.slot = true
				finished()
			}
		}
		if err := ctx.Err(); err != nil {
			permit.finish(false, nil)
			return nil, err
		}
		select {
		case <-changed:
			permit.finish(false, nil)
			return nil, errDialIdentityChanged
		default:
		}
		return permit, nil
	}
}

// finish records only actual failed connection attempts, never business
// requests. It does not retry a dial or a mutation. Caller cancellation neither
// poisons another owner nor erases previous failure history.
func (p *dialPermit) finish(attempted bool, err error) {
	d := p.control
	if p.slot {
		<-d.slots
	}
	d.mu.Lock()
	e := p.state
	if attempted && p.epoch == e.epoch {
		if err == nil {
			e.failures, e.retryAt = 0, time.Time{}
		} else {
			if e.failures < 7 {
				e.failures++
			}
			base := min(maximumDialBackoff, minimumDialBackoff<<(e.failures-1))
			lo, hi := max(minimumDialBackoff, base*3/4), min(maximumDialBackoff, base*5/4)
			e.retryAt = d.now().Add(d.jitter(lo, hi))
		}
	}
	close(e.busy)
	e.busy = nil
	e.refs--
	d.mu.Unlock()
}
