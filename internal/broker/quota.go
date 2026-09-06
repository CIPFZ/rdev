package broker

import (
	"context"
	"errors"
	"sync"
)

var ErrQueueFull = errors.New("broker request queue is full")

// Quota bounds total host work and each owner's share.
type Quota struct {
	mu                      sync.Mutex
	host, perClient, queued int
	active                  int
	owners                  map[string]int
	hosts                   map[string]int
	notify                  chan struct{}
	waiting                 int
}

func NewQuota(host, perClient, queue int) *Quota {
	return &Quota{host: host, perClient: perClient, queued: queue, owners: make(map[string]int), hosts: make(map[string]int), notify: make(chan struct{}, 1)}
}

func (q *Quota) Acquire(ctx context.Context, owner string) error {
	return q.AcquireHost(ctx, "", owner)
}

func (q *Quota) AcquireHost(ctx context.Context, host, owner string) error {
	if owner == "" {
		return errors.New("owner required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.active >= q.host || q.owners[owner] >= q.perClient || (host != "" && q.hosts[host] >= q.host) {
		return ErrQueueFull
	}
	q.active++
	q.owners[owner]++
	if host != "" {
		q.hosts[host]++
	}
	return nil
}

func (q *Quota) AcquireHostContext(ctx context.Context, host, owner string) error {
	for {
		err := q.AcquireHost(ctx, host, owner)
		if err == nil {
			return nil
		}
		if err != ErrQueueFull {
			return err
		}
		q.mu.Lock()
		if q.waiting >= q.queued {
			q.mu.Unlock()
			return ErrQueueFull
		}
		q.waiting++
		q.mu.Unlock()
		select {
		case <-q.notify:
		case <-ctx.Done():
			q.mu.Lock()
			q.waiting--
			q.mu.Unlock()
			return ctx.Err()
		}
		q.mu.Lock()
		q.waiting--
		q.mu.Unlock()
	}
}

func (q *Quota) Release(owner string) {
	q.ReleaseHost("", owner)
}

func (q *Quota) ReleaseHost(host, owner string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.active > 0 {
		q.active--
	}
	if q.owners[owner] > 0 {
		q.owners[owner]--
	}
	if host != "" && q.hosts[host] > 0 {
		q.hosts[host]--
	}
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
