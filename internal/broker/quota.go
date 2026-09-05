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
}

func NewQuota(host, perClient, queue int) *Quota {
	return &Quota{host: host, perClient: perClient, queued: queue, owners: make(map[string]int)}
}

func (q *Quota) Acquire(ctx context.Context, owner string) error {
	if owner == "" {
		return errors.New("owner required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.active >= q.host || q.owners[owner] >= q.perClient {
		return ErrQueueFull
	}
	q.active++
	q.owners[owner]++
	return nil
}

func (q *Quota) Release(owner string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.active > 0 {
		q.active--
	}
	if q.owners[owner] > 0 {
		q.owners[owner]--
	}
}
