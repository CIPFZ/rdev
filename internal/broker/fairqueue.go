package broker

import "sync"

type queuedRequest struct {
	Owner string
	Value any
}

// FairQueue serves owners round-robin with configurable positive weights.
type FairQueue struct {
	mu      sync.Mutex
	queues  map[string][]queuedRequest
	order   []string
	weight  map[string]int
	cursor  int
	credits map[string]int
}

func NewFairQueue() *FairQueue {
	return &FairQueue{queues: make(map[string][]queuedRequest), weight: make(map[string]int), credits: make(map[string]int)}
}

func (q *FairQueue) Enqueue(owner string, value any, weight int) {
	if weight < 1 {
		weight = 1
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.queues[owner]; !ok {
		q.order = append(q.order, owner)
		q.weight[owner] = weight
	}
	q.queues[owner] = append(q.queues[owner], queuedRequest{owner, value})
}

func (q *FairQueue) Next() (any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return nil, false
	}
	for n := 0; n < len(q.order); n++ {
		owner := q.order[q.cursor%len(q.order)]
		if len(q.queues[owner]) == 0 {
			q.cursor++
			continue
		}
		if q.credits[owner] <= 0 {
			q.credits[owner] = q.weight[owner]
		}
		item := q.queues[owner][0]
		q.queues[owner] = q.queues[owner][1:]
		q.credits[owner]--
		if q.credits[owner] == 0 {
			q.cursor++
		}
		return item.Value, true
	}
	return nil, false
}
