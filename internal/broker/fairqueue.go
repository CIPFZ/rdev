package broker

import "sync"

// FairQueue serves eligible owners round-robin with configurable positive
// weights. Empty/canceled owner queues are removed, including their credits.
type FairQueue struct {
	mu      sync.Mutex
	queues  map[string][]any
	order   []string
	weight  map[string]int
	cursor  int
	credits map[string]int
}

func NewFairQueue() *FairQueue {
	return &FairQueue{queues: make(map[string][]any), weight: make(map[string]int), credits: make(map[string]int)}
}
func (q *FairQueue) Enqueue(owner string, value any, weight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.queues[owner]; !ok {
		q.order = append(q.order, owner)
	}
	q.setWeight(owner, weight)
	q.queues[owner] = append(q.queues[owner], value)
}
func (q *FairQueue) setWeight(owner string, weight int) {
	if weight < 1 {
		weight = 1
	}
	if q.weight[owner] != weight {
		q.weight[owner] = weight
		q.credits[owner] = 0
	}
}
func (q *FairQueue) SetWeights(weights map[string]int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, owner := range q.order {
		q.setWeight(owner, weights[owner])
	}
}
func (q *FairQueue) removeOwner(index int) {
	owner := q.order[index]
	delete(q.queues, owner)
	delete(q.weight, owner)
	delete(q.credits, owner)
	copy(q.order[index:], q.order[index+1:])
	q.order[len(q.order)-1] = ""
	q.order = q.order[:len(q.order)-1]
	if index < q.cursor {
		q.cursor--
	}
	if q.cursor >= len(q.order) {
		q.cursor = 0
	}
}
func (q *FairQueue) Next() (any, bool) { return q.NextEligible(func(any) bool { return true }) }

// NextEligible scans beyond an ineligible owner's head (including other hosts
// for that owner), without spending that owner's credits. The predicate must
// not call back into the queue.
func (q *FairQueue) NextEligible(eligible func(any) bool) (any, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for n := 0; n < len(q.order); n++ {
		owner := q.order[q.cursor]
		for i, item := range q.queues[owner] {
			if !eligible(item) {
				continue
			}
			if q.credits[owner] <= 0 {
				q.credits[owner] = q.weight[owner]
			}
			items := q.queues[owner]
			copy(items[i:], items[i+1:])
			items[len(items)-1] = nil
			q.queues[owner] = items[:len(items)-1]
			q.credits[owner]--
			if len(q.queues[owner]) == 0 {
				q.removeOwner(q.cursor)
			} else if q.credits[owner] == 0 {
				q.cursor = (q.cursor + 1) % len(q.order)
			}
			return item, true
		}
		q.cursor = (q.cursor + 1) % len(q.order)
	}
	return nil, false
}
func (q *FairQueue) RemoveIf(remove func(any) bool) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	count := 0
	for i := 0; i < len(q.order); {
		owner := q.order[i]
		items := q.queues[owner]
		kept := items[:0]
		for _, item := range items {
			if remove(item) {
				count++
			} else {
				kept = append(kept, item)
			}
		}
		clear(items[len(kept):])
		q.queues[owner] = kept
		if len(kept) == 0 {
			q.removeOwner(i)
		} else {
			i++
		}
	}
	return count
}
