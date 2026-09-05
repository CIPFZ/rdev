package broker

import (
	"context"
	"sync"
)

// RequestRegistry tracks broker-local cancellation without owning the shared
// transport. Disconnecting one owner only cancels that owner's requests.
type RequestRegistry struct {
	mu       sync.Mutex
	requests map[string]context.CancelFunc
}

func NewRequestRegistry() *RequestRegistry {
	return &RequestRegistry{requests: make(map[string]context.CancelFunc)}
}

func (r *RequestRegistry) Add(id string, parent context.Context) (context.Context, bool) {
	if id == "" {
		return nil, false
	}
	ctx, cancel := context.WithCancel(parent)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.requests[id]; exists {
		cancel()
		return nil, false
	}
	r.requests[id] = cancel
	return ctx, true
}

func (r *RequestRegistry) Remove(id string) {
	r.mu.Lock()
	if cancel := r.requests[id]; cancel != nil {
		delete(r.requests, id)
		cancel()
	}
	r.mu.Unlock()
}
func (r *RequestRegistry) CancelOwner(ids []string) {
	for _, id := range ids {
		r.Remove(id)
	}
}
