package broker

import "sync"

type WatchHub struct {
	mu       sync.Mutex
	watchers map[string]map[chan any]struct{}
	latest   map[string]any
}

func NewWatchHub() *WatchHub {
	return &WatchHub{watchers: make(map[string]map[chan any]struct{}), latest: make(map[string]any)}
}

func (h *WatchHub) Subscribe(job string) (<-chan any, func()) {
	ch := make(chan any, 8)
	h.mu.Lock()
	if h.watchers[job] == nil {
		h.watchers[job] = make(map[chan any]struct{})
	}
	h.watchers[job][ch] = struct{}{}
	if event, ok := h.latest[job]; ok {
		ch <- event
	}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		if set := h.watchers[job]; set != nil {
			delete(set, ch)
			if len(set) == 0 {
				delete(h.watchers, job)
			}
		}
		close(ch)
		h.mu.Unlock()
	}
}

func (h *WatchHub) Publish(job string, event any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.latest[job] = event
	for ch := range h.watchers[job] {
		select {
		case ch <- event:
		default:
		}
	}
}
func (h *WatchHub) Watching(job string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.watchers[job])
}
