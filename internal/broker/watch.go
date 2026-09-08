package broker

import (
	"encoding/json"
	"sync"
)

const maxWatchKeys = 1024
const maxWatchSubscribers = 512
const maxWatchEventBytes = 64 << 10

type WatchHub struct {
	mu          sync.Mutex
	watchers    map[string]map[chan any]struct{}
	latest      map[string][]byte
	order       []string
	subscribers int
}

func NewWatchHub() *WatchHub {
	return &WatchHub{watchers: make(map[string]map[chan any]struct{}), latest: make(map[string][]byte)}
}

func (h *WatchHub) Subscribe(job string) (<-chan any, func()) {
	ch := make(chan any, 8)
	h.mu.Lock()
	if len(job) > 1024 || h.subscribers >= maxWatchSubscribers {
		h.mu.Unlock()
		close(ch)
		return ch, func() {}
	}
	h.subscribers++
	if h.watchers[job] == nil {
		h.watchers[job] = make(map[chan any]struct{})
	}
	h.watchers[job][ch] = struct{}{}
	if data, ok := h.latest[job]; ok {
		var event any
		if json.Unmarshal(data, &event) == nil {
			ch <- event
		}
	}
	h.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			if set := h.watchers[job]; set != nil {
				delete(set, ch)
				if len(set) == 0 {
					delete(h.watchers, job)
				}
			}
			close(ch)
			h.subscribers--
			h.mu.Unlock()
		})
	}
}

func (h *WatchHub) Publish(job string, event any) {
	if len(job) > 1024 {
		return
	}
	data, err := json.Marshal(event)
	if err != nil || len(data) > maxWatchEventBytes {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.latest[job]; !exists {
		h.order = append(h.order, job)
		if len(h.order) > maxWatchKeys {
			delete(h.latest, h.order[0])
			h.order = h.order[1:]
		}
	}
	h.latest[job] = data
	for ch := range h.watchers[job] {
		var copy any
		if json.Unmarshal(data, &copy) != nil {
			continue
		}
		select {
		case ch <- copy:
		default:
		}
	}
}
func (h *WatchHub) Watching(job string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.watchers[job])
}
