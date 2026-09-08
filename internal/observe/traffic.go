package observe

import (
	"context"
	"sync/atomic"
)

// Traffic contains only byte counters. It may outlive a request while canceled
// streams drain; it never retains request parameters or caller-controlled labels.
type Traffic struct {
	Sent     atomic.Uint64
	Received atomic.Uint64
}

type TrafficSnapshot struct {
	SentBytes     uint64 `json:"sent_bytes"`
	ReceivedBytes uint64 `json:"received_bytes"`
}

func (t *Traffic) Snapshot() TrafficSnapshot {
	if t == nil {
		return TrafficSnapshot{}
	}
	return TrafficSnapshot{SentBytes: t.Sent.Load(), ReceivedBytes: t.Received.Load()}
}

type trafficKey struct{}

func WithTraffic(ctx context.Context, t *Traffic) context.Context {
	return context.WithValue(ctx, trafficKey{}, t)
}
func TrafficFromContext(ctx context.Context) *Traffic {
	t, _ := ctx.Value(trafficKey{}).(*Traffic)
	return t
}
