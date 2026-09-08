package observe

import (
	"context"
	"sync/atomic"
	"time"
)

// DialStage is a closed vocabulary. Remote stderr, host/owner names and paths
// never become diagnostic labels.
type DialStage uint8

const (
	DialValidation DialStage = iota
	DialControlPath
	DialProbe
	DialLookup
	DialInstall
	DialAgentStart
	DialHandshake
	DialNegotiation
	DialCanceled
	dialStageCount
)

var dialStageNames = [...]string{"validation", "control_path", "probe", "agent_lookup", "agent_install", "agent_start", "handshake", "negotiation", "canceled"}

type ConnectionActivity struct {
	started, succeeded, retries atomic.Uint64
	inFlight                    atomic.Int64
	durationNS, maxDurationNS   atomic.Uint64
	failures                    [dialStageCount]atomic.Uint64
}

type ConnectionSnapshot struct {
	DialStarted       uint64            `json:"dial_started"`
	DialSucceeded     uint64            `json:"dial_succeeded"`
	DialInFlight      int64             `json:"dial_inflight"`
	DialDurationNS    uint64            `json:"dial_duration_ns"`
	MaxDialDurationNS uint64            `json:"max_dial_duration_ns"`
	RetryAttempts     uint64            `json:"retry_attempts"`
	DialFailures      map[string]uint64 `json:"dial_failures"`
}

func (a *ConnectionActivity) BeginDial() {
	if a != nil {
		a.started.Add(1)
		a.inFlight.Add(1)
	}
}
func (a *ConnectionActivity) EndDial(stage DialStage, succeeded bool, elapsed time.Duration) {
	if a == nil {
		return
	}
	if succeeded {
		a.succeeded.Add(1)
	} else if stage < dialStageCount {
		a.failures[stage].Add(1)
	}
	ns := uint64(max(0, elapsed.Nanoseconds()))
	a.durationNS.Add(ns)
	for old := a.maxDurationNS.Load(); ns > old; old = a.maxDurationNS.Load() {
		if a.maxDurationNS.CompareAndSwap(old, ns) {
			break
		}
	}
	a.inFlight.Add(-1)
}
func (a *ConnectionActivity) Retry() {
	if a != nil {
		a.retries.Add(1)
	}
}
func (a *ConnectionActivity) Snapshot() ConnectionSnapshot {
	out := ConnectionSnapshot{DialFailures: make(map[string]uint64, len(dialStageNames))}
	for _, name := range dialStageNames {
		out.DialFailures[name] = 0
	}
	if a == nil {
		return out
	}
	out.DialStarted, out.DialSucceeded, out.DialInFlight = a.started.Load(), a.succeeded.Load(), a.inFlight.Load()
	out.DialDurationNS, out.MaxDialDurationNS, out.RetryAttempts = a.durationNS.Load(), a.maxDurationNS.Load(), a.retries.Load()
	for i, name := range dialStageNames {
		out.DialFailures[name] = a.failures[i].Load()
	}
	return out
}

type connectionActivityKey struct{}

func WithConnectionActivity(ctx context.Context, a *ConnectionActivity) context.Context {
	return context.WithValue(ctx, connectionActivityKey{}, a)
}
func ConnectionActivityFromContext(ctx context.Context) *ConnectionActivity {
	a, _ := ctx.Value(connectionActivityKey{}).(*ConnectionActivity)
	return a
}
