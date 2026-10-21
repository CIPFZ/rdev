package observe

import (
	"context"
	"fmt"
)

// Tracer receives coarse lifecycle phases of one CLI operation: host
// resolution, stdin, ssh/agent setup, request sent, result received. It exists
// so a stuck command can say where it is stuck (RDEV_DEBUG=1, rdev -v).
//
// Phases are fixed labels plus host aliases, addresses, ops and sizes. Argv,
// env, payloads and remote stderr never reach a tracer.
type Tracer func(phase string)

type tracerKey struct{}

// WithTracer attaches fn to ctx. A nil fn returns ctx unchanged.
func WithTracer(ctx context.Context, fn Tracer) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, tracerKey{}, fn)
}

// Trace reports one phase when ctx carries a tracer, and is free otherwise.
func Trace(ctx context.Context, format string, args ...any) {
	if ctx == nil {
		return
	}
	fn, _ := ctx.Value(tracerKey{}).(Tracer)
	if fn == nil {
		return
	}
	if len(args) == 0 {
		fn(format)
		return
	}
	fn(fmt.Sprintf(format, args...))
}

// TraceDialStage reports a transport dial stage by its closed-vocabulary name.
func TraceDialStage(ctx context.Context, stage DialStage) {
	if stage < dialStageCount {
		Trace(ctx, "ssh dial: %s", dialStageNames[stage])
	}
}
