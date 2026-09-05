package broker

import "sync/atomic"

type Readiness struct{ ready atomic.Bool }

func (r *Readiness) SetReady(v bool) { r.ready.Store(v) }
func (r *Readiness) Ready() bool     { return r.ready.Load() }
