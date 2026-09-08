package observe

import (
	"context"
	"sync/atomic"
)

// BulkPayload reports application payload consumed by an internal operation that
// does not return its remote response to the frontend (for example a secret
// import). It carries only counts and belongs to one scheduled request.
type BulkPayload struct {
	bytes    atomic.Uint64
	recorded atomic.Bool
}

func (p *BulkPayload) Snapshot() (uint64, bool) {
	recorded := p.recorded.Load()
	return p.bytes.Load(), recorded
}

type bulkPayloadKey struct{}

func WithBulkPayload(ctx context.Context, p *BulkPayload) context.Context {
	return context.WithValue(ctx, bulkPayloadKey{}, p)
}
func RecordBulkPayload(ctx context.Context, n uint64) {
	if p, _ := ctx.Value(bulkPayloadKey{}).(*BulkPayload); p != nil {
		p.bytes.Add(n)
		p.recorded.Store(true)
	}
}
