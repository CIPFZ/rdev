package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

const maxBrokerRequestBytes = proto.AbsoluteRequestFrameBytes + (16 << 10)
const brokerFrameTimeout = 5 * time.Second

// boundedBrokerDecoder preserves concatenated/pretty JSON compatibility and
// pipelined hello bytes, while discarding each decoder's large scratch buffer
// after a document. Encoded bytes remain charged until the handler releases it.
type boundedBrokerDecoder struct {
	conn      net.Conn
	lease     *broker.IngressLease
	pending   []byte
	deadline  time.Time
	lastBytes int64
}

type ingressReader struct {
	decoder *boundedBrokerDecoder
	started bool
}

func (r *ingressReader) Read(p []byte) (int, error) {
	if len(p) > 4096 {
		p = p[:4096]
	}
	if err := r.decoder.lease.Reserve(int64(len(p))); err != nil {
		return 0, err
	}
	n, err := r.decoder.conn.Read(p)
	r.decoder.lease.Release(int64(len(p) - n))
	if n > 0 && !r.started && len(bytes.TrimLeft(p[:n], " \r\n\t")) > 0 {
		r.started = true
		deadline := time.Now().Add(brokerFrameTimeout)
		if !r.decoder.deadline.IsZero() && r.decoder.deadline.Before(deadline) {
			deadline = r.decoder.deadline
		}
		_ = r.decoder.conn.SetReadDeadline(deadline)
	}
	return n, err
}

func (d *boundedBrokerDecoder) Decode(out any, limit int64) (func(), error) {
	// Encoder delimiters from the preceding document are not a new partial
	// frame. Otherwise every idle authenticated connection would time out
	// while an unrelated long-running request was still executing.
	prior := bytes.TrimLeft(d.pending, " \r\n\t")
	d.lease.Release(int64(len(d.pending) - len(prior)))
	pending := bytes.NewReader(prior)
	source := &ingressReader{decoder: d}
	if len(prior) > 0 {
		source.started = true
		deadline := time.Now().Add(brokerFrameTimeout)
		if !d.deadline.IsZero() && d.deadline.Before(deadline) {
			deadline = d.deadline
		}
		_ = d.conn.SetReadDeadline(deadline)
	}
	input := &io.LimitedReader{R: io.MultiReader(pending, source), N: limit + 1}
	dec := json.NewDecoder(input)
	if err := dec.Decode(out); err != nil {
		return nil, err
	}
	consumed := dec.InputOffset()
	d.lastBytes = consumed
	if consumed > limit {
		return nil, errors.New("broker JSON document exceeds size limit")
	}
	tail, err := io.ReadAll(dec.Buffered())
	if err != nil {
		return nil, err
	}
	// The decoder need not have consumed the full previous prefetch buffer.
	tail = append(tail, prior[len(prior)-pending.Len():]...)
	d.pending = tail
	_ = d.conn.SetReadDeadline(d.deadline)
	var once sync.Once
	return func() { once.Do(func() { d.lease.Release(consumed) }) }, nil
}
