package main

import (
	"context"
	"encoding/json"
	"net"
	"time"
)

const brokerWriteTimeout = 2 * time.Second

// A slow reader may retain at most one response per admitted connection for
// this interval. Failed writes also cancel active connection-owned work.
type boundedBrokerEncoder struct {
	conn     net.Conn
	cancel   context.CancelFunc
	deadline time.Time
}

func (e *boundedBrokerEncoder) Encode(value any) error {
	deadline := time.Now().Add(brokerWriteTimeout)
	if !e.deadline.IsZero() && e.deadline.Before(deadline) {
		deadline = e.deadline
	}
	_ = e.conn.SetWriteDeadline(deadline)
	err := json.NewEncoder(e.conn).Encode(value)
	if err != nil {
		e.cancel()
		_ = e.conn.Close()
	}
	return err
}
