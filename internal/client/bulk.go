package client

import (
	"context"
	"time"

	"github.com/CIPFZ/rdev/internal/session"
)

type bulkConnection struct {
	pooled    pooledConnection
	active    int
	idleSince time.Time
}

func (c *Client) leasedBulkConn(ctx context.Context, host, target string) (pooledConnection, session.State, func(), error) {
	// The base transport bootstraps and initializes secrets once. Keep its
	// immutable identity lease through bulk setup, I/O and response redaction.
	base, st, releaseIdentity, err := c.leasedConnForTarget(ctx, host, target)
	if err != nil {
		return pooledConnection{}, session.State{}, nil, err
	}
	name := base.conn.Host().Name
	lock := c.dialLock("bulk\x00" + name)
	select {
	case lock <- struct{}{}:
	case <-ctx.Done():
		releaseIdentity()
		return pooledConnection{}, session.State{}, nil, ctx.Err()
	}
	defer func() { <-lock }()
	c.mu.Lock()
	entry := c.bulkConns[name]
	if entry != nil && (entry.pooled.generation != base.generation || entry.pooled.fingerprint != base.fingerprint || entry.pooled.connectionFingerprint != base.connectionFingerprint) {
		delete(c.bulkConns, name)
		c.mu.Unlock()
		_ = entry.pooled.conn.Close()
		entry = nil
		c.mu.Lock()
	}
	if entry != nil {
		entry.active++
		entry.idleSince = time.Time{}
		c.mu.Unlock()
		return entry.pooled, st, c.bulkRelease(entry, releaseIdentity), nil
	}
	c.mu.Unlock()
	conn, err := c.dial(ctx, base.conn.Host(), c.lookup)
	if err != nil {
		releaseIdentity()
		return pooledConnection{}, session.State{}, nil, err
	}
	base.conn, base.bulk = conn, true
	entry = &bulkConnection{pooled: base, active: 1}
	c.mu.Lock()
	c.bulkConns[name] = entry
	c.mu.Unlock()
	return entry.pooled, st, c.bulkRelease(entry, releaseIdentity), nil
}
func (c *Client) bulkRelease(entry *bulkConnection, releaseIdentity func()) func() {
	return func() {
		c.mu.Lock()
		entry.active--
		if entry.active == 0 {
			entry.idleSince = time.Now()
		}
		c.mu.Unlock()
		releaseIdentity()
	}
}

// ReapBulkIdle atomically detaches only idle bulk generations. Admission
// increments active under the same mutex; closing an old transport happens
// outside it and cannot remove its replacement or alter base security state.
func (c *Client) ReapBulkIdle(now time.Time, ttl time.Duration) int {
	c.mu.Lock()
	var retired []*bulkConnection
	for name, entry := range c.bulkConns {
		if entry.active == 0 && !entry.idleSince.IsZero() && now.Sub(entry.idleSince) >= ttl {
			delete(c.bulkConns, name)
			retired = append(retired, entry)
		}
	}
	c.mu.Unlock()
	for _, entry := range retired {
		_ = entry.pooled.conn.Close()
	}
	return len(retired)
}
