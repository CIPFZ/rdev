package client

import (
	"context"
	"errors"
	"time"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/session"
)

type bulkConnection struct {
	pooled    pooledConnection
	active    int
	idleSince time.Time
}

func (c *Client) leasedBulkConn(ctx context.Context, host, target string) (pooledConnection, session.State, func(), error) {
	for {
		pooled, state, release, err := c.leasedBulkConnAttempt(ctx, host, target)
		if errors.Is(err, errDialIdentityChanged) {
			continue
		}
		return pooled, state, release, err
	}
}

func (c *Client) leasedBulkConnAttempt(ctx context.Context, host, target string) (pooledConnection, session.State, func(), error) {
	changed := c.dialControl.changes()
	// The base transport bootstraps and initializes secrets once. Capture its
	// publication, then reacquire the identity after every admission wait.
	base, st, releaseIdentity, err := c.leasedConnForTarget(ctx, host, target)
	if err != nil {
		return pooledConnection{}, session.State{}, nil, err
	}
	name := base.conn.Host().Name
	lock := c.dialLock("bulk\x00" + name)
	releaseIdentity()
	if hook := c.dialControl.testBeforeBulkLockWait; hook != nil {
		hook()
	}
	if err := c.dialControl.waitSetupLock(ctx, lock); err != nil {
		return pooledConnection{}, session.State{}, nil, err
	}
	defer func() { <-lock }()
	releaseIdentity, valid := c.Hosts.AcquireIdentity(name, base.generation, base.fingerprint)
	if !valid {
		return pooledConnection{}, session.State{}, nil, errDialIdentityChanged
	}
	c.mu.Lock()
	current, published := c.conns[name]
	if !published || current.conn != base.conn || current.publication != base.publication {
		c.mu.Unlock()
		releaseIdentity()
		return pooledConnection{}, session.State{}, nil, errDialIdentityChanged
	}
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
		st = c.Hosts.State(name)
		return entry.pooled, st, c.bulkRelease(entry, releaseIdentity), nil
	}
	c.mu.Unlock()
	// Waiting for backoff/global dial capacity must not pin host policy or its
	// redaction generation. Reacquire and validate the exact base publication
	// once admitted, before any SSH setup or business request.
	releaseIdentity()
	permit, err := c.dialControl.reserve(ctx, base.conn.Host(), changed)
	if err != nil {
		return pooledConnection{}, session.State{}, nil, err
	}
	releaseIdentity, valid = c.Hosts.AcquireIdentity(name, base.generation, base.fingerprint)
	if !valid {
		permit.finish(false, nil)
		return pooledConnection{}, session.State{}, nil, errDialIdentityChanged
	}
	c.mu.Lock()
	current, published = c.conns[name]
	c.mu.Unlock()
	if !published || current.conn != base.conn || current.publication != base.publication {
		permit.finish(false, nil)
		releaseIdentity()
		return pooledConnection{}, session.State{}, nil, errDialIdentityChanged
	}
	if target != "" {
		current, err := c.ProtocolTargetIdentity(host)
		if err != nil || current != target {
			permit.finish(false, nil)
			releaseIdentity()
			return pooledConnection{}, session.State{}, nil, errors.New("approved target changed before bulk connection setup")
		}
	}
	st = c.Hosts.State(name)
	conn, err := c.dial(observe.WithConnectionAggregate(ctx, &c.dialControl.activity), base.conn.Host(), c.lookup)
	setupRejection := canonicalSetupRejection(err)
	permit.finish(ctx.Err() == nil && setupRejection == nil, err)
	if err != nil {
		releaseIdentity()
		if setupRejection != nil {
			return pooledConnection{}, session.State{}, nil, setupRejection
		}
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

// DetachIdleBulk removes one idle bulk generation and returns its cleanup.
// Broker pool accounting keeps this host reserved until cleanup completes.
func (c *Client) DetachIdleBulk(host string, now time.Time, ttl time.Duration) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.bulkConns[host]
	if entry == nil || entry.active != 0 || entry.idleSince.IsZero() || now.Sub(entry.idleSince) < ttl {
		return nil
	}
	delete(c.bulkConns, host)
	return func() { _ = entry.pooled.conn.Close() }
}
