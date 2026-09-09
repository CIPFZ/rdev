package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

type dialWaitBarrier struct {
	delay   time.Duration
	proceed chan struct{}
}
type dialTestClock struct {
	nanos atomic.Int64
	waits chan dialWaitBarrier
}

func installDialClock(d *dialControl) *dialTestClock {
	clock := &dialTestClock{waits: make(chan dialWaitBarrier, 16)}
	clock.nanos.Store(time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC).UnixNano())
	d.now = func() time.Time { return time.Unix(0, clock.nanos.Load()) }
	d.jitter = func(lo, hi time.Duration) time.Duration { return hi }
	d.wait = func(ctx context.Context, delay time.Duration, changed <-chan struct{}) error {
		barrier := dialWaitBarrier{delay: delay, proceed: make(chan struct{})}
		clock.waits <- barrier
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			return errDialIdentityChanged
		case <-barrier.proceed:
			clock.nanos.Add(int64(delay))
			return nil
		}
	}
	return clock
}

func awaitDialWait(t *testing.T, clock *dialTestClock) dialWaitBarrier {
	t.Helper()
	select {
	case wait := <-clock.waits:
		return wait
	case <-time.After(time.Second):
		t.Fatal("backoff barrier not reached")
		return dialWaitBarrier{}
	}
}

func TestDialBackoffGrowthCancellationResetAndCanonicalAliases(t *testing.T) {
	d := newDialControl(make(chan struct{}, 6))
	clock := installDialClock(d)
	host := transport.Host{Name: "a", Addr: "u@[2001:db8::1]:2222", RemoteDir: "~/.cache/rdev"}
	alias := transport.Host{Name: "b", Addr: "u@2001:db8::1", Port: 2222, RemoteDir: ".cache/rdev"}
	for attempt := 0; attempt < 9; attempt++ {
		permit, err := d.reserve(t.Context(), host, d.changes())
		if err != nil {
			t.Fatal(err)
		}
		permit.finish(true, context.DeadlineExceeded) // an internal dial timeout is a real failure
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			p, err := d.reserve(ctx, alias, d.changes())
			if err == nil {
				p.finish(false, nil)
			}
			done <- err
		}()
		wait := awaitDialWait(t, clock)
		if wait.delay < minimumDialBackoff || wait.delay > maximumDialBackoff {
			t.Fatalf("backoff outside bounds: %s", wait.delay)
		}
		if attempt == 0 && wait.delay > time.Second {
			t.Fatal("initial backoff is not bounded near 500ms")
		}
		if attempt >= 6 && wait.delay != maximumDialBackoff {
			t.Fatalf("capped backoff=%s", wait.delay)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("backoff cancellation: %v", err)
		}
		clock.nanos.Add(int64(wait.delay))
	}
	if len(d.states) != 1 {
		t.Fatal("alias/platform spelling split canonical state")
	}
	// A newer trusted registry generation may retry immediately; an old alias
	// cannot reset the failure history of that generation afterwards.
	d.identityChanged(&alias, true)
	permit, err := d.reserve(t.Context(), alias, d.changes())
	if err != nil {
		t.Fatal(err)
	}
	permit.finish(true, errors.New("network unavailable"))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		p, err := d.reserve(ctx, host, d.changes())
		if err == nil {
			p.finish(false, nil)
		}
		done <- err
	}()
	wait := awaitDialWait(t, clock)
	if wait.delay > time.Second {
		t.Fatal("new generation did not reset previous failures")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	clock.nanos.Add(int64(wait.delay))
	permit, err = d.reserve(t.Context(), alias, d.changes())
	if err != nil {
		t.Fatal(err)
	}
	permit.finish(true, nil)
	permit, err = d.reserve(t.Context(), alias, d.changes())
	if err != nil {
		t.Fatal(err)
	}
	permit.finish(false, nil)
	if len(d.slots) != 0 {
		t.Fatal("admission leaked a global dial slot")
	}
}

func TestProcessDialLimitIncludesBaseBulkAndIndependentClients(t *testing.T) {
	baseClient, bulkClient := newTestClient(), newTestClient()
	defer baseClient.Close()
	defer bulkClient.Close()
	for i := 0; i < 6; i++ {
		h := transport.Host{Name: fmt.Sprintf("warm%d", i), Addr: fmt.Sprintf("warm%d.invalid", i)}
		if err := bulkClient.Hosts.Add(h); err != nil {
			t.Fatal(err)
		}
	}
	bulkClient.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: h}, nil
	}
	for i := 0; i < 6; i++ {
		if _, err := bulkClient.conn(t.Context(), fmt.Sprintf("warm%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	var active, peak, calls atomic.Int32
	entered := make(chan struct{}, 12)
	release := make(chan struct{})
	dial := func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		n := active.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		calls.Add(1)
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return &fakeRemoteConn{host: h}, nil
	}
	baseClient.dial, bulkClient.dial = dial, dial
	done := make(chan error, 12)
	for i := 0; i < 6; i++ {
		i := i
		go func() { _, err := baseClient.conn(t.Context(), fmt.Sprintf("cold%d.invalid", i)); done <- err }()
		go func() {
			_, _, cleanup, err := bulkClient.leasedBulkConn(t.Context(), fmt.Sprintf("warm%d", i), "")
			if err == nil {
				cleanup()
			}
			done <- err
		}()
	}
	for range 6 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("six admitted dials did not start")
		}
	}
	// The seventh caller proves real cancellation while all process slots are
	// held by other clients/lanes. No fake clock or delay replaces this queue.
	queuedClient := newTestClient()
	defer queuedClient.Close()
	queued := make(chan struct{})
	queuedClient.dialControl.testBeforeSlotWait = func() { close(queued) }
	queuedClient.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		t.Error("canceled queued caller dialed")
		return nil, errors.New("unexpected")
	}
	ctx, cancel := context.WithCancel(t.Context())
	canceled := make(chan error, 1)
	go func() { _, err := queuedClient.conn(ctx, "queued.invalid"); canceled <- err }()
	<-queued
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued cancellation: %v", err)
	}
	close(release)
	for range 12 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if peak.Load() != 6 || calls.Load() != 12 || len(processDialSlots) != 0 {
		t.Fatalf("process dial budget: peak=%d calls=%d slots=%d", peak.Load(), calls.Load(), len(processDialSlots))
	}
}

func TestHostUpdateInterruptsBackoffWithoutHoldingIdentity(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		for _, approved := range []bool{false, true} {
			t.Run(fmt.Sprintf("bulk=%t/approved=%t", bulk, approved), func(t *testing.T) {
				c := newTestClient()
				defer c.Close()
				clock := installDialClock(c.dialControl)
				if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "old.invalid"}); err != nil {
					t.Fatal(err)
				}
				var oldCalls, newCalls atomic.Int32
				c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
					if h.Addr == "old.invalid" {
						n := oldCalls.Add(1)
						if !bulk || n > 1 {
							return nil, errors.New("old host unavailable")
						}
					} else {
						newCalls.Add(1)
					}
					return &fakeRemoteConn{host: h}, nil
				}
				target := ""
				if approved {
					var err error
					target, err = c.ProtocolTargetIdentity("dev")
					if err != nil {
						t.Fatal(err)
					}
				}
				call := func(ctx context.Context) error {
					if bulk {
						_, _, release, err := c.leasedBulkConn(ctx, "dev", target)
						if err == nil {
							release()
						}
						return err
					}
					_, err := c.connForTarget(ctx, "dev", target)
					return err
				}
				if err := call(t.Context()); err == nil {
					t.Fatal("unavailable first dial succeeded")
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- call(ctx) }()
				awaitDialWait(t, clock)
				updated := make(chan error, 1)
				go func() { updated <- c.Hosts.Add(transport.Host{Name: "dev", Addr: "new.invalid"}) }()
				select {
				case err := <-updated:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("backoff held the identity lease against host update")
				}
				select {
				case err := <-done:
					if (err != nil) != approved {
						t.Fatalf("approved=%t result=%v", approved, err)
					}
				case <-time.After(time.Second):
					t.Fatal("host update did not wake backoff")
				}
				if approved && newCalls.Load() != 0 {
					t.Fatal("changed approved target dialed")
				}
				wantOld := int32(1)
				if bulk {
					wantOld = 2
				}
				if oldCalls.Load() != wantOld {
					t.Fatal("old endpoint redialed after update")
				}
			})
		}
	}
}

func TestDialFailureStateBudgetDoesNotEvictActiveBackoff(t *testing.T) {
	d := newDialControl(make(chan struct{}, 6))
	clock := installDialClock(d)
	for i := 0; i < maxDialStates; i++ {
		p, err := d.reserve(t.Context(), transport.Host{Addr: fmt.Sprintf("h%d.invalid", i)}, d.changes())
		if err != nil {
			t.Fatal(err)
		}
		p.finish(true, errors.New("network failure"))
	}
	_, err := d.reserve(t.Context(), transport.Host{Addr: "overflow.invalid"}, d.changes())
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeQueueFull || len(d.states) != maxDialStates {
		t.Fatalf("state budget failed closed incorrectly: %v entries=%d", err, len(d.states))
	}
	clock.nanos.Add(int64(time.Minute))
	p, err := d.reserve(t.Context(), transport.Host{Addr: "overflow.invalid"}, d.changes())
	if err != nil {
		t.Fatal(err)
	}
	p.finish(true, nil)
	if len(d.states) != maxDialStates {
		t.Fatal("expired bookkeeping did not stay bounded")
	}
}

func TestAddingAliasCannotResetCanonicalFailure(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	clock := installDialClock(c.dialControl)
	if err := c.Hosts.Add(transport.Host{Name: "first", Addr: "u@same.invalid"}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		calls.Add(1)
		return nil, errors.New("network unavailable")
	}
	if _, err := c.conn(t.Context(), "first"); err == nil {
		t.Fatal("failed host connected")
	}
	// Registry generations differ here: both names still refer to the same
	// target. A new alias is not an explicit change to the failing identity.
	if err := c.Hosts.Add(transport.Host{Name: "second", Addr: "u@same.invalid"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := c.conn(ctx, "second"); done <- err }()
	awaitDialWait(t, clock)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if calls.Load() != 1 || len(c.dialControl.states) != 1 {
		t.Fatal("alias creation bypassed canonical backoff")
	}
}

func TestBulkAliasAdmissionDoesNotHoldIdentity(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "old.invalid"}); err != nil {
		t.Fatal(err)
	}
	var dials atomic.Int32
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		dials.Add(1)
		return &fakeRemoteConn{host: h}, nil
	}
	if _, err := c.conn(t.Context(), "dev"); err != nil {
		t.Fatal(err)
	}
	target, err := c.ProtocolTargetIdentity("dev")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	lock := c.dialLock("bulk\x00dev")
	lock <- struct{}{}
	locked := true
	defer func() {
		if locked {
			<-lock
		}
	}()
	waiting := make(chan struct{}, 1)
	c.dialControl.testBeforeBulkLockWait = func() { waiting <- struct{}{} }
	done := make(chan error, 1)
	go func() {
		_, _, release, err := c.leasedBulkConn(ctx, "dev", target)
		if err == nil {
			release()
		}
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("bulk caller did not reach alias admission")
	}
	updated := make(chan error, 1)
	go func() { updated <- c.Hosts.Add(transport.Host{Name: "dev", Addr: "new.invalid"}) }()
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("bulk alias queue pinned old host identity")
	}
	<-lock
	locked = false
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("changed approval target connected")
		}
	case <-time.After(time.Second):
		t.Fatal("bulk caller did not revalidate target after alias admission")
	}
	if dials.Load() != 1 {
		t.Fatal("changed approved target dialed")
	}
}
