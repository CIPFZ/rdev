package connmgr

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeConn struct{ closed atomic.Int32 }

func (f *fakeConn) Close() error { f.closed.Add(1); return nil }

type blockingCloseConn struct {
	closed  atomic.Int32
	started chan struct{}
	release chan struct{}
}

type gracefulConn struct {
	closed, graceful atomic.Int32
}

func (f *gracefulConn) GracefulClose(context.Context) error { f.graceful.Add(1); return nil }
func (f *gracefulConn) Close() error                        { f.closed.Add(1); return nil }

func (f *blockingCloseConn) Close() error {
	f.closed.Add(1)
	close(f.started)
	<-f.release
	return nil
}

func TestAcquireSingleflightAndLeaseCounts(t *testing.T) {
	m := New(Config{})
	var dials atomic.Int32
	gate := make(chan struct{})
	var wg sync.WaitGroup
	leases := make(chan *Lease, 2)
	dial := func(context.Context) (Connection, error) { dials.Add(1); <-gate; return &fakeConn{}, nil }
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := m.Acquire(context.Background(), "k", dial)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			leases <- l
		}()
	}
	time.Sleep(10 * time.Millisecond)
	close(gate)
	wg.Wait()
	close(leases)
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials=%d, want 1", got)
	}
	s := m.Snapshot("k")
	if s.State != Warm || s.Lease != 2 {
		t.Fatalf("snapshot=%+v", s)
	}
	for l := range leases {
		l.Release()
	}
	if m.Snapshot("k").Lease != 0 {
		t.Fatal("lease leak")
	}
}

func TestEvictionNeverClosesBusyAndDrainsRace(t *testing.T) {
	m := New(Config{})
	c := &fakeConn{}
	l, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	if m.Evict("k", "idle") {
		t.Fatal("evicted leased connection")
	}
	if s := m.Snapshot("k"); s.State != Draining {
		t.Fatalf("state=%v", s.State)
	}
	// A new request cancels draining and safely shares the connection.
	l2, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { t.Fatal("redial"); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	l2.Release()
	if c.closed.Load() != 0 {
		t.Fatal("busy connection closed")
	}
	if err := m.Begin("k"); err != nil {
		t.Fatal(err)
	}
	if err := m.End("k"); err != nil {
		t.Fatal(err)
	}
	if !m.Evict("k", "idle") {
		t.Fatal("idle eviction failed")
	}
	if c.closed.Load() != 1 {
		t.Fatalf("close count=%d", c.closed.Load())
	}
	if s := m.Snapshot("k"); s.State != Cold {
		t.Fatalf("state=%v", s.State)
	}
}

func TestQueueBoundedAndInflightAccounting(t *testing.T) {
	m := New(Config{MaxQueue: 1})
	_, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return &fakeConn{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	release, err := m.Queue("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Queue("k"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v", err)
	}
	if err := m.Begin("k"); err != nil {
		t.Fatal(err)
	}
	release()
	if s := m.Snapshot("k"); s.Queued != 0 || s.Inflight != 1 {
		t.Fatalf("snapshot=%+v", s)
	}
	_ = m.End("k")
}

func TestAcquireWaitsForEvictionBeforeRedial(t *testing.T) {
	m := New(Config{})
	c := &blockingCloseConn{started: make(chan struct{}), release: make(chan struct{})}
	initial, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	initial.Release()
	// The connection is idle, so Evict starts Close outside the state lock.
	evicted := make(chan bool, 1)
	go func() { evicted <- m.Evict("k", "test") }()
	select {
	case <-c.started:
	case <-time.After(time.Second):
		t.Fatal("eviction did not start closing")
	}

	var dials atomic.Int32
	acquired := make(chan *Lease, 1)
	acquireErr := make(chan error, 1)
	go func() {
		l, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) {
			dials.Add(1)
			return &fakeConn{}, nil
		})
		if err != nil {
			acquireErr <- err
			return
		}
		acquired <- l
	}()
	select {
	case <-acquired:
		t.Fatal("acquire redialed before old close completed")
	case <-time.After(20 * time.Millisecond):
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials=%d while eviction close blocked", got)
	}
	close(c.release)
	if !<-evicted {
		t.Fatal("eviction failed")
	}
	select {
	case err := <-acquireErr:
		t.Fatal(err)
	case l := <-acquired:
		if l == nil || l.Conn() == nil {
			t.Fatal("acquire returned an empty lease")
		}
		l.Release()
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials=%d after eviction, want 1", got)
	}
}

func TestAcquireRejectsNilContextAndNilConnection(t *testing.T) {
	m := New(Config{})
	if _, err := m.Acquire(nil, "k", func(context.Context) (Connection, error) { return &fakeConn{}, nil }); err == nil {
		t.Fatal("nil context unexpectedly accepted")
	}
	if _, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return nil, nil }); err == nil {
		t.Fatal("nil connection unexpectedly accepted")
	}
	if got := m.Snapshot("k"); got.State != Backoff || got.LastError == "" {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestMaxWarmHostsEvictsLeastRecentlyUsedIdle(t *testing.T) {
	clock := time.Unix(100, 0)
	m := New(Config{MaxWarmHosts: 2, IdleTTL: time.Hour, Now: func() time.Time { return clock }})
	var conns []*fakeConn
	acquire := func(key string) *Lease {
		c := &fakeConn{}
		conns = append(conns, c)
		l, err := m.Acquire(context.Background(), key, func(context.Context) (Connection, error) { return c, nil })
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	l1 := acquire("a")
	l1.Release()
	clock = clock.Add(time.Second)
	l2 := acquire("b")
	l2.Release()
	clock = clock.Add(time.Second)
	l3 := acquire("c")
	l3.Release()
	if conns[0].closed.Load() != 1 {
		t.Fatalf("oldest connection close count=%d", conns[0].closed.Load())
	}
	warm := 0
	for _, s := range m.Snapshots() {
		if s.State == Warm {
			warm++
		}
	}
	if warm != 2 {
		t.Fatalf("warm snapshots=%d, want 2", warm)
	}
}

func TestReapUsesLastClientGraceAndGracefulClose(t *testing.T) {
	clock := time.Unix(200, 0)
	m := New(Config{IdleTTL: time.Hour, LastClientGrace: 10 * time.Second, DrainTimeout: time.Second, Now: func() time.Time { return clock }})
	c := &gracefulConn{}
	l, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	clock = clock.Add(9 * time.Second)
	if n := m.Reap(clock); n != 0 {
		t.Fatalf("reaped early: %d", n)
	}
	clock = clock.Add(time.Second)
	if n := m.Reap(clock); n != 1 {
		t.Fatalf("reaped=%d, want 1", n)
	}
	if c.graceful.Load() != 1 || c.closed.Load() != 1 {
		t.Fatalf("close graceful=%d close=%d", c.graceful.Load(), c.closed.Load())
	}
}

func TestDrainingClosesAfterQueueLeaves(t *testing.T) {
	m := New(Config{})
	c := &fakeConn{}
	l, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	release, err := m.Queue("k")
	if err != nil {
		t.Fatal(err)
	}
	if m.Evict("k", "test") {
		t.Fatal("busy queue was evicted synchronously")
	}
	l.Release()
	release()
	if c.closed.Load() != 1 {
		t.Fatalf("close count=%d, want 1", c.closed.Load())
	}
}

func TestConfigValidation(t *testing.T) {
	for _, cfg := range []Config{
		{MaxQueue: -1},
		{MaxWarmHosts: -1},
		{IdleTTL: -time.Second},
		{IdleTTL: 10 * time.Second, MaxIdleTTL: time.Second},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("Validate(%+v) unexpectedly succeeded", cfg)
		}
	}
	if err := (Config{MaxWarmHosts: 2, IdleTTL: time.Second, MaxIdleTTL: 2 * time.Second}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
