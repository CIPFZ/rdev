package connmgr

import (
	"context"
	"errors"
	"fmt"
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

func TestMaxWarmHostsCountsConnectionDuringClose(t *testing.T) {
	m := New(Config{MaxWarmHosts: 1})
	c := &blockingCloseConn{started: make(chan struct{}), release: make(chan struct{})}
	l, err := m.Acquire(context.Background(), "a", func(context.Context) (Connection, error) { return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	evicted := make(chan bool, 1)
	go func() { evicted <- m.Evict("a", "test") }()
	select {
	case <-c.started:
	case <-time.After(time.Second):
		t.Fatal("eviction did not start closing")
	}
	var dialed atomic.Int32
	if _, err := m.Acquire(context.Background(), "b", func(context.Context) (Connection, error) {
		dialed.Add(1)
		return &fakeConn{}, nil
	}); !errors.Is(err, ErrWarmLimit) {
		t.Fatalf("acquire while close pending: %v, want ErrWarmLimit", err)
	}
	if dialed.Load() != 0 {
		t.Fatal("dial started while the warm-host slot was still closing")
	}
	close(c.release)
	if !<-evicted {
		t.Fatal("eviction failed")
	}
	l2, err := m.Acquire(context.Background(), "b", func(context.Context) (Connection, error) { return &fakeConn{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	l2.Release()
}

func TestMaxWarmHostsCountsDialingConnection(t *testing.T) {
	m := New(Config{MaxWarmHosts: 1})
	started := make(chan struct{})
	release := make(chan struct{})
	first := make(chan *Lease, 1)
	go func() {
		l, err := m.Acquire(context.Background(), "a", func(context.Context) (Connection, error) {
			close(started)
			<-release
			return &fakeConn{}, nil
		})
		if err != nil {
			t.Errorf("first acquire: %v", err)
			return
		}
		first <- l
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	var dialed atomic.Int32
	if _, err := m.Acquire(context.Background(), "b", func(context.Context) (Connection, error) {
		dialed.Add(1)
		return &fakeConn{}, nil
	}); !errors.Is(err, ErrWarmLimit) {
		t.Fatalf("acquire while another dial is pending: %v, want ErrWarmLimit", err)
	}
	if dialed.Load() != 0 {
		t.Fatal("second dial started while warm-host slot was occupied")
	}
	close(release)
	l := <-first
	l.Release()
}

func TestBeginDrainingDoesNotConsumeQueue(t *testing.T) {
	m := New(Config{})
	l, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return &fakeConn{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	release, err := m.Queue("k")
	if err != nil {
		t.Fatal(err)
	}
	if m.Evict("k", "busy") {
		t.Fatal("busy connection was evicted synchronously")
	}
	if err := m.Begin("k"); !errors.Is(err, ErrDraining) {
		t.Fatalf("Begin on draining connection: %v", err)
	}
	if got := m.Snapshot("k").Queued; got != 1 {
		t.Fatalf("queued=%d after rejected Begin, want 1", got)
	}
	release()
	l.Release()
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

func TestGlobalDialSemaphoreBoundsHosts(t *testing.T) {
	m := New(Config{MaxConcurrentDials: 2, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	var active, peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l, err := m.Acquire(context.Background(), fmt.Sprintf("h-%d", i), func(context.Context) (Connection, error) {
				n := active.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				started <- struct{}{}
				<-release
				active.Add(-1)
				return &fakeConn{}, nil
			})
			if err == nil {
				l.Release()
			} else {
				t.Errorf("acquire: %v", err)
			}
		}(i)
	}
	for i := 0; i < 2; i++ {
		<-started
	}
	if got := peak.Load(); got > 2 {
		t.Fatalf("peak concurrent dials=%d", got)
	}
	close(release)
	wg.Wait()
}

func TestCanceledSemaphoreLeaderDoesNotPoisonWaiters(t *testing.T) {
	m := New(Config{MaxConcurrentDials: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond})
	blockStarted := make(chan struct{})
	blockRelease := make(chan struct{})
	blockDone := make(chan struct{})
	go func() {
		defer close(blockDone)
		l, err := m.Acquire(context.Background(), "block", func(context.Context) (Connection, error) {
			close(blockStarted)
			<-blockRelease
			return &fakeConn{}, nil
		})
		if err != nil {
			t.Errorf("blocking acquire: %v", err)
			return
		}
		l.Release()
	}()
	<-blockStarted

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	leaderDone := make(chan error, 1)
	go func() {
		_, err := m.Acquire(ctx, "target", func(context.Context) (Connection, error) {
			return nil, errors.New("canceled leader unexpectedly reached dial")
		})
		leaderDone <- err
	}()

	var dials atomic.Int32
	waiterDone := make(chan *Lease, 1)
	waiterErr := make(chan error, 1)
	go func() {
		l, err := m.Acquire(context.Background(), "target", func(context.Context) (Connection, error) {
			dials.Add(1)
			return &fakeConn{}, nil
		})
		if err != nil {
			waiterErr <- err
			return
		}
		waiterDone <- l
	}()

	if err := <-leaderDone; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("leader err=%v, want deadline exceeded", err)
	}
	close(blockRelease)
	<-blockDone
	select {
	case err := <-waiterErr:
		t.Fatalf("waiter failed after leader cancellation: %v", err)
	case l := <-waiterDone:
		if l == nil {
			t.Fatal("waiter returned nil lease")
		}
		l.Release()
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("waiter dials=%d, want one takeover dial", got)
	}
}

func TestDialSemaphoreFIFO(t *testing.T) {
	s := newDialSemaphore(1)
	if err := s.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	order := make(chan int, 2)
	go func() {
		if err := s.acquire(context.Background()); err != nil {
			t.Errorf("acquire 0: %v", err)
			return
		}
		order <- 0
		s.release()
	}()
	// Wait until the first caller is queued before starting the second, so the
	// expected FIFO order is independent of goroutine scheduling.
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		queued := len(s.waiters)
		s.mu.Unlock()
		if queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiters did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	go func() {
		if err := s.acquire(context.Background()); err != nil {
			t.Errorf("acquire 1: %v", err)
			return
		}
		order <- 1
		s.release()
	}()
	for {
		s.mu.Lock()
		queued := len(s.waiters)
		s.mu.Unlock()
		if queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second waiter did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	s.release()
	if got := <-order; got != 0 {
		t.Fatalf("first waiter=%d, want 0", got)
	}
	if got := <-order; got != 1 {
		t.Fatalf("second waiter=%d, want 1", got)
	}
}

func TestDialFailureBackoffAndCancellation(t *testing.T) {
	clock := time.Unix(100, 0)
	m := New(Config{BaseBackoff: time.Second, MaxBackoff: 4 * time.Second, Now: func() time.Time { return clock }})
	var dials atomic.Int32
	_, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) {
		dials.Add(1)
		return nil, errors.New("connection refused")
	})
	if err == nil || dials.Load() != 1 {
		t.Fatalf("first dial err=%v dials=%d", err, dials.Load())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err = m.Acquire(ctx, "k", func(context.Context) (Connection, error) { dials.Add(1); return &fakeConn{}, nil }); err == nil || err.Error() != "connection refused" {
		t.Fatalf("backoff err=%v", err)
	}
	if dials.Load() != 1 {
		t.Fatalf("redial during backoff: %d", dials.Load())
	}
	clock = clock.Add(time.Second)
	if _, err = m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { dials.Add(1); return &fakeConn{}, nil }); err != nil || dials.Load() != 2 {
		t.Fatalf("recovery err=%v dials=%d", err, dials.Load())
	}
}

func TestConcurrentDialFailureIsShared(t *testing.T) {
	m := New(Config{BaseBackoff: time.Second, MaxBackoff: time.Second})
	var dials atomic.Int32
	dial := func(context.Context) (Connection, error) {
		dials.Add(1)
		return nil, errors.New("connection refused")
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _, err := m.Acquire(context.Background(), "same", dial); errs <- err }()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err == nil || err.Error() != "connection refused" {
			t.Fatalf("shared err=%v", err)
		}
	}
	if dials.Load() != 1 {
		t.Fatalf("dials=%d, want one singleflight attempt", dials.Load())
	}
}

func TestBackoffJitterUsesDeterministicClockAndRandom(t *testing.T) {
	clock := time.Unix(300, 0)
	m := New(Config{BaseBackoff: 10 * time.Second, MaxBackoff: 20 * time.Second, Jitter: true,
		Now: func() time.Time { return clock }, Random: func() float64 { return 1 }})
	_, _ = m.Acquire(context.Background(), "jitter", func(context.Context) (Connection, error) {
		return nil, errors.New("network unavailable")
	})
	s := m.Snapshot("jitter")
	if want := clock.Add(15 * time.Second); !s.RetryAt.Equal(want) {
		t.Fatalf("retry at=%v, want deterministic jitter at %v", s.RetryAt, want)
	}
}
