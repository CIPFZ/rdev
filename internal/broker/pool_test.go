package broker

import (
	"context"
	"errors"
	"github.com/CIPFZ/rdev/internal/proto"
	"sync"
	"testing"
	"time"
)

func awaitPool(t *testing.T, p *HostPool, f func(PoolHealth) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if f(p.Snapshot()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pool condition not met: %+v", p.Snapshot())
}
func poolLease(t *testing.T, p *HostPool, host string, lane Lane) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	release, err := p.Acquire(ctx, host, lane)
	if err != nil {
		t.Fatal(err)
	}
	return release
}
func TestHostPoolClosingSlotsCountAndColdFIFO(t *testing.T) {
	entered, finish := make(chan string, 4), make(chan struct{})
	p := NewHostPool(1, func(h string) func() { return func() { entered <- h; <-finish } })
	t.Cleanup(func() { close(finish); _ = p.Close(context.Background()) })
	first := poolLease(t, p, "a", LaneExec)
	type result struct {
		release func()
		err     error
	}
	cold := make(chan result, 1)
	go func() { r, e := p.Acquire(t.Context(), "b", LaneExec); cold <- result{r, e} }()
	awaitPool(t, p, func(h PoolHealth) bool { return h.Queued == 1 })
	hotCtx, cancelHot := context.WithCancel(t.Context())
	hot := make(chan result, 1)
	go func() { r, e := p.Acquire(hotCtx, "a", LaneExec); hot <- result{r, e} }()
	awaitPool(t, p, func(h PoolHealth) bool { return h.Queued == 2 })
	control := poolLease(t, p, "a", LaneControl)
	first()
	if p.Snapshot().ClosingHosts != 0 {
		t.Fatal("active control evicted")
	}
	control()
	if h := <-entered; h != "a" {
		t.Fatal("wrong generation retired")
	}
	if h := p.Snapshot(); h.ReservedHosts != 1 || h.ClosingHosts != 1 || h.Queued != 2 {
		t.Fatalf("closing slot released early: %+v", h)
	}
	cancelHot()
	if r := <-hot; !errors.Is(r.err, context.Canceled) {
		t.Fatal("queued warm cancellation lost")
	}
	// Permit the first closer only; cleanup of b is consumed by test cleanup.
	finish <- struct{}{}
	r := <-cold
	if r.err != nil {
		t.Fatal(r.err)
	}
	if h := p.Snapshot(); h.ReservedHosts != 1 || h.ActiveHosts != 1 || h.Queued != 0 {
		t.Fatalf("cold not admitted first: %+v", h)
	}
	r.release()
	r.release()
	if p.Snapshot().ActiveLeases != 0 {
		t.Fatal("duplicate release corrupted pool")
	}
}
func TestHostPoolLRUIdleReloadAndCancelAdmission(t *testing.T) {
	var mu sync.Mutex
	var retired []string
	p := NewHostPool(2, func(h string) func() { mu.Lock(); retired = append(retired, h); mu.Unlock(); return func() {} })
	defer p.Close(context.Background())
	a := poolLease(t, p, "a", LaneExec)
	a()
	b := poolLease(t, p, "b", LaneBulk)
	b()
	a = poolLease(t, p, "a", LaneExec)
	a()
	c := poolLease(t, p, "c", LaneExec)
	mu.Lock()
	if len(retired) != 1 || retired[0] != "b" {
		t.Errorf("not idle LRU: %v", retired)
	}
	mu.Unlock()
	p.Configure(1)
	awaitPool(t, p, func(h PoolHealth) bool { return h.ReservedHosts == 1 })
	if n := p.Reap(time.Now().Add(time.Hour), time.Second, "idle_ttl"); n != 0 {
		t.Fatal("active lease reaped")
	}
	c()
	if n := p.Reap(time.Now(), time.Second, "idle_ttl"); n != 0 {
		t.Fatal("TTL ignored")
	}
	if n := p.Reap(time.Now().Add(time.Hour), time.Second, "idle_ttl"); n != 1 {
		t.Fatal("idle not reaped")
	}
	awaitPool(t, p, func(h PoolHealth) bool { return h.ReservedHosts == 0 })
	h := p.Snapshot()
	if h.Evictions["capacity_lru"].Count != 1 || h.Evictions["capacity_reload"].Count != 1 || h.Evictions["idle_ttl"].Count != 1 {
		t.Fatalf("missing reason accounting: %+v", h)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := p.Acquire(ctx, "new", LaneExec); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled admission consumed a slot")
	}
}
func TestHostPoolShutdownIsBoundedAndRejectsWaiters(t *testing.T) {
	finish := make(chan struct{})
	p := NewHostPool(1, func(string) func() { return func() { <-finish } })
	release := poolLease(t, p, "a", LaneExec)
	waiter := make(chan error, 1)
	go func() { _, e := p.Acquire(t.Context(), "b", LaneExec); waiter <- e }()
	awaitPool(t, p, func(h PoolHealth) bool { return h.Queued == 1 })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close ignored blocked transport")
	}
	if err := <-waiter; !errors.Is(err, ErrClosed) {
		t.Fatal("shutdown left waiter pending")
	}
	if _, err := p.Acquire(t.Context(), "a", LaneControl); !errors.Is(err, ErrClosed) {
		t.Fatal("shutdown admitted control")
	}
	release()
	close(finish)
	if err := p.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHostPoolBulkCleanupRetainsCapacityAndBoundsShutdown(t *testing.T) {
	p := NewHostPool(1, func(string) func() { return func() {} })
	lease := poolLease(t, p, "a", LaneBulk)
	lease()
	entered, finish := make(chan struct{}), make(chan struct{})
	defer close(finish)
	detach := func(string, time.Time, time.Duration) func() { return func() { close(entered); <-finish } }
	if p.ReapBulk(time.Now(), time.Second, detach) != 1 {
		t.Fatal("idle bulk not detached")
	}
	<-entered
	if p.ReapBulk(time.Now(), time.Second, detach) != 0 {
		t.Fatal("unbounded duplicate detached closer")
	}
	if p.Reap(time.Now().Add(time.Hour), time.Second, "idle_ttl") != 0 {
		t.Fatal("bulk cleanup freed warm host early")
	}
	warm := poolLease(t, p, "a", LaneControl)
	warm()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := p.Acquire(ctx, "b", LaneExec); done <- err }()
	awaitPool(t, p, func(h PoolHealth) bool { return h.Queued == 1 && h.ClosingBulk == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	closeCtx, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	if err := p.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown released a still-closing bulk slot")
	}
	h := p.Snapshot()
	if h.ReservedHosts != 1 || h.ClosingBulk != 1 || h.ClosingHosts != 1 {
		t.Fatalf("missing detached bulk accounting: %+v", h)
	}
}

func TestSchedulerColdHostDoesNotConsumeWarmControlWorker(t *testing.T) {
	p := NewHostPool(1, func(string) func() { return func() {} })
	s := NewScheduler(QoSConfig{}, 128)
	s.SetHostPool(p)
	defer p.Close(context.Background())
	defer s.Close(context.Background())
	started, finish := make(chan struct{}), make(chan struct{})
	longDone := make(chan error, 1)
	go func() {
		_, err := s.Do(t.Context(), "warm", "a", LaneExec, func(ctx context.Context) (*proto.Response, error) {
			close(started)
			select {
			case <-finish:
			case <-ctx.Done():
			}
			return &proto.Response{OK: true}, nil
		})
		longDone <- err
	}()
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	coldDone := make(chan error, 1)
	go func() {
		_, err := s.Do(ctx, "cold", "b", LaneControl, func(context.Context) (*proto.Response, error) {
			t.Error("cold host started before warm released")
			return nil, nil
		})
		coldDone <- err
	}()
	awaitPool(t, p, func(h PoolHealth) bool { return h.Queued == 1 })
	controlCtx, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	_, err := s.Do(controlCtx, "warm", "b", LaneControl, func(ctx context.Context) (*proto.Response, error) {
		release, err := p.dispatchLease(ctx, "warm", LaneControl)
		if err != nil {
			return nil, err
		}
		defer release()
		if p.Snapshot().ActiveLeases != 2 {
			t.Error("scheduled dispatch acquired duplicate host lease")
		}
		return &proto.Response{OK: true}, nil
	})
	if err != nil {
		t.Fatal("cold control occupied worker needed for warm control", err)
	}
	cancel()
	if err := <-coldDone; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(finish)
	if err := <-longDone; err != nil {
		t.Fatal(err)
	}
	awaitPool(t, p, func(h PoolHealth) bool { return h.ActiveLeases == 0 && h.Queued == 0 })
}

func TestHostPoolColdDemandQuiescesOverlappingHotControl(t *testing.T) {
	p := NewHostPool(1, func(string) func() { return func() {} })
	defer p.Close(context.Background())
	first, ok := p.TryAcquire("hot", LaneControl)
	if !ok {
		t.Fatal("initial control")
	}
	second, ok := p.TryAcquire("hot", LaneControl)
	if !ok {
		t.Fatal("second control")
	}
	p.Demand("cold", 1)
	if _, ok := p.TryAcquire("cold", LaneExec); ok {
		t.Fatal("cold evicted active controls")
	}
	first()
	if release, ok := p.TryAcquire("hot", LaneControl); ok {
		release()
		t.Fatal("overlapping controls can starve cold forever")
	}
	second()
	if release, ok := p.TryAcquire("hot", LaneControl); ok {
		release()
		t.Fatal("hot control reacquired quiescent slot before cold")
	}
	if release, ok := p.TryAcquire("cold", LaneExec); ok {
		release()
		t.Fatal("cold ignored cleanup slot")
	}
	awaitPool(t, p, func(h PoolHealth) bool { return h.ReservedHosts == 0 })
	release, ok := p.TryAcquire("cold", LaneExec)
	if !ok {
		t.Fatal("cold did not progress")
	}
	p.Demand("cold", -1)
	release()
}
