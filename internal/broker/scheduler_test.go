package broker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func testScheduler(t *testing.T, c QoSConfig) *Scheduler {
	t.Helper()
	s := NewScheduler(c, 128)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}
func awaitScheduler(t *testing.T, s *Scheduler, owner string, active, queued int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		st := s.Snapshot(owner)
		if st.Active == active && st.Queued == queued {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: active=%d queued=%d, want %d/%d", owner, st.Active, st.Queued, active, queued)
		}
		time.Sleep(time.Millisecond)
	}
}
func scheduleBlock(s *Scheduler, ctx context.Context, host, owner string, lane Lane, ran *atomic.Int32) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := s.Do(ctx, host, owner, lane, func(ctx context.Context) (*proto.Response, error) {
			if ran != nil {
				ran.Add(1)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		})
		done <- err
	}()
	return done
}
func TestSchedulerOwnerCannotOccupyAllExecutionOrQueue(t *testing.T) {
	s := testScheduler(t, QoSConfig{MaxQueued: 16, PerOwnerQueued: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 6; i++ {
		scheduleBlock(s, ctx, "h", "flood", LaneExec, nil)
	}
	awaitScheduler(t, s, "flood", 3, 3)
	// The owner's fourth pending slot is reserved for its own control calls.
	if _, err := s.Do(ctx, "h", "flood", LaneExec, func(context.Context) (*proto.Response, error) { t.Error("rejected work executed"); return nil, nil }); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	other := scheduleBlock(s, ctx, "h", "other", LaneExec, nil)
	awaitScheduler(t, s, "other", 1, 0)
	controlCtx, controlCancel := context.WithTimeout(ctx, time.Second)
	defer controlCancel()
	if _, err := s.Do(controlCtx, "h", "flood", LaneControl, func(context.Context) (*proto.Response, error) { return &proto.Response{OK: true}, nil }); err != nil {
		t.Fatal(err)
	}
	// Unknown owners see neither the flood owner's counts nor its history.
	if st := s.Snapshot("outsider"); st.Active+st.Queued != 0 || st.Started+st.Rejected != 0 || len(st.Lanes) != 0 {
		t.Fatalf("cross-owner snapshot: %+v", st)
	}
	cancel()
	<-other
	awaitScheduler(t, s, "flood", 0, 0)
	awaitScheduler(t, s, "other", 0, 0)
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.owners) != 0 || len(s.hosts) != 0 || len(s.ownerLanes) != 0 {
		t.Fatal("cancellation retained owner accounting")
	}
}
func TestSchedulerAllWorkersStartAndSkipIneligibleHost(t *testing.T) {
	s := testScheduler(t, QoSConfig{PerHost: 5, PerOwner: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Host h1 is full for exec (two slots remain reserved for control).
	for i := 0; i < 3; i++ {
		scheduleBlock(s, ctx, "h1", fmt.Sprint(i), LaneExec, nil)
	}
	for i := 0; i < 3; i++ {
		awaitScheduler(t, s, fmt.Sprint(i), 1, 0)
	}
	scheduleBlock(s, ctx, "h1", "blocked", LaneExec, nil)
	awaitScheduler(t, s, "blocked", 0, 1)
	// Same owner's next request targets another host and must pass its blocked
	// head; then all eight exec workers must run without another external wake.
	scheduleBlock(s, ctx, "h2", "blocked", LaneExec, nil)
	awaitScheduler(t, s, "blocked", 1, 1)
	for i := 3; i < 7; i++ {
		scheduleBlock(s, ctx, "h"+fmt.Sprint(i), fmt.Sprint(i), LaneExec, nil)
	}
	for i := 3; i < 7; i++ {
		awaitScheduler(t, s, fmt.Sprint(i), 1, 0)
	}
	s.mu.Lock()
	active := s.lanes[LaneExec].Active
	s.mu.Unlock()
	if active != 8 {
		t.Fatalf("only %d workers started", active)
	}
	for i := 0; i < 2; i++ {
		scheduleBlock(s, ctx, "h1", "ctl"+fmt.Sprint(i), LaneControl, nil)
	}
	for i := 0; i < 2; i++ {
		awaitScheduler(t, s, "ctl"+fmt.Sprint(i), 1, 0)
	}
}
func TestSchedulerCancellationFreesQueuedCapacityImmediately(t *testing.T) {
	s := testScheduler(t, QoSConfig{MaxQueued: 16, PerOwnerQueued: 4})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scheduleBlock(s, ctx, "h", "holder", LaneBulk, nil)
	awaitScheduler(t, s, "holder", 1, 0)
	queuedCtx, queuedCancel := context.WithCancel(ctx)
	var ran atomic.Int32
	done := scheduleBlock(s, queuedCtx, "h", "queued", LaneBulk, &ran)
	awaitScheduler(t, s, "queued", 0, 1)
	queuedCancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	awaitScheduler(t, s, "queued", 0, 0)
	if ran.Load() != 0 {
		t.Fatal("canceled queued request ran")
	}
	if s.Snapshot("holder").Active != 1 {
		t.Fatal("cancellation affected the running owner")
	}
}
func TestSchedulerReloadAppliesWeightsToAlreadyQueuedRequests(t *testing.T) {
	s := testScheduler(t, QoSConfig{})
	holdCtx, release := context.WithCancel(context.Background())
	defer release()
	scheduleBlock(s, holdCtx, "h", "holder", LaneBulk, nil)
	awaitScheduler(t, s, "holder", 1, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	order := make(chan string, 80)
	for i := 0; i < 24; i++ {
		for _, owner := range []string{"heavy", "light"} {
			go func(owner string) {
				_, _ = s.Do(ctx, "h", owner, LaneBulk, func(context.Context) (*proto.Response, error) { order <- owner; return nil, nil })
			}(owner)
		}
	}
	awaitScheduler(t, s, "heavy", 0, 24)
	awaitScheduler(t, s, "light", 0, 24)
	s.Configure(QoSConfig{}, 128, map[string]int{"heavy": 3, "light": 1})
	release()
	h, l := 0, 0
	for i := 0; i < 24; i++ {
		select {
		case owner := <-order:
			if owner == "heavy" {
				h++
			} else {
				l++
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if h != 18 || l != 6 {
		t.Fatalf("reload did not apply weights to backlog: %d:%d", h, l)
	}
}
func TestSchedulerCloseCancelsActiveAndQueuedWithoutReplay(t *testing.T) {
	s := testScheduler(t, QoSConfig{})
	var ran atomic.Int32
	active := scheduleBlock(s, context.Background(), "h", "a", LaneBulk, &ran)
	awaitScheduler(t, s, "a", 1, 0)
	pending := scheduleBlock(s, context.Background(), "h", "b", LaneBulk, &ran)
	awaitScheduler(t, s, "b", 0, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(<-active, context.Canceled) || !errors.Is(<-pending, ErrClosed) {
		t.Fatal("incorrect terminal results")
	}
	if ran.Load() != 1 {
		t.Fatal("queued work executed during shutdown")
	}
	awaitScheduler(t, s, "a", 0, 0)
	awaitScheduler(t, s, "b", 0, 0)
	if _, err := s.Do(ctx, "h", "a", LaneBulk, nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestSchedulerQoSValidation(t *testing.T) {
	for _, c := range []QoSConfig{{PerOwner: 11}, {PerHost: 2}, {MaxActive: -1}, {MaxQueued: 4097}, {PerOwnerQueued: 256}, {PerOwnerQueued: 1}} {
		if c.validate() == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
}
func TestLaneForOperation(t *testing.T) {
	for op, want := range map[string]Lane{"job_stop": LaneControl, "job_status": LaneControl, "ping": LaneControl, "job_wait": LaneExec, "exec": LaneExec, "read_file": LaneBulk, "write_file": LaneBulk, "sync.pull": LaneBulk} {
		if got := LaneForOperation(op); got != want {
			t.Errorf("%s=%s want %s", op, got, want)
		}
	}
}

func TestSchedulerBulkByteBudgetPreservesControlAndChargesExactOwner(t *testing.T) {
	s := testScheduler(t, QoSConfig{BulkBytesPerSecond: 1 << 20})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := s.Do(ctx, "h", "a", LaneBulk, func(context.Context) (*proto.Response, error) {
		return &proto.Response{Read: &proto.ReadResult{Content: string(make([]byte, 128<<10))}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.Snapshot("a").BulkPayloadBytes != 128<<10 || s.Snapshot("b").BulkPayloadBytes != 0 {
		t.Fatal("bulk bytes crossed owner scopes")
	}
	bulkStarted := make(chan time.Time, 1)
	go func() {
		_, _ = s.Do(ctx, "h", "b", LaneBulk, func(context.Context) (*proto.Response, error) { bulkStarted <- time.Now(); return nil, nil })
	}()
	awaitScheduler(t, s, "b", 0, 1)
	// A byte-limited bulk lane has no occupied execution worker or active owner
	// quota, and must not delay an independent control request.
	if s.Snapshot("a").Active != 0 {
		t.Fatal("byte pacing occupied an active worker")
	}
	_, err = s.Do(ctx, "h", "control", LaneControl, func(context.Context) (*proto.Response, error) {
		select {
		case <-bulkStarted:
			t.Error("bulk started before reserved control")
		default:
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case at := <-bulkStarted:
		if at.Sub(start) < 125*time.Millisecond {
			t.Fatal("actual payload was not paced")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
