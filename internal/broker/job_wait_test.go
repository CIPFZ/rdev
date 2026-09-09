package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

type testJobWaitBackend struct {
	mu                          sync.Mutex
	finish                      map[string]chan struct{}
	active, peak, waits, status map[string]int
}

func newTestJobWaitService(t *testing.T, ids ...string) (*Service, Owner, *testJobWaitBackend) {
	t.Helper()
	s := NewService(nil)
	if err := s.Client().Hosts.Add(transport.Host{Name: "host", Addr: "host.invalid"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	owner := Owner{ClientID: "wait-review", ProjectID: "p"}
	b := &testJobWaitBackend{finish: map[string]chan struct{}{}, active: map[string]int{}, peak: map[string]int{}, waits: map[string]int{}, status: map[string]int{}}
	for _, id := range ids {
		b.finish[id] = make(chan struct{})
		if err := s.Jobs.Put(JobRef{ID: id, Host: "host", Owner: owner.Key()}); err != nil {
			t.Fatal(err)
		}
	}
	s.SetDispatcher(func(ctx context.Context, host string, req *proto.Request) (*proto.Response, error) {
		if host != "host" || req.ClientID != owner.ClientID || req.ProjectID != owner.ProjectID || req.Job == nil {
			t.Error("observation changed owner or host")
		}
		id := req.Job.ID
		b.mu.Lock()
		b.active[id]++
		b.peak[id] = max(b.peak[id], b.active[id])
		if req.Op == proto.OpJobWait {
			b.waits[id]++
		} else {
			b.status[id]++
		}
		n := b.waits[id]
		b.mu.Unlock()
		defer func() { b.mu.Lock(); b.active[id]--; b.mu.Unlock() }()
		if req.Op == proto.OpJobWait {
			if req.DeadlineUnixMilli != 0 || req.Job.WaitTimeoutSec != 1 || req.Job.TailOnExit != maxJobWaitTail || req.Job.WaitAny || len(req.Job.IDs) != 0 {
				t.Error("subscriber conditions reached canonical observer")
			}
			select {
			case <-b.finish[id]:
			case <-time.After(30 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else if req.Op != proto.OpJobStatus {
			t.Error("unexpected remote observation", req.Op)
		}
		state := proto.JobRunning
		select {
		case <-b.finish[id]:
			state = proto.JobExited
		default:
		}
		result := &proto.JobResult{Info: &proto.JobInfo{ID: id, PID: 123, State: state}, TimedOut: state == proto.JobRunning}
		if req.Op == proto.OpJobWait && state != proto.JobRunning {
			result.Logs = "zero\none\ntwo\x00"
			result.LogsTruncation, _ = proto.NewTruncation(30, int64(len(result.Logs)))
		}
		return &proto.Response{OK: true, Terminal: true, Execution: proto.StateCompleted, OperationID: fmt.Sprintf("op_wait_%s_%d", id, n), Job: result}, nil
	})
	return s, owner, b
}
func awaitJobWait(t *testing.T, check func() bool) {
	t.Helper()
	end := time.Now().Add(time.Second)
	for !check() {
		if time.Now().After(end) {
			t.Fatal("job wait condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

type jobWaitAnswer struct {
	response *proto.Response
	err      error
}

func startJobWait(s *Service, ctx context.Context, owner Owner, p *proto.JobParams, deadline int64) <-chan jobWaitAnswer {
	done := make(chan jobWaitAnswer, 1)
	go func() {
		r, err := s.DispatchJobWait(ctx, owner, "host", &proto.Request{Op: proto.OpJobWait, Job: p, DeadlineUnixMilli: deadline})
		done <- jobWaitAnswer{r, err}
	}()
	return done
}
func getJobWait(t *testing.T, ch <-chan jobWaitAnswer) jobWaitAnswer {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("subscriber did not finish")
		return jobWaitAnswer{}
	}
}

func TestJobWaitSubscriberBudgetsAndTailDoNotSplitObservation(t *testing.T) {
	s, owner, b := newTestJobWaitService(t, "one")
	short := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 1, TailOnExit: 1}, 0)
	long := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 3, TailOnExit: 2}, 0)
	deadline := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 3}, time.Now().Add(100*time.Millisecond).UnixMilli())
	awaitJobWait(t, func() bool {
		return s.SharedWaitStatus(owner.Key()) == (SharedWaitStatus{Observers: 1, Subscribers: 3})
	})
	if got := getJobWait(t, deadline); !errors.Is(got.err, context.DeadlineExceeded) {
		t.Fatal("wire deadline not local", got.err)
	}
	got := getJobWait(t, short)
	if got.err != nil || got.response == nil || got.response.Job == nil || !got.response.Job.TimedOut || got.response.Job.Info == nil || got.response.Job.Info.State != proto.JobRunning {
		t.Fatal("short wait lost its running snapshot", got)
	}
	select {
	case r := <-long:
		t.Fatal("short subscriber ended long wait", r.err)
	default:
	}
	tailOne := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 3, TailOnExit: 1}, 0)
	awaitJobWait(t, func() bool { return s.SharedWaitStatus(owner.Key()).Subscribers == 2 })
	close(b.finish["one"])
	two, one := getJobWait(t, long), getJobWait(t, tailOne)
	if one.err != nil || two.err != nil || one.response.Job.Logs != "two\x00" || two.response.Job.Logs != "one\ntwo\x00" || one.response.OperationID != two.response.OperationID || one.response.Job.TimedOut || two.response.Job.TimedOut {
		t.Fatal("subscriber tail projection/shared terminal identity failed", one, two)
	}
	for _, r := range []*proto.Response{one.response, two.response} {
		tr := r.Job.LogsTruncation
		if tr.Validate() != nil || tr.OriginalBytes != 30 || tr.RetainedBytes != int64(len(r.Job.Logs)) {
			t.Fatal("tail truncation accounting", tr)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.status["one"] != 1 || b.peak["one"] != 1 {
		t.Fatal("same job performed parallel/repeated initial observation", b.status, b.peak)
	}
}

func TestJobWaitIntersectingBatchesWaitAnyAndOrder(t *testing.T) {
	s, owner, b := newTestJobWaitService(t, "a", "b", "c")
	any := startJobWait(s, t.Context(), owner, &proto.JobParams{IDs: []string{"a", "b"}, WaitAny: true, WaitTimeoutSec: 3}, 0)
	all := startJobWait(s, t.Context(), owner, &proto.JobParams{IDs: []string{"b", "c", "a", "b"}, WaitTimeoutSec: 3, TailOnExit: 1}, 0)
	awaitJobWait(t, func() bool {
		return s.SharedWaitStatus(owner.Key()) == (SharedWaitStatus{Observers: 3, Subscribers: 5})
	})
	// Wait until every job has a status snapshot before projecting WaitAny.
	awaitJobWait(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.status["a"] == 1 && b.status["b"] == 1 && b.status["c"] == 1
	})
	close(b.finish["b"])
	got := getJobWait(t, any)
	if got.err != nil || len(got.response.Job.Waited) != 2 || got.response.Job.Waited[0].ID != "a" || got.response.Job.Waited[1].ID != "b" || got.response.Job.TimedOut {
		t.Fatal("WaitAny order/result", got)
	}
	select {
	case r := <-all:
		t.Fatal("WaitAny ended independent all wait", r.err)
	default:
	}
	close(b.finish["a"])
	close(b.finish["c"])
	got = getJobWait(t, all)
	if got.err != nil {
		t.Fatal(got.err)
	}
	ids := []string{}
	for _, w := range got.response.Job.Waited {
		ids = append(ids, w.ID)
		if w.Info == nil || w.Info.State == proto.JobRunning || w.Logs != "two\x00" {
			t.Fatal("batch incomplete", w)
		}
	}
	if strings.Join(ids, ",") != "b,c,a" {
		t.Fatal("batch order/dedup changed", ids)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, id := range []string{"a", "b", "c"} {
		if b.status[id] != 1 || b.peak[id] != 1 {
			t.Fatal("overlap duplicated observation", id, b.status, b.peak)
		}
	}
}

func TestJobWaitZeroSubscribersRetainLeaseAndPersistOneTerminal(t *testing.T) {
	s, owner, b := newTestJobWaitService(t, "one")
	path := t.TempDir() + "/events"
	if err := s.Events.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := startJobWait(s, ctx, owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 3}, 0)
	awaitJobWait(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.waits["one"] > 0 })
	cancel()
	if got := getJobWait(t, done); !errors.Is(got.err, context.Canceled) {
		t.Fatal(got.err)
	}
	if got := s.SharedWaitStatus(owner.Key()); got != (SharedWaitStatus{Observers: 1, Subscribers: 0}) {
		t.Fatal(got)
	}
	if s.Reapable(time.Now().Add(time.Hour)) || s.Ingress.Snapshot(owner.Key()).ObservationBytes == 0 {
		t.Fatal("disconnect released observation lease/resources")
	}
	other := Owner{ClientID: owner.ClientID, ProjectID: "other"}
	if _, err := s.DispatchJobWait(t.Context(), other, "host", &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: "one"}}); err == nil {
		t.Fatal("other owner joined observation")
	}
	close(b.finish["one"])
	awaitJobWait(t, func() bool {
		return s.SharedWaitStatus(owner.Key()) == (SharedWaitStatus{}) && s.Ingress.Snapshot(owner.Key()).ObservationBytes == 0
	})
	history := NewJobHistory()
	if err := history.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	page, err := history.Query(owner.Key(), "host", "one", JobEventCursor{}, 64)
	if err != nil || len(page.Events) != 2 || page.Events[0].State != proto.JobRunning || page.Events[1].State != proto.JobExited {
		t.Fatal("zero-subscriber durable terminal lost/duplicated", page, err)
	}
}

func TestJobWaitRejectsBatchAtomicallyAndReleasesOnShutdown(t *testing.T) {
	s, owner, _ := newTestJobWaitService(t, "a", "b", "c", "d", "e")
	if err := s.ReloadConfig(Config{MaxHosts: 1, IdleTTL: time.Second, QoS: QoSConfig{MaxActive: 4, PerHost: 4, PerOwner: 2, MaxQueued: 8, PerOwnerQueued: 2}}); err != nil {
		t.Fatal(err)
	}
	_, err := s.DispatchJobWait(t.Context(), owner, "host", &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{IDs: []string{"a", "b", "c", "d", "e"}}})
	if !errors.Is(err, ErrQueueFull) || s.SharedWaitStatus(owner.Key()) != (SharedWaitStatus{}) || s.Ingress.Snapshot(owner.Key()).Bytes != 0 {
		t.Fatal("partial batch admission", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	pending := startJobWait(s, ctx, owner, &proto.JobParams{ID: "a", WaitTimeoutSec: 3}, 0)
	awaitJobWait(t, func() bool { return s.SharedWaitStatus(owner.Key()).Observers == 1 })
	s.stopObservations()
	if got := getJobWait(t, pending); !errors.Is(got.err, context.Canceled) {
		t.Fatal("shutdown observer", got.err)
	}
	cancel()
	awaitJobWait(t, func() bool {
		return s.SharedWaitStatus(owner.Key()) == (SharedWaitStatus{}) && s.Ingress.Snapshot(owner.Key()).Bytes == 0
	})
}

func TestJobWaitBatchLargerThanActiveQuotaStillObservesEveryJob(t *testing.T) {
	s, owner, b := newTestJobWaitService(t, "a", "b", "c", "d", "e")
	done := startJobWait(s, t.Context(), owner, &proto.JobParams{IDs: []string{"a", "b", "c", "d", "e"}, WaitTimeoutSec: 3}, 0)
	awaitJobWait(t, func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, id := range []string{"a", "b", "c", "d", "e"} {
			if b.waits[id] == 0 {
				return false
			}
		}
		return true
	})
	if got := s.Scheduler.Snapshot(owner.Key()); got.Active > 3 || got.Queued > 5 {
		t.Fatal("wait bypassed owner execution quota", got)
	}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		close(b.finish[id])
	}
	got := getJobWait(t, done)
	if got.err != nil || got.response.Job.TimedOut || len(got.response.Job.Waited) != 5 {
		t.Fatal("batch could not observe beyond active slots", got)
	}
	for _, w := range got.response.Job.Waited {
		if w.Err != "" || w.Info == nil || w.Info.State == proto.JobRunning {
			t.Fatal("batch result lost a job", w)
		}
	}
}

func TestJobWaitSubscriptionValidationAndTailWindow(t *testing.T) {
	s, owner, _ := newTestJobWaitService(t, "one")
	for _, p := range []*proto.JobParams{{ID: "one", WaitTimeoutSec: -1}, {ID: "one", WaitTimeoutSec: 3601}, {ID: "one", TailOnExit: -1}, {ID: "one", TailOnExit: 1001}, {IDs: make([]string, 65)}, {}} {
		if _, err := s.DispatchJobWait(t.Context(), owner, "host", &proto.Request{Op: proto.OpJobWait, Job: p}); err == nil {
			t.Fatal("invalid subscriber admitted", p)
		}
	}
	if s.SharedWaitStatus(owner.Key()) != (SharedWaitStatus{}) || s.Ingress.Snapshot(owner.Key()).Bytes != 0 {
		t.Fatal("validation retained resources")
	}
	window := strings.Repeat("large\x00line\n", 60000) + "last\x00line"
	if got := tailJobWaitLogs(window, 2); got != "large\x00line\nlast\x00line" {
		t.Fatal("binary tail window changed")
	}
	// A projected result must report timeout when all observations expire even
	// if another member of a WaitAll batch already reached a terminal state.
	expired := &jobObservation{key: jobObservationKey{id: "one"}, latest: &proto.Response{Job: &proto.JobResult{Info: &proto.JobInfo{ID: "one", State: proto.JobRunning}}}}
	r, err := projectJobWait([]*jobObservation{expired}, &proto.JobParams{ID: "one"}, time.Now(), true)
	if err != nil || !r.Job.TimedOut || r.Job.Info == nil {
		t.Fatal("expired snapshot projection", r, err)
	}
}

func TestJobWaitDoesNotJoinOrContinueOnChangedHostTarget(t *testing.T) {
	s, owner, _ := newTestJobWaitService(t, "one")
	entered, release := make(chan struct{}), make(chan struct{})
	s.SetDispatcher(func(ctx context.Context, _ string, req *proto.Request) (*proto.Response, error) {
		if req.Op == proto.OpJobWait {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &proto.Response{OK: true, OperationID: "op_old_target", Job: &proto.JobResult{Info: &proto.JobInfo{ID: "one", State: proto.JobRunning, PID: 123}}}, nil
	})
	pending := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 3}, 0)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial wait did not start")
	}
	if err := s.Client().Hosts.Add(transport.Host{Name: "host", Addr: "replacement.invalid"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DispatchJobWait(t.Context(), owner, "host", &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: "one", WaitTimeoutSec: 3}}); err == nil {
		t.Fatal("new subscriber joined the old target")
	}
	close(release)
	got := getJobWait(t, pending)
	if got.err == nil {
		t.Fatal("observer crossed target identity")
	}
	awaitJobWait(t, func() bool {
		return s.SharedWaitStatus(owner.Key()) == (SharedWaitStatus{}) && s.Ingress.Snapshot(owner.Key()).Bytes == 0
	})
}

func TestJobWaitHorizonReleasesQueuedObservation(t *testing.T) {
	s, owner, _ := newTestJobWaitService(t, "one")
	if err := s.ReloadConfig(Config{MaxHosts: 1, MaxWarmHosts: 16, IdleTTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	holdCtx, release := context.WithCancel(t.Context())
	defer release()
	entered := make(chan struct{})
	holdDone := make(chan struct{})
	go func() {
		defer close(holdDone)
		_, _ = s.DispatchScheduled(holdCtx, "occupied-host", "occupant", LaneExec, func(ctx context.Context) (*proto.Response, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	}()
	<-entered
	pending := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 1}, 0)
	if got := getJobWait(t, pending); got.err == nil {
		t.Fatal("queued wait returned a successful missing snapshot")
	}
	expiry := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(expiry) && s.SharedWaitStatus(owner.Key()).Observers != 0 {
		time.Sleep(time.Millisecond)
	}
	if got := s.SharedWaitStatus(owner.Key()); got != (SharedWaitStatus{}) {
		t.Fatal("expired observation retained its queue/lease", got)
	}
	if got := s.Ingress.Snapshot(owner.Key()); got.Bytes != 0 {
		t.Fatal("expired observation retained ingress", got)
	}
	if got := s.Scheduler.Snapshot(owner.Key()); got.Active+got.Queued != 0 {
		t.Fatal("expired observation still scheduled", got)
	}
	if s.Scheduler.Snapshot("occupant").Active != 1 {
		t.Fatal("expiry canceled another owner")
	}
	release()
	<-holdDone
}

func TestJobWaitExtendedHorizonJoinsCanceledAttemptBeforeRetry(t *testing.T) {
	s, owner, _ := newTestJobWaitService(t, "one")
	firstEntered, firstCanceled := make(chan struct{}), make(chan struct{})
	allowCleanup, secondEntered, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	calls, active, peak := 0, 0, 0
	s.SetDispatcher(func(ctx context.Context, _ string, req *proto.Request) (*proto.Response, error) {
		result := &proto.JobResult{Info: &proto.JobInfo{ID: "one", PID: 123, State: proto.JobRunning}}
		if req.Op == proto.OpJobWait {
			mu.Lock()
			calls++
			n := calls
			active++
			peak = max(peak, active)
			mu.Unlock()
			defer func() { mu.Lock(); active--; mu.Unlock() }()
			if n == 1 {
				close(firstEntered)
				<-ctx.Done()
				close(firstCanceled)
				<-allowCleanup
				return nil, ctx.Err()
			}
			if n != 2 {
				t.Error("unexpected duplicate retry", n)
			}
			close(secondEntered)
			select {
			case <-finish:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			result.Info.State = proto.JobExited
		}
		return &proto.Response{OK: true, Terminal: true, OperationID: "op_extended_wait", Job: result}, nil
	})
	short := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 1}, 0)
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first wait did not start")
	}
	long := startJobWait(s, t.Context(), owner, &proto.JobParams{ID: "one", WaitTimeoutSec: 3}, 0)
	awaitJobWait(t, func() bool { return s.SharedWaitStatus(owner.Key()).Subscribers == 2 })
	got := getJobWait(t, short)
	if got.err != nil || got.response == nil || !got.response.Job.TimedOut {
		t.Fatal("short subscriber outcome", got)
	}
	select {
	case <-firstCanceled:
	case <-time.After(time.Second):
		t.Fatal("old RPC ignored its horizon")
	}
	select {
	case <-secondEntered:
		t.Fatal("extended observer overlapped old canceled RPC")
	default:
	}
	select {
	case got := <-long:
		t.Fatal("old horizon terminated extended subscriber", got)
	default:
	}
	close(allowCleanup)
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		t.Fatal("extended observer did not resume")
	}
	close(finish)
	got = getJobWait(t, long)
	if got.err != nil || got.response == nil || got.response.Job.TimedOut || got.response.Job.Info.State != proto.JobExited {
		t.Fatal("extended subscriber lost terminal result", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 || peak != 1 {
		t.Fatal("extended observation was not serial", calls, peak)
	}
}
