package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// waitClock is deliberately tiny so watcher fan-out and cancellation tests can
// advance time without sleeps.
type waitClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type realWaitClock struct{}

func (realWaitClock) Now() time.Time                         { return time.Now() }
func (realWaitClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

const (
	maxJobWatchers      = 32
	maxJobWaiters       = 128
	maxJobWaitersPerJob = 32
	maxJobWaitIDs       = 64
)

type waitObservation struct {
	info *proto.JobInfo
	err  error
}

type jobWatcher struct {
	key   string
	id    string
	state string
	meta  *jobMeta
	stop  chan struct{}
	subs  map[chan waitObservation]struct{}
}

// waitHub ensures that one job has one polling loop regardless of how many
// callers wait for it. Subscriber channels are single-shot and bounded; a slow
// or canceled caller cannot block the poller.
type waitHub struct {
	mu           sync.Mutex
	clock        waitClock
	watchers     map[string]*jobWatcher
	totalWaiters int
	maxWatchers  int
	maxWaiters   int
	perJob       int
}

func newWaitHub(clock waitClock) *waitHub {
	if clock == nil {
		clock = realWaitClock{}
	}
	return &waitHub{
		clock: clock, watchers: make(map[string]*jobWatcher),
		maxWatchers: maxJobWatchers, maxWaiters: maxJobWaiters,
		perJob: maxJobWaitersPerJob,
	}
}

var defaultWaitHub = newWaitHub(realWaitClock{})

func watcherKey(state, id string) string { return state + "\x00" + id }

func (h *waitHub) subscribe(id, state string) (<-chan waitObservation, func(), error) {
	if _, err := validatedJobDir(state, id); err != nil {
		return nil, nil, err
	}
	key := watcherKey(state, id)

	h.mu.Lock()
	if h.totalWaiters >= h.maxWaiters {
		h.mu.Unlock()
		return nil, nil, proto.NewError(proto.CodeWatcherLimit, "", proto.StateAccepted)
	}
	w := h.watchers[key]
	if w != nil && len(w.subs) >= h.perJob {
		h.mu.Unlock()
		return nil, nil, proto.NewError(proto.CodeWatcherLimit, "", proto.StateAccepted)
	}
	if w == nil {
		if len(h.watchers) >= h.maxWatchers {
			h.mu.Unlock()
			return nil, nil, proto.NewError(proto.CodeWatcherLimit, "", proto.StateAccepted)
		}
		meta, err := readMeta(jobDir(state, id))
		if err != nil {
			h.mu.Unlock()
			return nil, nil, objectNotFoundError(err)
		}
		w = &jobWatcher{
			key: key, id: id, state: state, meta: meta,
			stop: make(chan struct{}), subs: make(map[chan waitObservation]struct{}),
		}
		h.watchers[key] = w
		go h.observe(w)
	}
	ch := make(chan waitObservation, 1)
	w.subs[ch] = struct{}{}
	h.totalWaiters++
	h.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() { h.unsubscribe(key, ch) })
	}
	return ch, cancel, nil
}

func (h *waitHub) unsubscribe(key string, ch chan waitObservation) {
	h.mu.Lock()
	defer h.mu.Unlock()
	w := h.watchers[key]
	if w == nil {
		return
	}
	if _, ok := w.subs[ch]; !ok {
		return
	}
	delete(w.subs, ch)
	h.totalWaiters--
	if len(w.subs) == 0 {
		delete(h.watchers, key)
		close(w.stop)
	}
}

func (h *waitHub) observe(w *jobWatcher) {
	interval := waitPollMin
	for {
		info := metaToInfo(w.meta, jobDir(w.state, w.id))
		if info.State != proto.JobRunning {
			h.finish(w, waitObservation{info: info})
			return
		}
		select {
		case <-w.stop:
			return
		case <-h.clock.After(interval):
		}
		if interval < waitPollMax {
			interval *= 2
			if interval > waitPollMax {
				interval = waitPollMax
			}
		}
	}
}

func (h *waitHub) finish(w *jobWatcher, result waitObservation) {
	h.mu.Lock()
	current := h.watchers[w.key]
	if current != w {
		h.mu.Unlock()
		return
	}
	delete(h.watchers, w.key)
	for ch := range w.subs {
		ch <- result
		close(ch)
		h.totalWaiters--
	}
	w.subs = nil
	h.mu.Unlock()
}

func (h *waitHub) counts() (watchers, waiters int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.watchers), h.totalWaiters
}

func waitBudget(p *proto.JobParams) (time.Duration, error) {
	budget := p.WaitTimeoutSec
	if budget < 0 {
		return 0, invalidRequestError("wait_timeout_sec must not be negative")
	}
	if budget == 0 {
		budget = defaultWaitSec
	}
	if budget > maxWaitSec {
		return 0, limitExceededError("wait_timeout_sec exceeds hard limit")
	}
	return time.Duration(budget) * time.Second, nil
}

func jobWaitContext(ctx context.Context, hub *waitHub, p *proto.JobParams, state string) (*proto.JobResult, error) {
	if hub == nil {
		hub = defaultWaitHub
	}
	budget, err := waitBudget(p)
	if err != nil {
		return nil, err
	}
	if p.TailOnExit < 0 {
		return nil, invalidRequestError("tail_on_exit must not be negative")
	}
	if p.TailOnExit > hardLogTailLines {
		return nil, proto.NewError(proto.CodeLimitExceeded, "", proto.StateAccepted)
	}
	ids := p.IDs
	if len(ids) == 0 {
		ids = []string{p.ID}
	}
	if len(ids) > maxJobWaitIDs {
		return nil, proto.NewError(proto.CodeLimitExceeded, "", proto.StateAccepted)
	}

	seen := make(map[string]struct{}, len(ids))
	type subscription struct {
		id     string
		ch     <-chan waitObservation
		cancel func()
		result *waitObservation
		err    string
		cause  error
	}
	subs := make([]*subscription, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ch, cancel, subErr := hub.subscribe(id, state)
		s := &subscription{id: id, ch: ch, cancel: cancel}
		if subErr != nil {
			var envelope *proto.ErrorEnvelope
			if errors.As(subErr, &envelope) {
				return nil, envelope
			}
			s.err = "job unavailable"
			s.cause = subErr
		} else {
			defer cancel()
		}
		subs = append(subs, s)
	}
	if len(subs) == 0 {
		return nil, invalidRequestError("job_wait needs a usable id")
	}

	start := hub.clock.Now()
	waitCtx, stopWait := context.WithTimeout(ctx, budget)
	defer stopWait()
	timedOut := false
	remaining := 0
	for _, s := range subs {
		if s.err == "" {
			remaining++
		}
	}
	for remaining > 0 {
		cases := []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(waitCtx.Done())},
		}
		indexes := make([]int, 0, remaining)
		for i, s := range subs {
			if s.result == nil && s.err == "" {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(s.ch)})
				indexes = append(indexes, i)
			}
		}
		chosen, value, ok := reflect.Select(cases)
		switch chosen {
		case 0:
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			timedOut, remaining = true, 0
		default:
			s := subs[indexes[chosen-1]]
			obs := waitObservation{}
			if ok {
				obs = value.Interface().(waitObservation)
			}
			s.result = &obs
			remaining--
			if p.WaitAny {
				remaining = 0
			}
		}
	}

	res := &proto.JobResult{TimedOut: timedOut, WaitedMS: hub.clock.Now().Sub(start).Milliseconds()}
	for _, s := range subs {
		w := &proto.WaitedJob{ID: s.id, Err: s.err}
		if s.result != nil {
			w.Info = s.result.info
			if s.result.err != nil {
				w.Err = "job unavailable"
				s.cause = s.result.err
			}
		} else if s.err == "" {
			if meta, readErr := readMeta(jobDir(state, s.id)); readErr == nil {
				w.Info = metaToInfo(meta, jobDir(state, s.id))
			}
		}
		if p.TailOnExit > 0 && w.Info != nil && w.Info.State != proto.JobRunning {
			dir, dirErr := validatedJobDir(state, s.id)
			if dirErr != nil {
				continue
			}
			logPath := filepath.Join(dir, "stdout")
			if secureRecordFile(logPath) != nil {
				continue
			}
			if logs, readErr := readTail(logPath, p.TailOnExit); readErr == nil {
				w.Logs = logs
				if info, statErr := os.Stat(logPath); statErr == nil {
					w.LogsTruncation, _ = proto.NewTruncation(info.Size(), int64(len(logs)))
				}
			}
		}
		res.Waited = append(res.Waited, w)
	}
	if len(p.IDs) == 0 {
		w := res.Waited[0]
		if w.Err != "" {
			if subs[0].cause != nil {
				return nil, subs[0].cause
			}
			return nil, processStateError("job wait failed")
		}
		res.Info, res.Logs, res.LogsTruncation, res.Waited = w.Info, w.Logs, w.LogsTruncation, nil
	}
	return res, nil
}
