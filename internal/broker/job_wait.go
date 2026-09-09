package broker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const maxJobWaitIDs = 64
const maxJobWaitTail = 1000 // remote agent's existing hard line limit
const defaultJobWaitSeconds = proto.DefaultJobWaitSeconds

type jobObservationKey struct{ owner, host, id string }
type jobObservation struct {
	key             jobObservationKey
	owner           Owner
	target          string
	changed         chan struct{}
	latest          *proto.Response
	err             error
	finished        bool
	subscribers     int
	until           time.Time
	releaseRequest  func()
	releaseResponse func()
}

// DispatchJobWait shares one observer per owned job, including intersecting
// batches. Wait budgets, batch order/WaitAny and tail selection belong to the
// subscriber; they are never copied into another subscriber's remote request.
func (s *Service) DispatchJobWait(ctx context.Context, owner Owner, host string, req *proto.Request) (*proto.Response, error) {
	if req == nil || req.Job == nil || req.Op != proto.OpJobWait || owner.Validate() != nil {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	p := req.Job
	seconds := p.WaitTimeoutSec
	if seconds == 0 {
		seconds = defaultJobWaitSeconds
	}
	if seconds < 0 || p.TailOnExit < 0 {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if seconds > int(proto.AbsoluteWaitSeconds) || p.TailOnExit > maxJobWaitTail || len(p.IDs) > maxJobWaitIDs {
		return nil, proto.NewError(proto.CodeLimitExceeded, "", proto.StateNotSent)
	}
	ids := p.IDs
	if len(ids) == 0 {
		ids = []string{p.ID}
	}
	seen := make(map[string]bool)
	ordered := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" && !seen[id] {
			seen[id] = true
			ordered = append(ordered, id)
		}
	}
	if len(ordered) == 0 {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if err := s.Jobs.ValidateRequest(host, owner.Key(), req); err != nil {
		return nil, err
	}
	if req.DeadlineUnixMilli != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.UnixMilli(req.DeadlineUnixMilli))
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	target, err := s.client.ProtocolTargetIdentity(host)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	until := start.Add(time.Duration(seconds) * time.Second)
	horizon := until
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(horizon) {
		horizon = deadline
	}
	observations, err := s.subscribeJobObservations(owner, host, target, ordered, horizon)
	if err != nil {
		return nil, err
	}
	defer s.unsubscribeJobObservations(observations)
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	timedOut := false
	for {
		s.sharedMu.Lock()
		complete, successful := 0, 0
		expired := false
		cases := []reflect.SelectCase{{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}, {Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timer.C)}}
		for _, o := range observations {
			if o.finished {
				complete++
				if o.err == nil {
					if o.latest != nil && o.latest.Job != nil && o.latest.Job.Info != nil && o.latest.Job.Info.State != proto.JobRunning {
						successful++
					} else {
						expired = true
					}
				}
			} else {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(o.changed)})
			}
		}
		ready := complete == len(observations) || p.WaitAny && successful > 0
		if ready || timedOut {
			response, projectionErr := projectJobWait(observations, p, start, timedOut && !ready || expired && !(p.WaitAny && successful > 0))
			s.sharedMu.Unlock()
			return response, projectionErr
		}
		s.sharedMu.Unlock()
		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			return nil, ctx.Err()
		}
		if chosen == 1 {
			timedOut = true
		}
	}
}

func (s *Service) subscribeJobObservations(owner Owner, host, target string, ids []string, until time.Time) ([]*jobObservation, error) {
	s.sharedMu.Lock()
	defer s.sharedMu.Unlock()
	if s.closed.Load() {
		return nil, ErrClosed
	}
	ownerObservers, ownerSubscribers, totalSubscribers := 0, 0, 0
	for k, o := range s.jobWaits {
		totalSubscribers += o.subscribers
		if k.owner == owner.Key() {
			ownerObservers++
			ownerSubscribers += o.subscribers
		}
	}
	for k, o := range s.shared {
		totalSubscribers += o.subscribers
		if k.owner == owner.Key() {
			ownerObservers++
			ownerSubscribers += o.subscribers
		}
	}
	added := 0
	for _, id := range ids {
		o := s.jobWaits[jobObservationKey{owner.Key(), host, id}]
		if o == nil {
			added++
		} else if o.target != target {
			return nil, errors.New("job observation target changed")
		}
	}
	limits := s.config.Get().QoS.effective()
	if totalSubscribers+len(ids) > maxSharedSubscribers || ownerSubscribers+len(ids) > maxOwnerSubscribers || len(s.shared)+len(s.jobWaits)+added > limits.MaxActive+limits.MaxQueued || ownerObservers+added > limits.PerOwner+limits.PerOwnerQueued {
		return nil, ErrQueueFull
	}
	// Reserve and admit the whole batch before publishing any observers. An
	// admission failure must neither create orphan workers nor partially subscribe.
	created := make([]*jobObservation, 0, added)
	rollback := func() {
		for _, o := range created {
			o.releaseRequest()
			s.EndRequest()
		}
	}
	out := make([]*jobObservation, 0, len(ids))
	for _, id := range ids {
		key := jobObservationKey{owner.Key(), host, id}
		o := s.jobWaits[key]
		if o == nil {
			canonical := proto.Request{Op: proto.OpJobWait, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{ID: id, WaitTimeoutSec: 1, TailOnExit: maxJobWaitTail}}
			raw, _ := json.Marshal(canonical)
			release, err := s.Ingress.Hold(owner.Key(), int64(len(raw)))
			if err != nil {
				rollback()
				return nil, err
			}
			if !s.BeginRequest() {
				release()
				rollback()
				return nil, ErrClosed
			}
			o = &jobObservation{key: key, owner: owner, target: target, changed: make(chan struct{}), until: until, releaseRequest: release, releaseResponse: func() {}}
			created = append(created, o)
		}
		out = append(out, o)
	}
	for _, o := range out {
		o.subscribers++
		if until.After(o.until) {
			o.until = until
		}
	}
	for _, o := range created {
		s.jobWaits[o.key] = o
		go s.observeJob(o)
	}
	return out, nil
}

func (s *Service) unsubscribeJobObservations(observations []*jobObservation) {
	s.sharedMu.Lock()
	defer s.sharedMu.Unlock()
	for _, o := range observations {
		o.subscribers--
		if o.finished && o.subscribers == 0 {
			o.releaseResponse()
			o.releaseResponse = func() {}
		}
	}
}

// A short canonical wait yields the execution quota regularly. A batch larger
// than the owner's active quota must still observe jobs beyond its first slots.
// Zero subscribers do not cancel an accepted observation: it continues until
// terminal completion or the latest accepted subscriber's observation horizon.
func (s *Service) observeJob(o *jobObservation) {
	defer s.EndRequest()
	defer o.releaseRequest()
	finishLocked := func(err error) {
		o.err, o.finished = err, true
		delete(s.jobWaits, o.key)
		close(o.changed)
		if o.subscribers == 0 {
			o.releaseResponse()
			o.releaseResponse = func() {}
		}
	}
	finish := func(err error) {
		s.sharedMu.Lock()
		finishLocked(err)
		s.sharedMu.Unlock()
	}
	initial := true
	for {
		s.sharedMu.Lock()
		if !time.Now().Before(o.until) {
			finishLocked(nil)
			s.sharedMu.Unlock()
			return
		}
		until := o.until
		s.sharedMu.Unlock()
		op := proto.OpJobWait
		params := &proto.JobParams{ID: o.key.id, WaitTimeoutSec: 1, TailOnExit: maxJobWaitTail}
		if initial {
			op = proto.OpJobStatus
			params = &proto.JobParams{ID: o.key.id}
		}
		request := &proto.Request{Op: op, ClientID: o.owner.ClientID, ProjectID: o.owner.ProjectID, Job: params}
		// The initial snapshot is part of wait work too: it must not consume
		// the caller's reserved control slot or bypass its wait/exec quota.
		callCtx, cancel := context.WithDeadline(s.observationCtx, until)
		// Scheduler cancellation can return before an already-running callback.
		// Join that attempt before retrying an extended horizon, or disarm a
		// callback that has not entered remote I/O yet. There must never be two
		// remote observations of this job during a deadline/extension race.
		var attemptMu sync.Mutex
		entered, abandoned := false, false
		attemptDone := make(chan struct{})
		response, err := s.DispatchScheduled(callCtx, o.key.host, o.key.owner, LaneExec, func(ctx context.Context) (*proto.Response, error) {
			attemptMu.Lock()
			if abandoned {
				attemptMu.Unlock()
				return nil, ctx.Err()
			}
			entered = true
			attemptMu.Unlock()
			defer close(attemptDone)
			// Reserve the actual protocol hard response ceiling during decode. Once
			// validated, only the encoded retained snapshot remains charged.
			release, err := s.Ingress.Hold(o.key.owner, proto.AbsoluteResponseFrameBytes)
			if err != nil {
				return nil, err
			}
			defer release()
			response, err := s.DispatchApproved(ctx, o.key.host, request, o.target)
			if err != nil {
				return response, err
			}
			if response == nil || !response.OK || response.Job == nil || response.Job.Info == nil || response.Job.Info.ID != o.key.id {
				return nil, errors.New("invalid shared job observation")
			}
			if err := s.RecordJobResponse(o.key.host, o.key.owner, request, response); err != nil {
				return nil, err
			}
			return response, nil
		})
		cancel()
		attemptMu.Lock()
		abandoned = true
		join := entered
		attemptMu.Unlock()
		if join {
			<-attemptDone
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && s.observationCtx.Err() == nil {
				s.sharedMu.Lock()
				extended := o.until.After(until)
				if !extended {
					finishLocked(nil)
				}
				s.sharedMu.Unlock()
				if extended {
					continue
				}
				return
			}
			finish(err)
			return
		}
		raw, err := json.Marshal(response)
		if err != nil || int64(len(raw)) > proto.AbsoluteResponseFrameBytes {
			finish(ErrIngressLimit)
			return
		}
		retained, err := s.Ingress.Hold(o.key.owner, int64(len(raw)))
		if err != nil {
			finish(err)
			return
		}
		s.sharedMu.Lock()
		o.releaseResponse()
		o.latest, o.releaseResponse = response, retained
		terminal := !initial && response.Job.Info.State != proto.JobRunning
		close(o.changed)
		if terminal {
			o.finished = true
			delete(s.jobWaits, o.key)
			if o.subscribers == 0 {
				o.releaseResponse()
				o.releaseResponse = func() {}
			}
		} else {
			o.changed = make(chan struct{})
		}
		s.sharedMu.Unlock()
		if terminal {
			s.Watches.Publish(o.key.owner+"\x00"+o.key.host+"\x00"+o.key.id, "observation_complete")
			return
		}
		initial = false
	}
}

func projectJobWait(observations []*jobObservation, p *proto.JobParams, start time.Time, timedOut bool) (*proto.Response, error) {
	response := &proto.Response{OK: true, Terminal: true, Execution: proto.StateCompleted}
	result := &proto.JobResult{Terminal: true, Execution: proto.StateCompleted, TimedOut: timedOut, WaitedMS: time.Since(start).Milliseconds()}
	response.Job = result
	for _, o := range observations {
		waited := &proto.WaitedJob{ID: o.key.id}
		if o.err != nil {
			if len(p.IDs) == 0 {
				return nil, o.err
			}
			waited.Err = "job unavailable"
		} else if o.latest == nil && len(p.IDs) == 0 {
			// A deadline before the initial snapshot (e.g. while queued for a
			// cold host) cannot produce a successful wait with missing job info.
			return nil, proto.NewError(proto.CodeDeadlineExceeded, "", proto.StateNotSent)
		} else if o.latest != nil && o.latest.Job != nil {
			source := o.latest.Job
			if source.Info != nil {
				info := *source.Info
				waited.Info = &info
			}
			if source.Info != nil && source.Info.State != proto.JobRunning && p.TailOnExit > 0 {
				waited.Logs = tailJobWaitLogs(source.Logs, p.TailOnExit)
				original := max(source.LogsTruncation.OriginalBytes, int64(len(source.Logs)))
				waited.LogsTruncation, _ = proto.NewTruncation(original, int64(len(waited.Logs)))
			}
			if len(p.IDs) == 0 {
				response.OperationID = o.latest.OperationID
				result.OperationID = o.latest.OperationID
			}
		}
		result.Waited = append(result.Waited, waited)
	}
	if len(p.IDs) == 0 {
		waited := result.Waited[0]
		result.Info, result.Logs, result.LogsTruncation, result.Waited = waited.Info, waited.Logs, waited.LogsTruncation, nil
	}
	return response, nil
}

func tailJobWaitLogs(logs string, lines int) string {
	if lines <= 0 {
		return ""
	}
	// The remote readTail already trims trailing newlines. Preserve empty final
	// lines within that canonical result and binary bytes in the retained window.
	for i := len(logs) - 1; i >= 0; i-- {
		if logs[i] == '\n' {
			lines--
			if lines == 0 {
				return strings.Clone(logs[i+1:])
			}
		}
	}
	return logs
}
