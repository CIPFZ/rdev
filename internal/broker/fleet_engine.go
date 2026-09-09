package broker

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func (s *Service) fleetApprovalValid(p FleetPlan) bool {
	d := s.DecideRequest(p.Owner, "fleet.execute", "")
	return p.ApprovalConsumed && p.ApprovalRef != "" && time.Now().Before(p.ApprovalExpiresAt) && d.Allow && d.Digest == p.ApprovalPolicy
}
func fleetThreshold(p FleetPlan) bool {
	failures, total := 0, 0
	for _, r := range p.Runs {
		switch r.State {
		case "success":
			total++
		case "failed", "unreachable", "ambiguous":
			failures++
			total++
		}
	}
	policy := p.Spec.Rollout
	return failures > 0 && (policy.MaxFailures != nil && failures > *policy.MaxFailures || policy.MaxFailureRatio != nil && total > 0 && float64(failures)/float64(total) > *policy.MaxFailureRatio)
}
func fleetCancelPending(p *FleetPlan) {
	for i := range p.Runs {
		if p.Runs[i].State == "pending" {
			p.Runs[i].State = "canceled"
			p.Runs[i].Reason = "remaining_canceled"
		}
	}
}
func fleetAdvance(p *FleetPlan, now time.Time) {
	if p.State != "running" {
		return
	}
	if fleetThreshold(*p) {
		p.Reason = "failure_threshold"
		if p.Spec.Rollout.OnThreshold == "cancel_remaining" {
			p.State = "canceled"
			fleetCancelPending(p)
		} else {
			p.State = "paused"
		}
		return
	}
	if p.WaveEnd == 0 {
		switch p.Spec.Rollout.Strategy {
		case "all_at_once":
			p.WaveEnd = len(p.Runs)
		case "canary":
			p.WaveEnd = p.Spec.Rollout.Canary
		default:
			p.WaveEnd = min(len(p.Runs), p.Spec.Rollout.WaveSize)
		}
	}
	done := true
	active := false
	for _, r := range p.Runs[:p.WaveEnd] {
		if r.State == "pending" || r.State == "dispatching" || r.State == "running" {
			done = false
		}
		if r.State == "dispatching" || r.State == "running" {
			active = true
		}
	}
	for _, r := range p.Runs[:p.WaveEnd] {
		if r.State == "ambiguous" {
			p.State = "paused"
			p.Reason = "ambiguous"
			return
		}
	}
	if !done {
		return
	}
	// An uncertain canary can never open a subsequent wave. A manual resume
	// does not waive this condition; reconcile or an approved retry is required.
	if p.Spec.Rollout.Strategy == "canary" {
		for _, r := range p.Runs[:p.Spec.Rollout.Canary] {
			if r.State != "success" {
				p.State = "paused"
				p.Reason = "canary_failed"
				return
			}
		}
	}
	for _, r := range p.Runs[:p.WaveEnd] {
		if r.State == "ambiguous" {
			p.State = "paused"
			p.Reason = "ambiguous"
			return
		}
	}
	if p.WaveEnd == len(p.Runs) && !active {
		p.State = "completed"
		for _, r := range p.Runs {
			if r.State != "success" {
				p.State = "failed"
			}
		}
		p.Reason = "finished"
		return
	}
	if p.NextWaveAt.IsZero() {
		p.NextWaveAt = now.Add(time.Duration(p.Spec.Rollout.PauseBetweenWavesSec) * time.Second)
	}
	if !now.Before(p.NextWaveAt) {
		p.WaveEnd = min(len(p.Runs), p.WaveEnd+p.Spec.Rollout.WaveSize)
		p.NextWaveAt = time.Time{}
	}
}

// RecoverFleet resumes observation of recorded attempts. Pending targets may be
// admitted only while the persisted approval remains valid under current policy.
func (s *Service) RecoverFleet() {
	for id, p := range s.Fleet.snapshot() {
		if p.State == "running" || fleetHasUnresolved(p) {
			s.startFleet(id)
		}
	}
}
func fleetHasUnresolved(p FleetPlan) bool {
	for _, r := range p.Runs {
		if r.State == "dispatching" || r.State == "running" || r.State == "ambiguous" {
			return true
		}
	}
	return false
}
func (s *Service) startFleet(id string) {
	f := s.Fleet
	f.mu.Lock()
	defer f.mu.Unlock()
	if s.closed.Load() {
		return
	}
	if f.active[id] {
		if f.reconcile[id] == nil {
			f.reconcile[id] = map[int]bool{}
		}
		p, _ := f.read(id)
		for i, r := range p.Runs {
			if r.State == "ambiguous" || r.State == "running" || r.State == "dispatching" {
				f.reconcile[id][i] = true
			}
		}
		return
	}
	if !s.BeginRequest() {
		return
	}
	f.active[id] = true
	go func() {
		defer s.EndRequest()
		defer func() {
			f.mu.Lock()
			delete(f.active, id)
			p, _ := f.read(id)
			again := !f.failed && (p.State == "running" || len(f.reconcile[id]) > 0) && s.observationCtx.Err() == nil
			f.mu.Unlock()
			if again {
				s.startFleet(id)
			}
		}()
		s.runFleet(id)
	}()
}
func (s *Service) runFleet(id string) {
	f := s.Fleet
	workers := map[int]bool{}
	observed := map[int]bool{}
	done := make(chan int, FleetMaxTargets)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		f.mu.Lock()
		next := f.snapshot()
		p, ok := next[id]
		if !ok {
			f.mu.Unlock()
			return
		}
		before := fleetHash(p)
		if s.observationCtx.Err() == nil && !f.failed {
			fleetAdvance(&p, time.Now())
		}
		if p.State == "canceled" {
			s.cancelFleetStarts(p)
		}
		launch := []struct {
			index int
			fresh bool
		}{}
		for i := range f.reconcile[id] {
			if !workers[i] {
				observed[i] = false
				delete(f.reconcile[id], i)
			}
		}
		for i, r := range p.Runs {
			if !workers[i] && !observed[i] && (r.State == "running" || r.State == "dispatching" || r.State == "ambiguous") {
				workers[i] = true
				observed[i] = true
				launch = append(launch, struct {
					index int
					fresh bool
				}{i, false})
			}
		}
		if s.observationCtx.Err() == nil && !f.failed && p.State == "running" {
			if !s.fleetApprovalValid(p) {
				p.State = "paused"
				p.Reason = "approval_or_policy_changed"
			} else {
				count := 0
				for _, r := range p.Runs {
					if r.State == "running" || r.State == "dispatching" {
						count++
					}
				}
				for i := 0; i < p.WaveEnd && count < p.Spec.Rollout.MaxParallel; i++ {
					r := &p.Runs[i]
					if r.State != "pending" {
						continue
					}
					if !s.fleetHostAdmission(p.Owner, r.Host.HostID, p.ApprovalPolicy) {
						p.State = "paused"
						p.Reason = "permission_changed"
						break
					}
					_, err := s.FleetDispatchIdentity(r.Host, r.Alias)
					if err != nil {
						r.State = "skipped"
						r.Reason = "target_changed"
						continue
					}
					if !s.fleetCapacity(next, p, *r) || !s.fleetTurn(next, p, *r) {
						continue
					}
					f.lastOwner = p.Owner.Key()
					f.lastPlan[p.Owner.Key()] = p.PlanID
					r.State = "dispatching"
					r.PolicyDigest = p.ApprovalPolicy
					r.ApprovalRef = p.ApprovalRef
					count++
					workers[i] = true
					observed[i] = true
					launch = append(launch, struct {
						index int
						fresh bool
					}{i, true})
					next[id] = p
				}
			}
		}
		if fleetHash(p) != before {
			p.UpdatedAt = time.Now()
			fleetAggregate(&p)
			next[id] = p
			if err := f.commit(next); err != nil {
				// Failed intent persistence is never a dispatch licence.
				for _, x := range launch {
					delete(workers, x.index)
				}
				launch = nil
			}
		}
		f.mu.Unlock()
		for _, x := range launch {
			go func(i int, fresh bool) { defer func() { done <- i }(); s.runFleetHost(id, i, fresh) }(x.index, x.fresh)
		}
		if len(workers) == 0 && (p.State != "running" || s.observationCtx.Err() != nil || frozenFleet(f)) {
			return
		}
		select {
		case i := <-done:
			delete(workers, i)
		case <-ticker.C:
		}
	}
}
func frozenFleet(f *FleetStore) bool { f.mu.Lock(); defer f.mu.Unlock(); return f.failed }
func (s *Service) setFleetRun(id string, index int, r HostRun) {
	f := s.Fleet
	f.mu.Lock()
	defer f.mu.Unlock()
	next := f.snapshot()
	p, ok := next[id]
	if !ok || index >= len(p.Runs) {
		return
	}
	// A retry reservation is independent of observation and must not be erased.
	r.RetryPlanID = p.Runs[index].RetryPlanID
	p.Runs[index] = r
	p.UpdatedAt = time.Now()
	fleetAdvance(&p, time.Now())
	if p.State == "canceled" {
		s.cancelFleetStarts(p)
	}
	fleetAggregate(&p)
	next[id] = p
	if f.commitRecorded(next) == nil {
		s.fleetAudit(p, &r, r.State)
	}
}
func (s *Service) runFleetHost(id string, index int, fresh bool) {
	p, ok := s.Fleet.read(id)
	if !ok {
		return
	}
	r := p.Runs[index]
	ctx, cancel := context.WithCancel(s.observationCtx)
	defer cancel()
	ctx = s.ObserveRelease(ctx, AuditEvent{Owner: p.Owner.Key(), Operation: "fleet.execute", PlanRef: fleetHash(p.PlanID), HostRef: fleetHash(r.Host.HostID), Attempt: r.Attempt, OperationRef: OperationReference(Request{Wire: &proto.Request{OperationID: r.OperationID}}), TargetDigest: r.Host.TargetDigest, PolicyDigest: r.PolicyDigest, ApprovalID: r.ApprovalRef, DigestScope: "fleet", TargetScope: "fleet"})
	wire := &proto.Request{Op: proto.OpJobStart, ClientID: p.Owner.ClientID, ProjectID: p.Owner.ProjectID, OperationID: r.OperationID, Job: cloneFleet(p.Spec.Job)}
	var m *MutationIntent
	if fresh {
		// Cancellation applies to a queued/submitting start, never a submitted job.
		startCtx, startCancel := context.WithCancel(ctx)
		f := s.Fleet
		f.mu.Lock()
		current, _ := f.read(id)
		if current.State == "canceled" {
			f.mu.Unlock()
			startCancel()
			r.State = "canceled"
			r.Reason = "not_started"
			s.setFleetRun(id, index, r)
			return
		}
		f.cancelStarts[r.OperationID] = startCancel
		f.mu.Unlock()
		defer func() { f.mu.Lock(); delete(f.cancelStarts, r.OperationID); f.mu.Unlock(); startCancel() }()
		dispatchDigest, identityErr := s.FleetDispatchIdentity(r.Host, r.Alias)
		if identityErr != nil {
			r.State = "skipped"
			r.Reason = "target_changed"
			s.setFleetRun(id, index, r)
			return
		}
		// Fleet owns the immutable operation payload; DispatchMutation supplies the
		// same broker ledger and scheduler used by individual supervised jobs.
		ap := ApprovalPlan{Owner: p.Owner, Operation: proto.OpJobStart, Host: r.Alias, TargetDigest: dispatchDigest, RequestDigest: fleetHash(wire), PolicyDigest: r.PolicyDigest, ApprovalID: r.ApprovalRef}
		_, intent, err := s.DispatchMutation(startCtx, Request{Owner: p.Owner, Host: r.Alias, Operation: proto.OpJobStart, Wire: wire}, ap)
		m = intent
		f.mu.Lock()
		delete(f.cancelStarts, r.OperationID)
		f.mu.Unlock()
		startCancel()
		if err != nil && m == nil {
			r.State = "unreachable"
			r.Reason = "not_admitted"
			s.setFleetRun(id, index, r)
			return
		}
	} else {
		old, err := s.Mutations.Get(p.Owner.Key(), r.OperationID)
		if err != nil {
			// Fleet intent exists but no mutation intent: dispatch was never admitted.
			// Do not manufacture a second attempt during recovery.
			r.State = "ambiguous"
			r.Reason = "mutation_unavailable"
			if errors.Is(err, ErrMutationUnknown) {
				r.State = "unreachable"
				r.Reason = "intent_not_admitted"
			}
			s.setFleetRun(id, index, r)
			return
		}
		m = &old
	}
	if m == nil {
		r.State = "ambiguous"
		r.Reason = "mutation_unavailable"
		s.setFleetRun(id, index, r)
		return
	}
	bound := cloneFleet(wire)
	bound.Job.DurableStart = true
	expectedJob, _ := proto.JobIDForOperation(proto.PrincipalID(p.Owner.ClientID, p.Owner.ProjectID), r.OperationID)
	expectedDigest, digestErr := proto.DurableJobDigest(bound)
	if digestErr != nil || m.Host != r.Alias || m.Operation != proto.OpJobStart || m.RequestDigest != fleetHash(wire) || m.JobID != expectedJob || m.JobDigest != expectedDigest || m.PolicyDigest != r.PolicyDigest || m.ApprovalID != r.ApprovalRef {
		r.State = "ambiguous"
		r.Reason = "mutation_binding_conflict"
		s.setFleetRun(id, index, r)
		return
	}
	r.JobID, r.JobDigest = m.JobID, m.JobDigest
	if m.State == "not_sent" || m.State == "prepared" {
		r.State = "unreachable"
		r.Reason = "not_sent"
		current, _ := s.Fleet.read(id)
		if current.State == "canceled" {
			r.State = "canceled"
			r.Reason = "remaining_canceled"
		}
		s.setFleetRun(id, index, r)
		return
	}
	if m.State == "completed" && !m.RemoteOK {
		r.State = "failed"
		r.Reason = "start_rejected"
		s.setFleetRun(id, index, r)
		return
	}
	// Even a completed start is only a submitted job. Query its authenticated,
	// durable identity until terminal; no raw output is retained in Fleet state.
	r.State = "running"
	r.Reason = "job_submitted"
	s.setFleetRun(id, index, r)
	for {
		if ctx.Err() != nil {
			r.State = "ambiguous"
			r.Reason = "observation_interrupted"
			s.setFleetRun(id, index, r)
			return
		}
		if !s.DecideRequest(p.Owner, proto.OpJobStatus, r.Host.HostID).Allow {
			r.State = "ambiguous"
			r.Reason = "observation_permission_changed"
			s.setFleetRun(id, index, r)
			return
		}
		digest, err := s.FleetDispatchIdentity(r.Host, r.Alias)
		if err != nil {
			r.State = "ambiguous"
			r.Reason = "target_changed"
			s.setFleetRun(id, index, r)
			return
		}
		qctx, qcancel := context.WithTimeout(ctx, 10*time.Second)
		response, err := s.DispatchScheduled(qctx, r.Alias, p.Owner.Key(), LaneForOperation(proto.OpJobStatus), func(c context.Context) (*proto.Response, error) {
			return s.DispatchApproved(c, r.Alias, &proto.Request{Op: proto.OpJobStatus, ClientID: p.Owner.ClientID, ProjectID: p.Owner.ProjectID, Job: &proto.JobParams{ID: r.JobID}}, digest)
		})
		qcancel()
		if err != nil || response == nil || !response.OK || response.Job == nil || !mutationMatchesJob(*m, response.Job.Info) {
			r.State = "ambiguous"
			r.Reason = "result_unavailable"
			s.setFleetRun(id, index, r)
			return
		}
		info := response.Job.Info
		if m.State == "ambiguous" || m.State == "dispatched" {
			if err := s.Mutations.Transition(p.Owner.Key(), m.OperationID, "completed", true); err != nil {
				current, getErr := s.Mutations.Get(p.Owner.Key(), m.OperationID)
				if getErr != nil || current.State != "completed" || !current.RemoteOK || !sameMutationBinding(current, *m) {
					r.State = "ambiguous"
					r.Reason = "result_persistence_failed"
					s.setFleetRun(id, index, r)
					return
				}
			}
			m.State = "completed"
			m.RemoteOK = true
		}
		switch info.State {
		case proto.JobExited:
			r.ExitCode = &info.ExitCode
			r.State = "failed"
			r.Reason = "job_exit"
			if info.ExitCode == 0 {
				r.State = "success"
			}
			s.setFleetRun(id, index, r)
			return
		case proto.JobRunning:
			if info.Orphaned {
				r.State = "ambiguous"
				r.Reason = "orphaned"
				s.setFleetRun(id, index, r)
				return
			}
		default:
			r.State = "ambiguous"
			r.Reason = "job_outcome_unknown"
			s.setFleetRun(id, index, r)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
}
func (s *Service) fleetApprove(owner Owner, id, digest string, ttl int) (Approval, FleetPlan, error) {
	if ttl == 0 {
		ttl = 60
	}
	if ttl < 1 || ttl > 600 {
		return Approval{}, FleetPlan{}, errors.New("fleet approval ttl must be 1..600 seconds")
	}
	f := s.Fleet
	f.mu.Lock()
	defer f.mu.Unlock()
	next := f.snapshot()
	p, ok := next[id]
	if !ok || p.Owner != owner {
		return Approval{}, FleetPlan{}, errors.New("fleet plan unavailable")
	}
	if p.Digest != digest {
		return Approval{}, p, errors.New("fleet digest mismatch")
	}
	if p.State != "planned" && p.State != "paused" {
		return Approval{}, p, errors.New("fleet approval requires planned or paused state")
	}
	d := s.DecideRequest(owner, "fleet.execute", "")
	if !d.Allow {
		return Approval{}, p, errors.New("fleet execute permission required")
	}
	for _, r := range p.Runs {
		if r.State != "pending" {
			continue
		}
		if !s.fleetHostAllowed(owner, r.Host.HostID) {
			return Approval{}, p, errors.New("fleet target unavailable")
		}
		_, err := s.FleetDispatchIdentity(r.Host, r.Alias)
		if err != nil {
			return Approval{}, p, errors.New("fleet target changed")
		}
	}
	a, err := NewApproval(owner.Key(), "fleet.execute", p.Digest+":"+d.Digest, time.Duration(ttl)*time.Second)
	if err != nil {
		return a, p, err
	}
	p.ApprovalRef = ApprovalReference(a.Token)
	p.ApprovalExpiresAt = a.ExpiresAt
	p.ApprovalPolicy = d.Digest
	p.ApprovalConsumed = false
	p.UpdatedAt = time.Now()
	next[id] = p
	if err = f.commit(next); err != nil {
		return Approval{}, p, err
	}
	s.fleetAudit(p, nil, "approval_issued")
	return a, p, nil
}
func (s *Service) fleetControl(req Request) (FleetPlan, error) {
	f := s.Fleet
	f.mu.Lock()
	next := f.snapshot()
	p, ok := next[req.Fleet.PlanID]
	if !ok || p.Owner != req.Owner {
		f.mu.Unlock()
		return FleetPlan{}, errors.New("fleet plan unavailable")
	}
	run := false
	switch req.Operation {
	case "fleet.execute":
		if req.Fleet.Digest != p.Digest {
			f.mu.Unlock()
			return FleetPlan{}, errors.New("fleet digest mismatch")
		}
		if p.State == "running" || p.State == "completed" || p.State == "failed" || p.State == "canceled" {
			f.mu.Unlock()
			return p, nil
		}
		d := s.DecideRequest(req.Owner, "fleet.execute", "")
		if p.ApprovalConsumed || req.Approval == "" || ApprovalReference(req.Approval) != p.ApprovalRef || !time.Now().Before(p.ApprovalExpiresAt) || !d.Allow || d.Digest != p.ApprovalPolicy {
			f.mu.Unlock()
			return FleetPlan{}, errors.New("fleet approval required for exact plan and policy")
		}
		p.ApprovalConsumed = true
		p.State = "running"
		p.Reason = "execute"
		run = true
	case "fleet.resume":
		if p.State == "running" {
			f.mu.Unlock()
			s.startFleet(p.PlanID)
			return p, nil
		}
		if p.State != "paused" || !s.fleetApprovalValid(p) {
			f.mu.Unlock()
			return FleetPlan{}, errors.New("resume requires paused plan and valid consumed approval; approve and execute to renew")
		}
		p.State = "running"
		p.Reason = "resumed"
		run = true
	case "fleet.pause":
		if p.State == "running" {
			p.State = "paused"
			p.Reason = "operator_pause"
		}
	case "fleet.cancel":
		for _, r := range p.Runs {
			if cancel := f.cancelStarts[r.OperationID]; cancel != nil {
				cancel()
			}
		}
		if p.State != "completed" && p.State != "failed" {
			p.State = "canceled"
			p.Reason = "operator_cancel"
			fleetCancelPending(&p)
		}
		run = fleetHasUnresolved(p)
	case "fleet.reconcile":
		run = true
	}
	p.UpdatedAt = time.Now()
	fleetAggregate(&p)
	next[p.PlanID] = p
	var err error
	if req.Operation == "fleet.cancel" || req.Operation == "fleet.reconcile" || req.Operation == "fleet.pause" {
		err = f.commitRecorded(next)
	} else {
		err = f.commit(next)
	}
	f.mu.Unlock()
	if err != nil {
		return FleetPlan{}, err
	}
	s.fleetAudit(p, nil, p.Reason)
	if run {
		s.startFleet(p.PlanID)
	}
	return p, nil
}

// Detached jobs keep a Fleet execution reservation through their terminal
// result. Unknown submitted jobs retain it too. Transport requests additionally
// pass through Scheduler, preserving control/owner/host lane reservations.
func (s *Service) fleetCapacity(plans map[string]FleetPlan, p FleetPlan, r HostRun) bool {
	global, owner, host := 0, 0, 0
	for _, plan := range plans {
		for _, run := range plan.Runs {
			if run.State != "running" && run.State != "dispatching" && run.State != "ambiguous" {
				continue
			}
			global++
			if plan.Owner == p.Owner {
				owner++
			}
			if run.Host.TargetDigest == r.Host.TargetDigest {
				host++
			}
		}
	}
	q := s.config.Get().QoS.effective()
	globalLimit := min(laneLimits[LaneExec], max(1, (q.MaxActive-2)/2))
	ownerLimit := min(max(1, globalLimit-1), max(1, (q.PerOwner-1)/2))
	return global < globalLimit && owner < ownerLimit && host < max(1, (q.PerHost-2)/2)
}

// Called under Fleet.mu. Round-robin owner and plan admission occurs before
// Scheduler enqueue, so a plan cannot monopolize the detached-job reservations.
func (s *Service) fleetTurn(plans map[string]FleetPlan, p FleetPlan, r HostRun) bool {
	candidates := map[string][]string{}
	for _, candidate := range plans {
		if candidate.State != "running" || !s.fleetApprovalValid(candidate) {
			continue
		}
		active := 0
		for _, run := range candidate.Runs {
			if run.State == "running" || run.State == "dispatching" || run.State == "ambiguous" {
				active++
			}
		}
		if active >= candidate.Spec.Rollout.MaxParallel {
			continue
		}
		end := candidate.WaveEnd
		if end == 0 {
			switch candidate.Spec.Rollout.Strategy {
			case "all_at_once":
				end = len(candidate.Runs)
			case "canary":
				end = candidate.Spec.Rollout.Canary
			default:
				end = min(len(candidate.Runs), candidate.Spec.Rollout.WaveSize)
			}
		}
		for _, run := range candidate.Runs[:end] {
			if run.State == "pending" && s.fleetCapacity(plans, candidate, run) && s.fleetHostAllowed(candidate.Owner, run.Host.HostID) {
				candidates[candidate.Owner.Key()] = append(candidates[candidate.Owner.Key()], candidate.PlanID)
				break
			}
		}
	}
	owners := make([]string, 0, len(candidates))
	for owner := range candidates {
		owners = append(owners, owner)
	}
	sort.Strings(owners)
	choose := func(values []string, last string) string {
		if len(values) == 0 {
			return ""
		}
		for _, v := range values {
			if v > last {
				return v
			}
		}
		return values[0]
	}
	owner := choose(owners, s.Fleet.lastOwner)
	if owner != p.Owner.Key() {
		return false
	}
	ids := candidates[owner]
	sort.Strings(ids)
	return choose(ids, s.Fleet.lastPlan[owner]) == p.PlanID
}
func (s *Service) cancelFleetStarts(p FleetPlan) {
	for _, r := range p.Runs {
		if cancel := s.Fleet.cancelStarts[r.OperationID]; cancel != nil {
			cancel()
		}
	}
}
