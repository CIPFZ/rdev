package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// Publish durable intent windows directly, then instantiate a fresh Service.
// This models crash-persistent records without pretending a Go shutdown is
// SIGKILL. The independent SSH harness tests the actual process crash boundary.
func TestFleetRecoveryEverySubmissionWindowNeverReplays(t *testing.T) {
	for _, window := range []string{"fleet_intent_only", "prepared_not_sent", "dispatched_unknown", "remote_success_local_missing", "mutation_complete_fleet_missing"} {
		t.Run(window, func(t *testing.T) {
			root := t.TempDir()
			m := newFleetMock()
			s := fleetHarness(t, 1, root, m)
			p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
			_, approved, err := s.fleetApprove(p.Owner, p.PlanID, p.Digest, 600)
			if err != nil {
				t.Fatal(err)
			}
			p = approved
			p.State = "paused"
			p.ApprovalConsumed = true
			p.Runs[0].State = "dispatching"
			p.Runs[0].PolicyDigest = p.ApprovalPolicy
			p.Runs[0].ApprovalRef = p.ApprovalRef
			r := p.Runs[0]
			plain := &proto.Request{Op: proto.OpJobStart, ClientID: p.Owner.ClientID, ProjectID: p.Owner.ProjectID, OperationID: r.OperationID, Job: cloneFleet(p.Spec.Job)}
			wire := cloneFleet(plain)
			wire.Job.DurableStart = true
			jobID, err := proto.JobIDForOperation(proto.PrincipalID(p.Owner.ClientID, p.Owner.ProjectID), r.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			digest, err := proto.DurableJobDigest(wire)
			if err != nil {
				t.Fatal(err)
			}
			intent := MutationIntent{Owner: p.Owner.Key(), OperationID: r.OperationID, Operation: proto.OpJobStart, Host: r.Alias, RequestDigest: fleetHash(plain), TargetDigest: r.DispatchDigest, PolicyDigest: p.ApprovalPolicy, ApprovalID: p.ApprovalRef, JobID: jobID, JobDigest: digest}
			if window != "fleet_intent_only" {
				if err := s.Mutations.Prepare(intent); err != nil {
					t.Fatal(err)
				}
			}
			if window != "fleet_intent_only" && window != "prepared_not_sent" {
				if err := s.Mutations.Transition(intent.Owner, intent.OperationID, "dispatched", false); err != nil {
					t.Fatal(err)
				}
			}
			if window == "remote_success_local_missing" || window == "mutation_complete_fleet_missing" {
				if _, err := m.dispatch(context.Background(), r.Alias, wire); err != nil {
					t.Fatal(err)
				}
				if window == "mutation_complete_fleet_missing" {
					if err := s.Mutations.Transition(intent.Owner, intent.OperationID, "completed", true); err != nil {
						t.Fatal(err)
					}
				}
			}
			s.Fleet.mu.Lock()
			next := s.Fleet.snapshot()
			next[p.PlanID] = p
			err = s.Fleet.commit(next)
			s.Fleet.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			before := m.total()
			fleetStopForTest(t, s)
			fresh := fleetHarness(t, 1, root, m)
			fresh.RecoverFleet()
			expected := "unreachable"
			if window == "dispatched_unknown" {
				expected = "ambiguous"
			}
			if window == "remote_success_local_missing" || window == "mutation_complete_fleet_missing" {
				expected = "success"
			}
			recovered := awaitFleet(t, fresh, p.PlanID, func(p FleetPlan) bool { return p.Runs[0].State == expected })
			if recovered.Runs[0].OperationID != r.OperationID || recovered.Runs[0].Attempt != 1 || m.total() != before {
				t.Fatal("recovery changed identity or repeated a mutation")
			}
			if expected == "ambiguous" {
				if _, err := fresh.fleetRetry(p.Owner, p.PlanID, []string{r.Host.HostID}); err == nil {
					t.Fatal("unknown result licensed a retry")
				}
			}
			m.assertOnce(t)
		})
	}
}

func TestFleetLostACKReconcileAndCancelDetachedJobs(t *testing.T) {
	t.Run("lost_ack_query_not_replay", func(t *testing.T) {
		m := newFleetMock()
		s := fleetHarness(t, 1, "", m)
		p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
		host := p.Runs[0].Alias
		m.change(func() { m.loseACK[host] = true; m.unavailable[host] = true })
		fleetStartForTest(t, s, p)
		awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["ambiguous"] == 1 })
		// Wait for the prior single-host observer to retire; repeated reconciliation
		// then starts an observer of the existing operation, never a new job.
		waitFleetInactive(t, s, p.PlanID)
		m.change(func() { m.unavailable[host] = false })
		fleetCommandForTest(t, s, p, "reconcile")
		recovered := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 1 })
		if recovered.Runs[0].OperationID != p.Runs[0].OperationID || m.total() != 1 {
			t.Fatal("lost ACK replayed marker")
		}
		m.assertOnce(t)
	})
	t.Run("cancel_keeps_submitted_jobs", func(t *testing.T) {
		m := newFleetMock()
		s := fleetHarness(t, 4, "", m)
		spec := fleetTestSpec("all_at_once")
		spec.Rollout.MaxParallel = 2
		p := fleetPlanForTest(t, s, spec)
		m.change(func() {
			for _, r := range p.Runs {
				m.held[r.Alias] = true
			}
		})
		fleetStartForTest(t, s, p)
		awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 2 })
		canceled := fleetCommandForTest(t, s, p, "cancel")
		if canceled.Counts["canceled"] != 2 || canceled.Counts["running"] != 2 || m.total() != 2 {
			t.Fatal("batch cancel lost submitted jobs")
		}
		m.change(func() {
			for _, r := range p.Runs {
				m.held[r.Alias] = false
			}
		})
		final := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 2 })
		if final.State != "canceled" || final.Counts["canceled"] != 2 || m.total() != 2 {
			t.Fatal("cancel resumed pending jobs or erased outcomes")
		}
		m.assertOnce(t)
	})
}

func waitFleetInactive(t *testing.T, s *Service, id string) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		s.Fleet.mu.Lock()
		active := s.Fleet.active[id]
		s.Fleet.mu.Unlock()
		if !active {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Fleet engine did not retire")
}

func TestFleetConcurrentRetryReservesOneChild(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	m.change(func() { m.exits[p.Runs[0].Alias] = 2 })
	fleetStartForTest(t, s, p)
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.State == "failed" })
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.fleetRetry(p.Owner, p.PlanID, []string{p.Runs[0].Host.HostID})
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		}
	}
	if accepted != 1 || len(s.Fleet.snapshot()) != 2 || m.total() != 1 {
		t.Fatalf("concurrent retries created %d reservations", accepted)
	}
}

func TestFleetExpiredAndRevokedAuthorizationStopsPending(t *testing.T) {
	for _, kind := range []string{"expiry", "host_permission", "policy_version"} {
		t.Run(kind, func(t *testing.T) {
			m := newFleetMock()
			s := fleetHarness(t, 2, "", m)
			spec := fleetTestSpec("all_at_once")
			spec.Rollout.MaxParallel = 1
			p := fleetPlanForTest(t, s, spec)
			m.change(func() { m.held[p.Runs[0].Alias] = true })
			fleetStartForTest(t, s, p)
			awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 1 })
			switch kind {
			case "expiry":
				s.Fleet.mu.Lock()
				next := s.Fleet.snapshot()
				changed := next[p.PlanID]
				changed.ApprovalExpiresAt = time.Now().Add(-time.Second)
				next[p.PlanID] = changed
				err := s.Fleet.commit(next)
				s.Fleet.mu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			case "host_permission":
				if err := s.GrantHost(p.Owner, p.Runs[1].Host.HostID, "", proto.OpJobStart, true); err != nil {
					t.Fatal(err)
				}
			case "policy_version":
				if err := s.Grant(p.Owner, "read_file"); err != nil {
					t.Fatal(err)
				}
			}
			awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.State == "paused" })
			m.change(func() { m.held[p.Runs[0].Alias] = false })
			done := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 1 })
			if done.Counts["pending"] != 1 || m.total() != 1 {
				t.Fatal("new admission ignored expiry/revocation or killed admitted job")
			}
			if _, err := s.fleetControl(Request{Owner: p.Owner, Operation: "fleet.resume", Fleet: &FleetRequest{PlanID: p.PlanID}}); err == nil {
				t.Fatal("resume ignored expired policy binding")
			}
		})
	}
}

func TestFleetResultPersistenceFailureNeverReplaysRemoteSuccess(t *testing.T) {
	root := t.TempDir()
	m := newFleetMock()
	s := fleetHarness(t, 1, root, m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	host := p.Runs[0].Alias
	m.change(func() { m.held[host] = true })
	fleetStartForTest(t, s, p)
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 1 })
	s.Fleet.mu.Lock()
	s.Fleet.persist = func(string, []byte) error { return errors.New("injected Fleet result fsync failure") }
	s.Fleet.mu.Unlock()
	m.change(func() { m.held[host] = false })
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		s.Fleet.mu.Lock()
		failed := s.Fleet.failed
		s.Fleet.mu.Unlock()
		if failed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.Fleet.mu.Lock()
	failed := s.Fleet.failed
	s.Fleet.mu.Unlock()
	if !failed {
		t.Fatal("fault did not reach result persistence window")
	}
	fleetStopForTest(t, s)
	fresh := fleetHarness(t, 1, root, m)
	fresh.RecoverFleet()
	done := awaitFleet(t, fresh, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 1 })
	if done.Runs[0].OperationID != p.Runs[0].OperationID || m.total() != 1 {
		t.Fatal("remote success replayed after local result loss")
	}
}

func TestFleetReconcileAmbiguousWhileAnotherHostRuns(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 2, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	unknown, held := p.Runs[0].Alias, p.Runs[1].Alias
	m.change(func() { m.loseACK[unknown] = true; m.unavailable[unknown] = true; m.held[held] = true })
	fleetStartForTest(t, s, p)
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["ambiguous"] == 1 && p.Counts["running"] == 1 })
	m.change(func() { m.unavailable[unknown] = false })
	fleetCommandForTest(t, s, p, "reconcile")
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 1 && p.Counts["running"] == 1 })
	if m.total() != 2 {
		t.Fatal("reconcile repeated the submitted mutation")
	}
	m.change(func() { m.held[held] = false })
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 2 })
	m.assertOnce(t)
}

func TestFleetQoSOwnerReservationAndSingleRequestProgress(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 4, "", m)
	// Exercise actual default reservation: only one detached run for this owner.
	if err := s.ReloadConfig(Config{MaxHosts: 128, IdleTTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	m.change(func() {
		for _, r := range p.Runs {
			m.held[r.Alias] = true
		}
	})
	fleetStartForTest(t, s, p)
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := s.DispatchScheduled(ctx, p.Runs[0].Alias, p.Owner.Key(), LaneControl, func(context.Context) (*proto.Response, error) { return &proto.Response{OK: true}, nil })
	if err != nil || resp == nil || !resp.OK {
		t.Fatal("Fleet starved ordinary control request", err)
	}
	// The normal execution lane also retains admission space under Fleet load.
	resp, err = s.DispatchScheduled(ctx, p.Runs[0].Alias, p.Owner.Key(), LaneExec, func(context.Context) (*proto.Response, error) { return &proto.Response{OK: true}, nil })
	if err != nil || resp == nil || !resp.OK {
		t.Fatal("Fleet starved single-host execution", err)
	}
	if m.total() != 1 {
		t.Fatal("Fleet bypassed owner detached reservation")
	}
	fleetCommandForTest(t, s, p, "cancel")
	m.change(func() {
		for _, r := range p.Runs {
			m.held[r.Alias] = false
		}
	})
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 1 })
}

func TestFleetCancelQueuedStartPreventsSideEffect(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	if err := s.ReloadConfig(Config{MaxHosts: 128, IdleTTL: time.Minute, QoS: QoSConfig{MaxActive: 3, PerHost: 3, PerOwner: 2}}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	busyDone := make(chan error, 1)
	go func() {
		_, err := s.DispatchScheduled(context.Background(), "occupied", fleetTestOwner.Key(), LaneExec, func(context.Context) (*proto.Response, error) {
			close(entered)
			<-release
			return &proto.Response{OK: true}, nil
		})
		busyDone <- err
	}()
	<-entered
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	fleetStartForTest(t, s, p)
	until := time.Now().Add(3 * time.Second)
	prepared := false
	for time.Now().Before(until) {
		intent, err := s.Mutations.Get(p.Owner.Key(), p.Runs[0].OperationID)
		if err == nil && intent.State == "prepared" {
			prepared = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !prepared {
		t.Fatal("Fleet start never reached queued durable intent")
	}
	fleetCommandForTest(t, s, p, "cancel")
	close(release)
	released = true
	if err := <-busyDone; err != nil {
		t.Fatal(err)
	}
	terminal := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Runs[0].State != "dispatching" })
	if m.total() != 0 {
		t.Fatal("batch cancel sent a queued mutation")
	}
	if terminal.Runs[0].State != "canceled" {
		t.Fatalf("known unsent cancellation was classified %s", terminal.Runs[0].State)
	}
}

func TestFleetMaxParallelCountsJobLifetimeAndPause(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 12, "", m)
	spec := fleetTestSpec("all_at_once")
	spec.Rollout.MaxParallel = 3
	p := fleetPlanForTest(t, s, spec)
	m.change(func() {
		for _, r := range p.Runs {
			m.held[r.Alias] = true
		}
	})
	fleetStartForTest(t, s, p)
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 3 })
	paused := fleetCommandForTest(t, s, p, "pause")
	if paused.State != "paused" || m.total() != 3 {
		t.Fatal("pause changed active jobs or admitted excess work")
	}
	m.change(func() {
		for _, r := range p.Runs[:3] {
			m.held[r.Alias] = false
		}
	})
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 3 })
	if m.total() != 3 {
		t.Fatal("paused plan started a new host after old job completion")
	}
	fleetCommandForTest(t, s, p, "resume")
	awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 3 && p.Counts["success"] == 3 })
	if m.total() != 6 {
		t.Fatal("resume repeated successful targets")
	}
	fleetCommandForTest(t, s, p, "cancel")
	m.change(func() {
		for _, r := range p.Runs {
			m.held[r.Alias] = false
		}
	})
	done := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.Counts["success"] == 6 })
	if done.Counts["canceled"] != 6 || m.total() != 6 {
		t.Fatal("cancel or resume expanded executed targets")
	}
	m.mu.Lock()
	peak := m.peak
	m.mu.Unlock()
	if peak != 3 {
		t.Fatalf("job lifetime max_parallel not enforced: peak %d", peak)
	}
	m.assertOnce(t)
}
