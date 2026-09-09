package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

// These regressions were added by a reviewer who did not author the Fleet
// engine. The synthetic boundary represents a crash after durable dispatch,
// without relying on a sleep to hit a narrow persistence window.
func fleetReviewRecordDispatch(t *testing.T, s *Service, p FleetPlan) MutationIntent {
	t.Helper()
	_, approved, err := s.fleetApprove(p.Owner, p.PlanID, p.Digest, 600)
	if err != nil {
		t.Fatal(err)
	}
	p = approved
	r := &p.Runs[0]
	r.PolicyDigest = p.ApprovalPolicy
	r.ApprovalRef = p.ApprovalRef
	wire := &proto.Request{Op: proto.OpJobStart, ClientID: p.Owner.ClientID, ProjectID: p.Owner.ProjectID, OperationID: r.OperationID, Job: cloneFleet(p.Spec.Job)}
	ap := ApprovalPlan{Owner: p.Owner, Operation: proto.OpJobStart, Host: r.Alias, TargetDigest: r.DispatchDigest, RequestDigest: fleetHash(wire), PolicyDigest: r.PolicyDigest, ApprovalID: p.ApprovalRef}
	_, intent, _ := s.DispatchMutation(t.Context(), Request{Owner: p.Owner, Host: r.Alias, Operation: proto.OpJobStart, Wire: wire}, ap)
	if intent == nil {
		t.Fatal("fixture did not record a dispatched mutation")
	}
	r.State, p.State, p.ApprovalConsumed, p.WaveEnd = "dispatching", "paused", true, 1
	fleetAggregate(&p)
	s.Fleet.mu.Lock()
	next := s.Fleet.snapshot()
	next[p.PlanID] = p
	err = s.Fleet.commit(next)
	s.Fleet.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	return *intent
}

func TestFleetReviewUnavailableLedgerCannotAuthorizeRetry(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	fleetReviewRecordDispatch(t, s, p)
	if m.total() != 1 {
		t.Fatal("fixture did not produce one real mock side effect")
	}
	// A durability-uncertain ledger cannot distinguish absence from a sent
	// mutation. It must never become an unreachable, retryable host.
	s.Mutations.mu.Lock()
	s.Mutations.failed = true
	s.Mutations.mu.Unlock()
	s.runFleetHost(p.PlanID, 0, false)
	got, _ := s.Fleet.read(p.PlanID)
	if got.Runs[0].State != "ambiguous" {
		t.Fatalf("ledger failure reclassified an executed mutation as %q", got.Runs[0].State)
	}
	if _, err := s.fleetRetry(p.Owner, p.PlanID, []string{p.Runs[0].Host.HostID}); err == nil {
		t.Fatal("ledger failure authorized a new attempt")
	}
	if m.total() != 1 {
		t.Fatal("recovery replayed the start")
	}
}

func TestFleetReviewRemoteResultClosesDispatchedLedger(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	intent := fleetReviewRecordDispatch(t, s, p)
	// Model remote success followed by failure to commit the local outcome.
	// This is still a valid ledger state before a process restart normalizes it.
	s.Mutations.mu.Lock()
	key := mutationKey{intent.Owner, intent.OperationID}
	recorded := s.Mutations.records[key]
	recorded.State, recorded.RemoteOK = "dispatched", false
	s.Mutations.records[key] = recorded
	s.Mutations.mu.Unlock()
	s.runFleetHost(p.PlanID, 0, false)
	got, _ := s.Fleet.read(p.PlanID)
	ledger, err := s.Mutations.Get(intent.Owner, intent.OperationID)
	if err != nil || got.Runs[0].State != "success" || ledger.State != "completed" || !ledger.RemoteOK {
		t.Fatalf("authenticated remote result not durably reconciled: host=%s ledger=%+v err=%v", got.Runs[0].State, ledger, err)
	}
	if m.total() != 1 {
		t.Fatal("result reconciliation replayed a successful start")
	}
}

func TestFleetReviewReconciledRunningJobCanLaterFinish(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	alias := p.Runs[0].Alias
	m.change(func() { m.loseACK[alias], m.held[alias] = true, true })
	intent := fleetReviewRecordDispatch(t, s, p)
	if intent.State != "ambiguous" {
		t.Fatal("fixture did not lose submission acknowledgement")
	}
	done := make(chan struct{})
	go func() { defer close(done); s.runFleetHost(p.PlanID, 0, false) }()
	awaitFleet(t, s, p.PlanID, func(FleetPlan) bool {
		ledger, err := s.Mutations.Get(intent.Owner, intent.OperationID)
		return err == nil && ledger.State == "completed"
	})
	m.change(func() { m.held[alias] = false })
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("reconciled job observation did not complete")
	}
	got, _ := s.Fleet.read(p.PlanID)
	if got.Runs[0].State != "success" || m.total() != 1 {
		t.Fatalf("second result poll lost completed ledger or replayed: state=%s reason=%s", got.Runs[0].State, got.Runs[0].Reason)
	}
}

func TestFleetReviewRestartGenerationDoesNotChangeApprovedTarget(t *testing.T) {
	root := t.TempDir()
	m := newFleetMock()
	s := fleetHarness(t, 1, root, m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	m.change(func() { m.loseACK[p.Runs[0].Alias] = true })
	intent := fleetReviewRecordDispatch(t, s, p)
	if intent.State != "ambiguous" {
		t.Fatalf("lost ACK fixture state=%s", intent.State)
	}
	fleetStopForTest(t, s)

	// A newly imported ordinary host precedes this alias after restart. The
	// process-local generation changes, while connection/session identity does
	// not. The immutable original plan and approval remain unchanged.
	resumed := NewService(nil)
	t.Cleanup(func() { _ = resumed.Close(context.Background()) })
	for _, h := range []transport.Host{{Name: "added-before", Addr: "unrelated.invalid"}, {Name: "logical-000", Addr: "logical-000.invalid"}} {
		if err := resumed.Client().Hosts.Add(h); err != nil {
			t.Fatal(err)
		}
	}
	for _, configure := range []struct {
		fn   func(string) error
		file string
	}{{resumed.ConfigureFleetInventory, "inventory.json"}, {resumed.Mutations.ConfigurePersistence, "mutations.json"}, {resumed.Jobs.ConfigurePersistence, "jobs.json"}, {resumed.ConfigureFleet, "plans.json"}} {
		if err := configure.fn(filepath.Join(root, configure.file)); err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range resumed.FleetInventorySnapshot().Records {
		fleetInventoryGrant(t, resumed, p.Owner, h.HostID)
	}
	for _, operation := range []string{"fleet.execute", "fleet.approve"} {
		if err := resumed.Grant(p.Owner, operation); err != nil {
			t.Fatal(err)
		}
	}
	resumed.SetDispatcher(m.dispatch)
	current, err := resumed.FleetDispatchIdentity(p.Runs[0].Host, p.Runs[0].Alias)
	if err != nil || current == p.Runs[0].DispatchDigest {
		t.Fatalf("fixture did not change only process generation: %v", err)
	}
	resumed.runFleetHost(p.PlanID, 0, false)
	got, _ := resumed.Fleet.read(p.PlanID)
	ledger, err := resumed.Mutations.Get(intent.Owner, intent.OperationID)
	if err != nil || got.Runs[0].State != "success" || ledger.State != "completed" || !ledger.RemoteOK {
		t.Fatalf("canonical target recovery failed: host=%s reason=%s ledger=%s err=%v", got.Runs[0].State, got.Runs[0].Reason, ledger.State, err)
	}
	if got.Digest != p.Digest || got.Runs[0].DispatchDigest != p.Runs[0].DispatchDigest || got.Runs[0].OperationID != p.Runs[0].OperationID || m.total() != 1 {
		t.Fatal("generation recovery changed approved identity or replayed mutation")
	}
	// A real connection change remains denied even though generation rebasing
	// is now allowed at the actual dispatch sink.
	if err := resumed.Client().Hosts.Add(transport.Host{Name: p.Runs[0].Alias, Addr: "replacement.invalid"}); err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.FleetDispatchIdentity(p.Runs[0].Host, p.Runs[0].Alias); !errors.Is(err, ErrFleetTargetChanged) {
		t.Fatalf("canonical mismatch accepted: %v", err)
	}
}

func TestFleetReviewRejectsUnsupportedStdin(t *testing.T) {
	spec := fleetTestSpec("all_at_once")
	spec.Job.Spec.Stdin = "not in the Fleet operation allowlist"
	if _, err := normalizeFleetSpec(spec); err == nil {
		t.Fatal("broker accepted stdin prohibited by FleetSpec schema")
	}
}

func TestFleetReviewRejectsIncompletePersistentTransitions(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	for name, mutate := range map[string]func(*FleetPlan){
		"completed-with-pending": func(v *FleetPlan) { v.State = "completed" },
		"running-without-admission": func(v *FleetPlan) {
			v.State, v.Runs[0].State = "running", "running"
		},
		"success-without-result": func(v *FleetPlan) {
			v.State, v.Runs[0].State = "completed", "success"
		},
		"consumed-without-approval": func(v *FleetPlan) { v.ApprovalConsumed = true },
	} {
		t.Run(name, func(t *testing.T) {
			broken := cloneFleet(p)
			mutate(&broken)
			fleetAggregate(&broken)
			if err := validateFleetPlan(broken); err == nil {
				t.Fatal("incomplete durable state accepted")
			}
		})
	}
}

func TestFleetReviewPartialInventoryRPCDoesNotDeleteTargets(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	if err := s.Grant(fleetTestOwner, "fleet.inventory.update"); err != nil {
		t.Fatal(err)
	}
	original := s.FleetInventorySnapshot()
	for name, mutate := range map[string]func(*FleetInventory){
		"missing-records":     func(v *FleetInventory) { v.Records = nil },
		"missing-retired-ids": func(v *FleetInventory) { v.RetiredIDs = nil },
		"missing-labels":      func(v *FleetInventory) { v.Records[0].Labels = nil },
	} {
		t.Run(name, func(t *testing.T) {
			partial := cloneFleetInventory(original)
			mutate(&partial)
			response := s.HandleFleet(t.Context(), Request{Owner: fleetTestOwner, Operation: "fleet.inventory.update", Fleet: &FleetRequest{Revision: original.Revision, Inventory: &partial}})
			if response.OK {
				t.Fatal("partial administrator document was accepted as replacement inventory")
			}
			if fleetHash(s.FleetInventorySnapshot()) != fleetHash(original) {
				t.Fatal("partial update changed authoritative inventory")
			}
		})
	}
}

func TestFleetReviewCancelQueuedStartKeepsNoSideEffect(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	if err := s.ReloadConfig(Config{MaxHosts: 128, IdleTTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	block, release := context.WithCancel(t.Context())
	defer release()
	for range 3 {
		scheduleBlock(s.Scheduler, block, "ordinary-request-host", fleetTestOwner.Key(), LaneExec, nil)
	}
	awaitScheduler(t, s.Scheduler, fleetTestOwner.Key(), 3, 0)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	fleetStartForTest(t, s, p)
	defer func() {
		if t.Failed() {
			latest, _ := s.Fleet.read(p.PlanID)
			t.Logf("queued cancellation fixture plan=%s reason=%s counts=%v starts=%d", latest.State, latest.Reason, latest.Counts, m.total())
		}
	}()
	awaitScheduler(t, s.Scheduler, fleetTestOwner.Key(), 3, 1)
	fleetCommandForTest(t, s, p, "cancel")
	got := awaitFleet(t, s, p.PlanID, func(v FleetPlan) bool {
		state := v.Runs[0].State
		return state != "pending" && state != "dispatching" && state != "running"
	})
	release()
	awaitScheduler(t, s.Scheduler, fleetTestOwner.Key(), 0, 0)
	if m.total() != 0 {
		t.Fatal("batch cancel let an unsent queued mutation execute")
	}
	if got.Runs[0].State != "canceled" || got.Counts["canceled"] != 1 {
		t.Fatalf("known queued cancellation misclassified: state=%s reason=%s", got.Runs[0].State, got.Runs[0].Reason)
	}
}

func TestFleetReviewRecoveryRejectsDifferentMutationBinding(t *testing.T) {
	for name, mutate := range map[string]func(*MutationIntent){
		"different-host":    func(v *MutationIntent) { v.Host = "other-host" },
		"different-request": func(v *MutationIntent) { v.RequestDigest = fleetHash("different approved command") },
	} {
		t.Run(name, func(t *testing.T) {
			m := newFleetMock()
			s := fleetHarness(t, 1, "", m)
			p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
			intent := fleetReviewRecordDispatch(t, s, p)
			// Existing single-host callers can also supply an operation ID. A
			// recovery lookup must bind the resulting record to this plan, not
			// merely trust a matching owner + operation ID pair.
			s.Mutations.mu.Lock()
			key := mutationKey{intent.Owner, intent.OperationID}
			other := s.Mutations.records[key]
			mutate(&other)
			s.Mutations.records[key] = other
			s.Mutations.mu.Unlock()
			s.runFleetHost(p.PlanID, 0, false)
			got, _ := s.Fleet.read(p.PlanID)
			if got.Runs[0].State != "ambiguous" {
				t.Fatalf("different mutation binding accepted as plan result: %s", got.Runs[0].State)
			}
			if m.total() != 1 {
				t.Fatal("binding conflict caused a new dispatch")
			}
		})
	}
}

func TestFleetReviewConcurrentObserverReconciliationIsIdempotent(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	m.change(func() { m.loseACK[p.Runs[0].Alias] = true })
	intent := fleetReviewRecordDispatch(t, s, p)
	var once sync.Once
	s.SetDispatcher(func(ctx context.Context, host string, request *proto.Request) (*proto.Response, error) {
		response, err := m.dispatch(ctx, host, request)
		if request.Op == proto.OpJobStatus && err == nil {
			once.Do(func() {
				// Model an independent authorized job status/wait observer
				// committing the same authenticated result before Fleet.
				if transitionErr := s.Mutations.Transition(intent.Owner, intent.OperationID, "completed", true); transitionErr != nil {
					t.Errorf("independent observer fixture: %v", transitionErr)
				}
			})
		}
		return response, err
	})
	s.runFleetHost(p.PlanID, 0, false)
	got, _ := s.Fleet.read(p.PlanID)
	if got.Runs[0].State != "success" || m.total() != 1 {
		t.Fatalf("successful concurrent reconciliation became uncertain: state=%s reason=%s", got.Runs[0].State, got.Runs[0].Reason)
	}
}

func TestFleetReviewRejectsBrokenRetryHistory(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	m.change(func() { m.exits[p.Runs[0].Alias] = 7 })
	fleetStartForTest(t, s, p)
	awaitFleet(t, s, p.PlanID, func(v FleetPlan) bool { return v.State == "failed" })
	child, err := s.fleetRetry(p.Owner, p.PlanID, []string{p.Runs[0].Host.HostID})
	if err != nil {
		t.Fatal(err)
	}
	original := s.Fleet.snapshot()
	if err := validateFleetLinks(original); err != nil {
		t.Fatalf("valid retry history rejected: %v", err)
	}
	for name, mutate := range map[string]func(map[string]FleetPlan){
		"missing-parent": func(plans map[string]FleetPlan) { delete(plans, p.PlanID) },
		"missing-child":  func(plans map[string]FleetPlan) { delete(plans, child.PlanID) },
		"cross-owner": func(plans map[string]FleetPlan) {
			changed := plans[child.PlanID]
			changed.Owner.ProjectID = "other-project"
			changed.Digest = fleetPlanDigest(changed)
			plans[child.PlanID] = changed
		},
		"wrong-attempt": func(plans map[string]FleetPlan) {
			changed := plans[child.PlanID]
			changed.Runs[0].Attempt++
			changed.Digest = fleetPlanDigest(changed)
			plans[child.PlanID] = changed
		},
		"changed-operation": func(plans map[string]FleetPlan) {
			changed := plans[child.PlanID]
			changed.Spec.Job.Spec.Argv = []string{"a different mutation"}
			changed.Digest = fleetPlanDigest(changed)
			plans[child.PlanID] = changed
		},
	} {
		t.Run(name, func(t *testing.T) {
			broken := cloneFleet(original)
			mutate(broken)
			if err := validateFleetLinks(broken); err == nil {
				t.Fatal("broken retry ancestry accepted")
			}
			// Exercise the durable reader as well as the relation validator.
			snapshot := fleetSnapshot{Schema: FleetSchemaVersion, Plans: []FleetPlan{}}
			for _, plan := range broken {
				snapshot.Plans = append(snapshot.Plans, plan)
			}
			data, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "plans.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			loaded := NewService(nil)
			defer loaded.Close(context.Background())
			if err := loaded.ConfigureFleet(path); err == nil {
				t.Fatal("durable reader published broken retry links")
			}
			retained, err := os.ReadFile(path)
			if err != nil || string(retained) != string(data) {
				t.Fatal("rejected retry history was overwritten")
			}
		})
	}
}
