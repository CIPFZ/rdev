package broker

import (
	"testing"
	"time"
)

// This reviewer regression forces one detached-job slot. The second plan must
// receive the next released slot while the 100-host plan is still unfinished;
// fairness of already enqueued RPCs alone cannot make this assertion pass.
func TestFleetAdmissionFairAcrossOwnersAndPlans(t *testing.T) {
	for _, sameOwner := range []bool{false, true} {
		name := "owners"
		if sameOwner {
			name = "plans"
		}
		t.Run(name, func(t *testing.T) {
			mock := newFleetMock()
			service := fleetHarness(t, 100, "", mock)
			if err := service.ReloadConfig(Config{MaxHosts: 128, IdleTTL: time.Minute, QoS: QoSConfig{MaxActive: 4, PerHost: 3, PerOwner: 2}}); err != nil {
				t.Fatal(err)
			}
			owner := Owner{ClientID: "fleet-competitor", ProjectID: "isolated"}
			if sameOwner {
				owner = fleetTestOwner
			}
			for _, host := range service.FleetInventorySnapshot().Records {
				fleetInventoryGrant(t, service, owner, host.HostID)
			}
			for _, operation := range []string{"fleet.execute", "fleet.approve"} {
				if err := service.Grant(owner, operation); err != nil {
					t.Fatal(err)
				}
			}
			large := fleetPlanForTest(t, service, fleetTestSpec("all_at_once"))
			smallSpec := fleetTestSpec("all_at_once")
			smallSpec.Selector = "alias=" + large.Runs[len(large.Runs)-1].Alias
			small, err := service.createFleetPlan(owner, smallSpec)
			if err != nil {
				t.Fatal(err)
			}
			mock.change(func() {
				for _, run := range large.Runs {
					mock.held[run.Alias] = true
				}
			})
			fleetStartForTest(t, service, large)
			running := awaitFleet(t, service, large.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 1 })
			fleetStartForTest(t, service, small)
			first := ""
			for _, run := range running.Runs {
				if run.State == "running" {
					first = run.Alias
				}
			}
			if first == "" || first == small.Runs[0].Alias {
				t.Fatal("fairness fixture did not isolate competing targets")
			}
			mock.change(func() { mock.held[first] = false })
			awaitFleet(t, service, small.PlanID, func(p FleetPlan) bool { return p.Counts["running"] == 1 })
			largeState, _ := service.Fleet.read(large.PlanID)
			if largeState.State != "running" || largeState.Counts["success"] != 1 || largeState.Counts["pending"] != 99 {
				t.Fatalf("large plan monopolized next reservation: %s %v", largeState.State, largeState.Counts)
			}
			mock.mu.Lock()
			peak := mock.peak
			mock.mu.Unlock()
			if peak != 1 {
				t.Fatalf("lifetime global reservation exceeded: %d", peak)
			}
			mock.assertOnce(t)
		})
	}
}

func TestFleetAmbiguousRetainsPlanLifetimeLimit(t *testing.T) {
	mock := newFleetMock()
	service := fleetHarness(t, 3, "", mock)
	spec := fleetTestSpec("all_at_once")
	spec.Rollout.MaxParallel = 1
	plan := fleetPlanForTest(t, service, spec)
	first := plan.Runs[0].Alias
	mock.change(func() { mock.held[first] = true; mock.unavailable[first] = true })
	fleetStartForTest(t, service, plan)
	awaitFleet(t, service, plan.PlanID, func(p FleetPlan) bool { return p.Counts["ambiguous"] == 1 })
	// The runner wakes every 100ms. Give it several admission turns while the
	// first detached job is provably still alive but its result cannot be read.
	deadline := time.Now().Add(350 * time.Millisecond)
	for time.Now().Before(deadline) {
		if mock.total() != 1 {
			t.Fatal("ambiguous submitted job released max_parallel reservation")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestFleetAmbiguousMissingJobProjectionRetainsGlobalLimit(t *testing.T) {
	mock := newFleetMock()
	service := fleetHarness(t, 2, "", mock)
	if err := service.ReloadConfig(Config{MaxHosts: 128, IdleTTL: time.Minute, QoS: QoSConfig{MaxActive: 4, PerHost: 3, PerOwner: 2}}); err != nil {
		t.Fatal(err)
	}
	plan := fleetPlanForTest(t, service, fleetTestSpec("all_at_once"))
	// This models durable Fleet dispatch intent before the job ID was copied
	// from a ledger that is now unavailable. The remote mutation may be live.
	plan.State = "paused"
	plan.Runs[0].State = "ambiguous"
	plan.Runs[0].JobID = ""
	plan.Runs[0].Reason = "mutation_unavailable"
	otherPlan := FleetPlan{Owner: Owner{ClientID: "another-fleet", ProjectID: "isolated"}}
	if service.fleetCapacity(map[string]FleetPlan{plan.PlanID: plan}, otherPlan, plan.Runs[1]) {
		t.Fatal("missing local job projection released an ambiguous lifetime reservation")
	}
}
