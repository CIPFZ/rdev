package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/CIPFZ/rdev/internal/proto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReviewExpiredRetryComponentSurvivesNextRetry(t *testing.T) {
	root := t.TempDir()
	s := fleetHarness(t, 1, root, newFleetMock())
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	s.Fleet.mu.Lock()
	next := s.Fleet.snapshot()
	p.State = "canceled"
	p.Runs[0].State = "canceled"
	next[p.PlanID] = p
	if err := s.Fleet.commit(next); err != nil {
		t.Fatal(err)
	}
	s.Fleet.mu.Unlock()
	child, err := s.fleetRetry(p.Owner, p.PlanID, []string{p.Runs[0].Host.HostID})
	if err != nil {
		t.Fatal(err)
	}
	s.Fleet.mu.Lock()
	next = s.Fleet.snapshot()
	for id, plan := range next {
		plan.State = "canceled"
		plan.Runs[0].State = "canceled"
		plan.UpdatedAt = time.Now().Add(-8 * 24 * time.Hour)
		next[id] = plan
	}
	if err := s.Fleet.commit(next); err != nil {
		t.Fatal(err)
	}
	s.Fleet.mu.Unlock()
	grandchild, err := s.fleetRetry(p.Owner, child.PlanID, []string{child.Runs[0].Host.HostID})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("grandchild created: %s, plans retained: %d", grandchild.PlanID, len(s.Fleet.snapshot()))
	if err := validateFleetLinks(s.Fleet.snapshot()); err != nil {
		t.Errorf("new retry lost its ancestor: %v", err)
	}
	loaded := NewService(nil)
	defer loaded.Close(context.Background())
	if err := loaded.ConfigureFleet(filepath.Join(root, "plans.json")); err != nil {
		t.Errorf("broker restart cannot load its committed plans: %v", err)
	}
}

func TestReviewFleetNearByteBudgetCanStillCancel(t *testing.T) {
	s := fleetHarness(t, 1, "", newFleetMock())
	base := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	plans := []FleetPlan{}
	var last FleetPlan
	encoded := func() []byte {
		b, err := json.Marshal(fleetSnapshot{Schema: FleetSchemaVersion, Plans: plans})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for n := 0; n < FleetMaxPlans; n++ {
		p := cloneFleet(base)
		p.PlanID = fmt.Sprintf("%032x", n+1000)
		p.Owner = Owner{ClientID: fmt.Sprintf("budget-owner-%d", n), ProjectID: "review"}
		p.Runs = nil
		for i := 0; i < FleetMaxTargets; i++ {
			r := cloneFleet(base.Runs[0])
			r.OperationID, _ = proto.NewOperationID()
			r.Host.HostID = fmt.Sprintf("%032x", i+1)
			r.Host.TargetDigest = fmt.Sprintf("%064x", i+1)
			r.Alias = fmt.Sprintf("host-%d", i)
			r.Host.Aliases = []string{r.Alias}
			r.Host.Labels = map[string]string{}
			for j := 0; j < 32; j++ {
				r.Host.Labels[fmt.Sprintf("k%062d", j)] = strings.Repeat("v", 128)
			}
			p.Runs = append(p.Runs, r)
		}
		p.Digest = fleetPlanDigest(p)
		fleetAggregate(&p)
		plans = append(plans, p)
		if len(encoded()) > FleetMaxBytes {
			for len(encoded()) > FleetMaxBytes {
				p.Runs = p.Runs[:len(p.Runs)-1]
				p.Digest = fleetPlanDigest(p)
				fleetAggregate(&p)
				plans[len(plans)-1] = p
			}
			last = p
			break
		}
	}
	// Pad only the legal private job argv, keeping a one-byte margin.
	delta := FleetMaxBytes - 1 - len(encoded())
	if delta < 0 || delta > 15000 {
		t.Fatalf("unexpected padding: %d", delta)
	}
	last.Spec.Job.Spec.Argv[0] += strings.Repeat("a", delta)
	last.Digest = fleetPlanDigest(last)
	plans[len(plans)-1] = last
	next := map[string]FleetPlan{}
	for _, p := range plans {
		if err := validateFleetPlan(p); err != nil {
			t.Fatal(err)
		}
		next[p.PlanID] = p
	}
	t.Logf("valid plans=%d bytes=%d max=%d", len(plans), len(encoded()), FleetMaxBytes)
	// Loading retained records may consume the admission headroom. This
	// valid near-hard-limit snapshot must load, while new admission below
	// must reject the same bytes. The disk reader is not plan admission.
	loadedPath := filepath.Join(t.TempDir(), "loaded-plans.json")
	if err := os.WriteFile(loadedPath, encoded(), 0600); err != nil {
		t.Fatal(err)
	}
	loaded := NewService(nil)
	defer loaded.Close(context.Background())
	if err := loaded.ConfigureFleet(loadedPath); err != nil {
		t.Fatalf("retained snapshot rejected as new admission: %v", err)
	}
	if len(loaded.Fleet.snapshot()) != len(plans) {
		t.Fatal("load lost retained plans")
	}
	s.Fleet.mu.Lock()
	err := s.Fleet.admit(next)
	s.Fleet.mu.Unlock()
	if err == nil {
		t.Fatal("new plans consumed space reserved for cancellation/results")
	}
	if len(s.Fleet.snapshot()) != 1 {
		t.Fatal("rejected admission changed existing plans")
	}
	// Remove complete new plans until admission fits, then prove cancellation
	// of every retained plan can consume the reserved lifecycle metadata.
	for err != nil && len(plans) > 1 {
		plans = plans[:len(plans)-1]
		next = map[string]FleetPlan{}
		for _, p := range plans {
			next[p.PlanID] = p
		}
		s.Fleet.mu.Lock()
		err = s.Fleet.admit(next)
		s.Fleet.mu.Unlock()
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range plans {
		_, err = s.fleetControl(Request{Owner: p.Owner, Operation: "fleet.cancel", Fleet: &FleetRequest{PlanID: p.PlanID}})
		if err != nil {
			t.Fatalf("reserved existing plan cannot cancel: %v", err)
		}
	}
	for _, p := range s.Fleet.snapshot() {
		if err := validateFleetPlan(p); err != nil {
			t.Fatal(err)
		}
		if p.State != "canceled" {
			t.Fatal("cancel lost")
		}
	}
}
