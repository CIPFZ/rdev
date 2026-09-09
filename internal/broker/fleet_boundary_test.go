package broker

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestFleetHandlerPermissionSnapshotAndSubstitutionBoundary(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 2, "", m)
	owner := fleetTestOwner
	if err := s.Grant(owner, "fleet.plan"); err != nil {
		t.Fatal(err)
	}
	inventory := s.FleetInventorySnapshot()
	hidden := inventory.Records[1]
	if err := s.GrantHost(owner, hidden.HostID, "", proto.OpJobStart, true); err != nil {
		t.Fatal(err)
	}
	call := func(op string, q FleetRequest) Response {
		return s.HandleFleet(context.Background(), Request{Owner: owner, Operation: op, Fleet: &q})
	}
	spec := fleetTestSpec("all_at_once")
	spec.Job.Spec.Env = map[string]string{"PRIVATE_ARGUMENT": "must-not-appear-in-audit"}
	planned := call("fleet.plan", FleetRequest{Spec: &spec})
	if !planned.OK || planned.Fleet == nil || planned.Fleet.Total != 1 || planned.Fleet.Runs[0].Host.HostID == hidden.HostID {
		t.Fatalf("partial authority leaked/expanded snapshot: %+v", planned)
	}
	p := *planned.Fleet
	for _, selector := range []string{"alias=" + inventory.Records[0].Aliases[0] + "," + hidden.Aliases[0], "alias=" + inventory.Records[0].Aliases[0] + ",nonexistent"} {
		spec.Selector = selector
		response := call("fleet.plan", FleetRequest{Spec: &spec})
		if response.OK || response.Fleet != nil || strings.Contains(response.Error, hidden.Aliases[0]) {
			t.Fatal("partial forbidden target leaked inventory or created plan")
		}
	}
	if r := call("fleet.inventory.list", FleetRequest{}); r.OK || r.Inventory != nil {
		t.Fatal("Fleet use conferred inventory management")
	}
	approved := call("fleet.approve", FleetRequest{PlanID: p.PlanID, Digest: p.Digest, TTLSeconds: 60})
	if !approved.OK || approved.Approval == nil {
		t.Fatalf("approval failed: %+v", approved)
	}
	base := Request{Owner: owner, Operation: "fleet.execute", Approval: approved.Approval.Token, Fleet: &FleetRequest{PlanID: p.PlanID, Digest: p.Digest}}
	cases := []Request{}
	q := cloneFleet(base)
	q.Capability = "exec"
	cases = append(cases, q)
	q = cloneFleet(base)
	q.Wire = &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"touch", "forbidden"}}}
	cases = append(cases, q)
	q = cloneFleet(base)
	q.Target = hidden.HostID
	cases = append(cases, q)
	q = cloneFleet(base)
	q.Fleet.Spec = &spec
	cases = append(cases, q)
	q = cloneFleet(base)
	q.ApprovalSpec = &ApprovalSpec{}
	cases = append(cases, q)
	q = cloneFleet(base)
	q.Risk = true
	cases = append(cases, q)
	for _, req := range cases {
		if r := s.HandleFleet(context.Background(), req); r.OK || r.Fleet != nil {
			t.Fatal("Fleet request accepted replacement fields")
		}
	}
	other := Owner{ClientID: owner.ClientID, ProjectID: "other"}
	for _, op := range []string{"fleet.plan", "fleet.execute", "fleet.approve"} {
		if err := s.Grant(other, op); err != nil {
			t.Fatal(err)
		}
	}
	for _, op := range []string{"fleet.status", "fleet.results", "fleet.pause", "fleet.cancel", "fleet.reconcile"} {
		r := s.HandleFleet(context.Background(), Request{Owner: other, Operation: op, Fleet: &FleetRequest{PlanID: p.PlanID}})
		if r.OK || r.Fleet != nil {
			t.Fatal("cross-project plan read/control accepted")
		}
	}
	page := s.HandleFleet(context.Background(), Request{Owner: other, Operation: "fleet.list", Fleet: &FleetRequest{}})
	if !page.OK || page.Fleets == nil || len(page.Fleets.Plans) != 0 {
		t.Fatal("list enumerated another project")
	}
	if m.total() != 0 {
		t.Fatal("negative routes created a remote side effect")
	}
	// The unrelated policy mutation above invalidates the earlier token too.
	if r := s.HandleFleet(context.Background(), base); r.OK {
		t.Fatal("approval ignored policy version replacement")
	}
	data, err := json.Marshal(s.Audit.QueryOwner(time.Time{}, owner.Key()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "must-not-appear-in-audit") {
		t.Fatal("audit copied argv/environment")
	}
}

func TestFleetOperationAndRolloutValidation(t *testing.T) {
	base := fleetTestSpec("waves")
	tests := map[string]func(*FleetSpec){
		"unsupported":           func(p *FleetSpec) { p.Operation = proto.OpExec },
		"argv_missing":          func(p *FleetSpec) { p.Job.Spec.Argv = nil },
		"caller_job_id":         func(p *FleetSpec) { p.Job.ID = "chosen" },
		"secret_names":          func(p *FleetSpec) { p.Job.Spec.Env = map[string]string{"TOKEN": "secret:name"} },
		"stdin":                 func(p *FleetSpec) { p.Job.Spec.Stdin = "ignored-input" },
		"exec_timeout":          func(p *FleetSpec) { p.Job.Spec.TimeoutSec = 1 },
		"negative_parallel":     func(p *FleetSpec) { p.Rollout.MaxParallel = -1 },
		"excess_parallel":       func(p *FleetSpec) { p.Rollout.MaxParallel = 17 },
		"negative_wave":         func(p *FleetSpec) { p.Rollout.WaveSize = -1 },
		"excess_wave":           func(p *FleetSpec) { p.Rollout.WaveSize = 129 },
		"canary_conflict":       func(p *FleetSpec) { p.Rollout.Canary = 1 },
		"all_conflict":          func(p *FleetSpec) { p.Rollout.Strategy = "all_at_once"; p.Rollout.WaveSize = 10 },
		"excess_pause":          func(p *FleetSpec) { p.Rollout.PauseBetweenWavesSec = 3601 },
		"negative_failure":      func(p *FleetSpec) { n := -1; p.Rollout.MaxFailures = &n },
		"excess_failure_ratio":  func(p *FleetSpec) { r := 1.01; p.Rollout.MaxFailureRatio = &r },
		"unsupported_threshold": func(p *FleetSpec) { p.Rollout.OnThreshold = "continue" },
		"wall_unbounded":        func(p *FleetSpec) { p.Job.Resources = &proto.ResourceEnvelope{WallTimeoutSec: -1} },
	}
	for name, edit := range tests {
		t.Run(name, func(t *testing.T) {
			p := cloneFleet(base)
			edit(&p)
			if _, err := normalizeFleetSpec(p); err == nil {
				t.Fatal("invalid Fleet input accepted")
			}
		})
	}
	normalized, err := normalizeFleetSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Rollout.MaxParallel != 4 || normalized.Rollout.WaveSize != 10 || normalized.Job.Resources.WallTimeoutSec != 3600 {
		t.Fatal("Fleet defaults differ from shared timeout contract")
	}
	m := newFleetMock()
	s := fleetHarness(t, 129, "", m)
	for _, selector := range []string{"", "all", "label:none=present"} {
		p := cloneFleet(base)
		p.Selector = selector
		if _, err := s.createFleetPlan(fleetTestOwner, p); err == nil {
			t.Fatal("empty/oversized/zero-match selector admitted")
		}
	}
	if len(s.Fleet.snapshot()) != 0 || m.total() != 0 {
		t.Fatal("invalid selector left an executable plan")
	}
}

func TestFleetInventoryDiscoveryUsesOnePolicySnapshot(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 1, "", m)
	host := s.FleetInventorySnapshot().Records[0]
	key := fleetTestOwner.Key()
	startOnly := map[string]bool{"fleet.plan": true, hostGrantKey(host.HostID, CapabilityForOperation(proto.OpJobStart), proto.OpJobStart): true}
	statusOnly := map[string]bool{"fleet.plan": true, hostGrantKey(host.HostID, CapabilityForOperation(proto.OpJobStatus), proto.OpJobStatus): true}
	publish := func(grants map[string]bool) {
		s.policy.mu.Lock()
		s.policy.grants[key] = grants
		s.policy.digest = policyDigest(s.policy.grants)
		s.policy.mu.Unlock()
	}
	publish(startOnly)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			publish(statusOnly)
			publish(startOnly)
		}
	}()
	leaked := false
	for range 4000 {
		if hosts, err := s.ResolveFleetTargets(fleetTestOwner, "all"); err == nil && len(hosts) > 0 {
			leaked = true
			break
		}
	}
	close(stop)
	<-done
	if leaked {
		t.Fatal("discovery combined mutually exclusive policy versions into a usable target")
	}
}
