package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

// These are 100 independent logical targets in a deterministic dispatcher, not
// 100 real SSH machines. The separate runtime harness supplies SSH evidence.
type fleetMock struct {
	mu           sync.Mutex
	jobs         map[string]proto.JobInfo
	starts       map[string]int
	hostStarts   map[string]int
	held         map[string]bool
	unavailable  map[string]bool
	loseACK      map[string]bool
	exits        map[string]int
	active, peak int
}

func newFleetMock() *fleetMock {
	return &fleetMock{jobs: map[string]proto.JobInfo{}, starts: map[string]int{}, hostStarts: map[string]int{}, held: map[string]bool{}, unavailable: map[string]bool{}, loseACK: map[string]bool{}, exits: map[string]int{}}
}
func (m *fleetMock) dispatch(ctx context.Context, host string, q *proto.Request) (*proto.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch q.Op {
	case proto.OpJobStart:
		m.starts[q.OperationID]++
		m.hostStarts[host]++
		principal := proto.PrincipalID(q.ClientID, q.ProjectID)
		id, err := proto.JobIDForOperation(principal, q.OperationID)
		if err != nil {
			return nil, err
		}
		digest, err := proto.DurableJobDigest(q)
		if err != nil {
			return nil, err
		}
		if _, exists := m.jobs[id]; exists {
			return nil, errors.New("test detected repeated job start")
		}
		info := proto.JobInfo{ID: id, StartOperationID: q.OperationID, StartPrincipalID: principal, StartDigest: digest, State: proto.JobRunning, StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		m.jobs[id] = info
		m.active++
		m.peak = max(m.peak, m.active)
		if m.loseACK[host] {
			return nil, errors.New("injected lost start ACK after marker")
		}
		return &proto.Response{OK: true, Job: &proto.JobResult{Info: &info}}, nil
	case proto.OpJobStatus:
		if m.unavailable[host] {
			return nil, errors.New("injected unavailable result query")
		}
		info, exists := m.jobs[q.Job.ID]
		if !exists {
			return nil, errors.New("job not found")
		}
		if !m.held[host] && info.State == proto.JobRunning {
			info.State = proto.JobExited
			info.ExitCode = m.exits[host]
			info.Terminal = true
			m.active--
			m.jobs[info.ID] = info
		}
		return &proto.Response{OK: true, Job: &proto.JobResult{Info: &info}}, nil
	default:
		return nil, fmt.Errorf("unexpected mock operation %s", q.Op)
	}
}
func (m *fleetMock) change(fn func()) { m.mu.Lock(); defer m.mu.Unlock(); fn() }
func (m *fleetMock) total() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, v := range m.starts {
		n += v
	}
	return n
}
func (m *fleetMock) assertOnce(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, n := range m.starts {
		if n != 1 {
			t.Fatalf("operation %s dispatched %d times", id, n)
		}
	}
}

var fleetTestOwner = Owner{ClientID: "fleet-test", ProjectID: "isolated"}

func fleetHarness(t *testing.T, n int, root string, m *fleetMock) *Service {
	t.Helper()
	s := NewService(nil)
	// These tests exercise the requested Fleet limit; give the broker an explicit
	// larger envelope so its documented default owner reservation is not the bottleneck.
	if err := s.ReloadConfig(Config{MaxHosts: 128, IdleTTL: time.Minute, QoS: QoSConfig{MaxActive: 64, PerHost: 40, PerOwner: 32, MaxQueued: 256, PerOwnerQueued: 64}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.Close(ctx)
	})
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("logical-%03d", i)
		if err := s.Client().Hosts.Add(transport.Host{Name: name, Addr: name + ".invalid"}); err != nil {
			t.Fatal(err)
		}
	}
	if root != "" {
		if err := s.ConfigureFleetInventory(filepath.Join(root, "inventory.json")); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.FleetInventorySnapshot().Records) == 0 {
		if _, err := s.FleetInventoryImport(s.FleetInventorySnapshot().Revision); err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range s.FleetInventorySnapshot().Records {
		fleetInventoryGrant(t, s, fleetTestOwner, h.HostID)
	}
	for _, op := range []string{"fleet.execute", "fleet.approve"} {
		if err := s.Grant(fleetTestOwner, op); err != nil {
			t.Fatal(err)
		}
	}
	if root != "" {
		if err := s.Mutations.ConfigurePersistence(filepath.Join(root, "mutations.json")); err != nil {
			t.Fatal(err)
		}
		if err := s.Jobs.ConfigurePersistence(filepath.Join(root, "jobs.json")); err != nil {
			t.Fatal(err)
		}
		if err := s.ConfigureFleet(filepath.Join(root, "plans.json")); err != nil {
			t.Fatal(err)
		}
	}
	s.SetDispatcher(m.dispatch)
	return s
}
func fleetTestSpec(strategy string) FleetSpec {
	return FleetSpec{Selector: "all", Operation: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"isolated-marker"}}}, Rollout: FleetRollout{Strategy: strategy, MaxParallel: 4}}
}
func fleetPlanForTest(t *testing.T, s *Service, spec FleetSpec) FleetPlan {
	t.Helper()
	p, err := s.createFleetPlan(fleetTestOwner, spec)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func fleetStartForTest(t *testing.T, s *Service, p FleetPlan) FleetPlan {
	t.Helper()
	a, _, err := s.fleetApprove(p.Owner, p.PlanID, p.Digest, 600)
	if err != nil {
		t.Fatal(err)
	}
	out, err := s.fleetControl(Request{Owner: p.Owner, Operation: "fleet.execute", Approval: a.Token, Fleet: &FleetRequest{PlanID: p.PlanID, Digest: p.Digest}})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func fleetCommandForTest(t *testing.T, s *Service, p FleetPlan, action string) FleetPlan {
	t.Helper()
	out, err := s.fleetControl(Request{Owner: p.Owner, Operation: "fleet." + action, Fleet: &FleetRequest{PlanID: p.PlanID}})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func awaitFleet(t *testing.T, s *Service, id string, accept func(FleetPlan) bool) FleetPlan {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		p, ok := s.Fleet.read(id)
		if ok && accept(p) {
			return p
		}
		time.Sleep(5 * time.Millisecond)
	}
	p, _ := s.Fleet.read(id)
	t.Fatalf("Fleet condition not reached: state=%s reason=%s counts=%v", p.State, p.Reason, p.Counts)
	return p
}
func fleetStopForTest(t *testing.T, s *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFleet100LogicalHostsPartialFailureRestartCancelRetry(t *testing.T) {
	root := t.TempDir()
	m := newFleetMock()
	s := fleetHarness(t, 100, root, m)
	spec := fleetTestSpec("waves")
	spec.Rollout.WaveSize = 10
	spec.Rollout.PauseBetweenWavesSec = 3600
	p := fleetPlanForTest(t, s, spec)
	m.change(func() { m.exits[p.Runs[1].Alias] = 7; m.exits[p.Runs[3].Alias] = 9 })
	fleetStartForTest(t, s, p)
	first := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool {
		return p.Counts["success"] == 8 && p.Counts["failed"] == 2 && !p.NextWaveAt.IsZero()
	})
	if m.total() != 10 || first.WaveEnd != 10 {
		t.Fatal("next wave entered before durable delay")
	}
	fleetCommandForTest(t, s, p, "pause")
	fleetStopForTest(t, s)
	resumed := fleetHarness(t, 100, root, m)
	resumed.RecoverFleet()
	durable, _ := resumed.Fleet.read(p.PlanID)
	if durable.State != "paused" || !durable.NextWaveAt.Equal(first.NextWaveAt) || durable.Counts["failed"] != 2 {
		t.Fatalf("restart lost boundary/results: %+v", durable)
	}
	canceled := fleetCommandForTest(t, resumed, durable, "cancel")
	if canceled.Counts["canceled"] != 90 || canceled.Counts["success"] != 8 || canceled.Counts["failed"] != 2 || canceled.ExitStatus != 1 {
		t.Fatalf("cancel erased or reclassified work: %v", canceled.Counts)
	}
	if m.total() != 10 {
		t.Fatal("pause/restart/cancel dispatched pending targets")
	}
	child, err := resumed.fleetRetry(p.Owner, p.PlanID, []string{p.Runs[1].Host.HostID})
	if err != nil {
		t.Fatal(err)
	}
	if child.ParentPlanID != p.PlanID || len(child.Runs) != 1 || child.Runs[0].Attempt != 2 || child.Runs[0].OperationID == p.Runs[1].OperationID {
		t.Fatal("retry lost explicit attempt ancestry")
	}
	m.change(func() { m.exits[child.Runs[0].Alias] = 0 })
	fleetStartForTest(t, resumed, child)
	done := awaitFleet(t, resumed, child.PlanID, func(p FleetPlan) bool { return p.State == "completed" })
	if done.Counts["success"] != 1 || m.total() != 11 {
		t.Fatal("retry expanded beyond selected failure")
	}
	for _, id := range []string{p.Runs[0].Host.HostID, p.Runs[1].Host.HostID} {
		if _, err := resumed.fleetRetry(p.Owner, p.PlanID, []string{id}); err == nil {
			t.Fatal("successful/already reserved attempt retried")
		}
	}
	m.assertOnce(t)
	m.mu.Lock()
	peak := m.peak
	m.mu.Unlock()
	if peak > 4 {
		t.Fatalf("max_parallel exceeded: %d", peak)
	}
	fleetStopForTest(t, resumed)
	third := fleetHarness(t, 100, root, m)
	third.RecoverFleet()
	again, _ := third.Fleet.read(child.PlanID)
	if again.State != "completed" || m.total() != 11 {
		t.Fatal("second restart lost completed child or replayed success")
	}
}

func TestFleet100LogicalHostsCompleteWithBoundedConcurrency(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 100, "", m)
	spec := fleetTestSpec("all_at_once")
	spec.Rollout.MaxParallel = 7
	p := fleetPlanForTest(t, s, spec)
	fleetStartForTest(t, s, p)
	done := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.State == "completed" })
	if done.Counts["success"] != 100 || done.ExitStatus != 0 || m.total() != 100 {
		t.Fatal("100-host result mismatch")
	}
	m.assertOnce(t)
	m.mu.Lock()
	peak := m.peak
	m.mu.Unlock()
	if peak > 7 || peak < 1 {
		t.Fatalf("concurrency=%d", peak)
	}
	// Repeated and concurrent execute is observation of the same durable plan.
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.fleetControl(Request{Owner: p.Owner, Operation: "fleet.execute", Fleet: &FleetRequest{PlanID: p.PlanID, Digest: p.Digest}})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if m.total() != 100 {
		t.Fatal("duplicate execute repeated mutation")
	}
}

func TestFleetCanaryFailureAndThresholdBoundaries(t *testing.T) {
	for _, on := range []string{"pause", "cancel_remaining"} {
		t.Run(on, func(t *testing.T) {
			m := newFleetMock()
			s := fleetHarness(t, 6, "", m)
			spec := fleetTestSpec("canary")
			spec.Rollout.Canary = 1
			spec.Rollout.WaveSize = 2
			spec.Rollout.MaxParallel = 1
			zero := 0
			spec.Rollout.MaxFailures = &zero
			spec.Rollout.OnThreshold = on
			p := fleetPlanForTest(t, s, spec)
			m.change(func() { m.exits[p.Runs[0].Alias] = 1 })
			fleetStartForTest(t, s, p)
			stopped := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.State == "paused" || p.State == "canceled" })
			if m.total() != 1 || stopped.Counts["failed"] != 1 {
				t.Fatal("failed canary admitted later wave")
			}
			if on == "cancel_remaining" && stopped.Counts["canceled"] != 5 {
				t.Fatal("threshold did not cancel remaining")
			}
			if on == "pause" {
				fleetCommandForTest(t, s, p, "resume")
				awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.State == "paused" })
				if m.total() != 1 {
					t.Fatal("resume waived failed canary")
				}
			}
		})
	}
	one := 1
	half := 0.5
	p := FleetPlan{Spec: FleetSpec{Rollout: FleetRollout{MaxFailures: &one, MaxFailureRatio: &half}}, Runs: []HostRun{{State: "success"}, {State: "failed"}, {State: "skipped"}, {State: "canceled"}}}
	if fleetThreshold(p) {
		t.Fatal("threshold incorrectly triggers at equality")
	}
	p.Runs = append(p.Runs, HostRun{State: "ambiguous"})
	if !fleetThreshold(p) {
		t.Fatal("ambiguous omitted from threshold")
	}
	p.Spec.Rollout = FleetRollout{}
	if fleetThreshold(p) {
		t.Fatal("omitted thresholds enabled")
	}
}

func TestFleetSnapshotApprovalOwnerAndPolicyBinding(t *testing.T) {
	m := newFleetMock()
	s := fleetHarness(t, 3, "", m)
	p := fleetPlanForTest(t, s, fleetTestSpec("waves"))
	other := Owner{ClientID: "fleet-test", ProjectID: "other"}
	_ = s.Grant(other, "fleet.execute")
	if _, _, err := s.fleetApprove(other, p.PlanID, p.Digest, 60); err == nil {
		t.Fatal("cross-project approval accepted")
	}
	for _, ttl := range []int{-1, 601} {
		if _, _, err := s.fleetApprove(p.Owner, p.PlanID, p.Digest, ttl); err == nil {
			t.Fatal("invalid approval expiry accepted")
		}
	}
	a, _, err := s.fleetApprove(p.Owner, p.PlanID, p.Digest, 60)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []Request{
		{Owner: other, Operation: "fleet.execute", Approval: a.Token, Fleet: &FleetRequest{PlanID: p.PlanID, Digest: p.Digest}},
		{Owner: p.Owner, Operation: "fleet.execute", Approval: a.Token, Fleet: &FleetRequest{PlanID: p.PlanID, Digest: strings.Repeat("a", 64)}},
		{Owner: p.Owner, Operation: "fleet.execute", Approval: "replacement", Fleet: &FleetRequest{PlanID: p.PlanID, Digest: p.Digest}},
	} {
		if _, err := s.fleetControl(q); err == nil {
			t.Fatal("approval substitution accepted")
		}
	}
	if m.total() != 0 {
		t.Fatal("rejected approval caused a side effect")
	}
	in := s.FleetInventorySnapshot()
	in.Records[0].Labels["changed"] = "yes"
	if _, err := s.FleetInventoryUpdate(in.Revision, in.Records); err != nil {
		t.Fatal(err)
	}
	frozen, _ := s.Fleet.read(p.PlanID)
	if frozen.Digest != p.Digest || frozen.Runs[0].Host.Labels["changed"] != "" {
		t.Fatal("inventory update mutated plan snapshot")
	}
	// Replacing the endpoint under a stable alias never inherits the old target.
	h := p.Runs[0]
	if err := s.Client().Hosts.Add(transport.Host{Name: h.Alias, Addr: "replacement.invalid"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.fleetApprove(p.Owner, p.PlanID, p.Digest, 60); err == nil {
		t.Fatal("old plan approved a changed connection")
	}
	if _, err := s.fleetControl(Request{Owner: p.Owner, Operation: "fleet.execute", Approval: a.Token, Fleet: &FleetRequest{PlanID: p.PlanID, Digest: p.Digest}}); err != nil {
		t.Fatal(err)
	}
	stopped := awaitFleet(t, s, p.PlanID, func(p FleetPlan) bool { return p.State == "failed" || p.State == "paused" || p.State == "completed" })
	if stopped.Counts["success"] == 3 {
		t.Fatal("replaced endpoint executed")
	}
	m.mu.Lock()
	replacedStarts := m.hostStarts[h.Alias]
	m.mu.Unlock()
	if replacedStarts != 0 {
		t.Fatal("old approval connected to replacement")
	}
}

func TestFleetStrictStateBudgetAndDiskFault(t *testing.T) {
	root := t.TempDir()
	m := newFleetMock()
	s := fleetHarness(t, 1, root, m)
	p := fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	data, err := os.ReadFile(filepath.Join(root, "plans.json"))
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func([]byte) []byte{
		"future": func(b []byte) []byte { return []byte(strings.Replace(string(b), `"schema":1`, `"schema":2`, 1)) },
		"duplicate": func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"schema":1`, `"schema":1,"schema":1`, 1))
		},
		"corrupt_digest": func(b []byte) []byte { return []byte(strings.Replace(string(b), p.Digest, strings.Repeat("0", 64), 1)) },
		"trailing":       func(b []byte) []byte { return append(b, []byte(` {}`)...) },
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "plans.json")
			bad := change(data)
			if err := os.WriteFile(path, bad, 0600); err != nil {
				t.Fatal(err)
			}
			loaded := NewService(nil)
			defer loaded.Close(context.Background())
			if err := loaded.ConfigureFleet(path); err == nil {
				t.Fatal("invalid Fleet state published")
			}
			after, _ := os.ReadFile(path)
			if string(after) != string(bad) {
				t.Fatal("invalid state overwritten")
			}
		})
	}
	for i := 1; i < FleetMaxOwnerPlans; i++ {
		fleetPlanForTest(t, s, fleetTestSpec("all_at_once"))
	}
	if _, err := s.createFleetPlan(p.Owner, fleetTestSpec("all_at_once")); err == nil {
		t.Fatal("owner plan budget bypassed")
	}
	s.Fleet.mu.Lock()
	s.Fleet.persist = func(string, []byte) error { return errors.New("injected disk failure") }
	s.Fleet.mu.Unlock()
	if _, _, err := s.fleetApprove(p.Owner, p.PlanID, p.Digest, 60); err == nil {
		t.Fatal("disk failure acknowledged approval")
	}
	old, ok := s.Fleet.read(p.PlanID)
	if !ok || old.State != "planned" || old.ApprovalRef != "" || m.total() != 0 {
		t.Fatal("failed persist changed published state or dispatched")
	}
	var snapshot fleetSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil || snapshot.Schema != FleetSchemaVersion {
		t.Fatal("private snapshot malformed")
	}
	if st, err := os.Stat(filepath.Join(root, "plans.json")); err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("Fleet state is not private")
	}
}
