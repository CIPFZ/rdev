package broker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func validFleetID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}
func newFleetID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	return hex.EncodeToString(b), err
}
func fleetHash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func normalizeFleetSpec(in FleetSpec) (FleetSpec, error) {
	spec := cloneFleet(in)
	if spec.Operation != proto.OpJobStart {
		return spec, errors.New("unsupported fleet operation; supported: job_start")
	}
	if spec.Job == nil || spec.Job.Spec == nil || len(spec.Job.Spec.Argv) == 0 || spec.Job.Spec.Argv[0] == "" {
		return spec, errors.New("fleet job argv required")
	}
	allowed := proto.JobParams{Spec: spec.Job.Spec, Resources: spec.Job.Resources, Label: spec.Job.Label}
	if fleetHash(allowed) != fleetHash(spec.Job) {
		return spec, errors.New("fleet job_start rejects lifecycle, secret and caller identity fields")
	}
	if spec.Job.Spec.Stdin != "" || spec.Job.Spec.TimeoutSec != 0 || spec.Job.Spec.MaxOutputBytes != 0 {
		return spec, errors.New("job limits must use resources")
	}
	if len(spec.Job.Spec.Env) > 64 || len(spec.Job.Spec.Argv) > 128 || len(spec.Job.Label) > 128 {
		return spec, errors.New("fleet job size limit")
	}
	for k, v := range spec.Job.Spec.Env {
		if k == "" || strings.ContainsAny(k, "=\x00") || strings.ContainsRune(v, 0) || strings.HasPrefix(v, "secret:") {
			return spec, errors.New("invalid fleet environment")
		}
	}
	for _, arg := range spec.Job.Spec.Argv {
		if strings.ContainsRune(arg, 0) {
			return spec, errors.New("invalid fleet argv")
		}
	}
	wire, err := proto.NormalizeTimeouts(&proto.Request{Op: proto.OpJobStart, Job: spec.Job})
	if err != nil {
		return spec, err
	}
	spec.Job = wire.Job
	r := spec.Job.Resources
	if r.CPUQuotaMillis < 0 || r.MemoryBytes < 0 || r.PIDs < 0 || r.FDs < 0 || r.JobCount < 0 || r.FDs > 1<<20 || r.JobCount > 1024 {
		return spec, errors.New("invalid fleet resource envelope")
	}
	b, _ := json.Marshal(spec.Job)
	if len(b) > FleetMaxSpecBytes {
		return spec, errors.New("fleet operation exceeds size limit")
	}
	p := &spec.Rollout
	if p.Strategy == "" {
		p.Strategy = "waves"
	}
	if p.MaxParallel == 0 {
		p.MaxParallel = 4
	}
	if p.MaxParallel < 1 || p.MaxParallel > 16 || p.PauseBetweenWavesSec < 0 || p.PauseBetweenWavesSec > 3600 {
		return spec, errors.New("invalid fleet parallelism or wave pause")
	}
	switch p.Strategy {
	case "all_at_once":
		if p.Canary != 0 || p.WaveSize != 0 || p.PauseBetweenWavesSec != 0 {
			return spec, errors.New("all_at_once conflicts with wave/canary settings")
		}
	case "waves", "canary":
		if p.WaveSize == 0 {
			p.WaveSize = 10
		}
		if p.WaveSize < 1 || p.WaveSize > FleetMaxTargets {
			return spec, errors.New("invalid wave size")
		}
		if p.Strategy == "canary" {
			if p.Canary == 0 {
				p.Canary = 1
			}
			if p.Canary < 1 || p.Canary > FleetMaxTargets {
				return spec, errors.New("invalid canary size")
			}
		} else if p.Canary != 0 {
			return spec, errors.New("canary count requires canary strategy")
		}
	default:
		return spec, errors.New("invalid fleet strategy")
	}
	if p.MaxFailures != nil && (*p.MaxFailures < 0 || *p.MaxFailures > FleetMaxTargets) {
		return spec, errors.New("invalid max_failures")
	}
	if p.MaxFailureRatio != nil && (math.IsNaN(*p.MaxFailureRatio) || math.IsInf(*p.MaxFailureRatio, 0) || *p.MaxFailureRatio < 0 || *p.MaxFailureRatio > 1) {
		return spec, errors.New("invalid max_failure_ratio")
	}
	if p.OnThreshold == "" {
		p.OnThreshold = "pause"
	}
	if p.OnThreshold != "pause" && p.OnThreshold != "cancel_remaining" {
		return spec, errors.New("invalid on_threshold")
	}
	return spec, nil
}
func fleetPlanDigest(p FleetPlan) string {
	type identity struct {
		Host                               FleetHost
		Alias, DispatchDigest, OperationID string
		Attempt                            int
	}
	hosts := make([]identity, 0, len(p.Runs))
	for _, r := range p.Runs {
		hosts = append(hosts, identity{r.Host, r.Alias, r.DispatchDigest, r.OperationID, r.Attempt})
	}
	return fleetHash(struct {
		ID, Parent string
		Owner      Owner
		Spec       FleetSpec
		Hosts      []identity
	}{p.PlanID, p.ParentPlanID, p.Owner, p.Spec, hosts})
}
func (s *Service) createFleetPlan(owner Owner, spec FleetSpec) (FleetPlan, error) {
	spec, err := normalizeFleetSpec(spec)
	if err != nil {
		return FleetPlan{}, err
	}
	hosts, err := s.ResolveFleetTargets(owner, spec.Selector)
	if err != nil {
		return FleetPlan{}, err
	}
	if len(hosts) > FleetMaxTargets {
		return FleetPlan{}, errors.New("fleet target hard limit exceeded")
	}
	if spec.Rollout.Canary > len(hosts) {
		return FleetPlan{}, errors.New("canary exceeds target count")
	}
	id, err := newFleetID()
	if err != nil {
		return FleetPlan{}, err
	}
	p := FleetPlan{PlanID: id, Owner: owner, Spec: spec, State: "planned", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	for _, h := range hosts {
		alias := h.Aliases[0]
		digest, err := s.FleetDispatchIdentity(h, alias)
		if err != nil {
			return FleetPlan{}, errors.New("inventory changed during plan; preview again")
		}
		op, err := proto.NewOperationID()
		if err != nil {
			return FleetPlan{}, err
		}
		p.Runs = append(p.Runs, HostRun{Host: h, Alias: alias, DispatchDigest: digest, State: "pending", Attempt: 1, OperationID: op})
	}
	p.Digest = fleetPlanDigest(p)
	fleetAggregate(&p)
	f := s.Fleet
	f.mu.Lock()
	defer f.mu.Unlock()
	next := f.snapshot()
	if err = f.capacity(next, owner); err != nil {
		return FleetPlan{}, err
	}
	next[id] = p
	if err = f.commit(next); err != nil {
		return FleetPlan{}, err
	}
	s.fleetAudit(p, nil, "planned")
	return p, nil
}
func fleetAggregate(p *FleetPlan) {
	p.RiskReasons = []string{"mutation"}
	if p.Spec.Selector == "all" {
		p.RiskReasons = append(p.RiskReasons, "explicit_all")
	}
	if len(p.Runs) > FleetLargeThreshold {
		p.RiskReasons = append(p.RiskReasons, "large_targets")
	}
	p.Counts = map[string]int{}
	for _, state := range []string{"pending", "dispatching", "running", "success", "failed", "skipped", "canceled", "ambiguous", "unreachable"} {
		p.Counts[state] = 0
	}
	for _, r := range p.Runs {
		p.Counts[r.State]++
	}
	p.Total = len(p.Runs)
	p.ExitStatus = 2
	if p.State == "completed" && p.Counts["success"] == p.Total {
		p.ExitStatus = 0
	} else if p.State == "failed" || p.State == "canceled" {
		p.ExitStatus = 1
	}
	if p.Counts["ambiguous"] > 0 {
		p.ExitStatus = 2
	}
}
func fleetPage(p FleetPlan, offset, limit int) FleetPlan {
	fleetAggregate(&p)
	end := min(len(p.Runs), offset+limit)
	if offset > len(p.Runs) {
		offset = len(p.Runs)
	}
	if end < len(p.Runs) {
		p.NextOffset = end
	}
	p.Runs = p.Runs[offset:end]
	return p
}
func (s *Service) fleetRetry(owner Owner, id string, ids []string) (FleetPlan, error) {
	if len(ids) == 0 || len(ids) > FleetMaxTargets {
		return FleetPlan{}, errors.New("explicit retry subset required")
	}
	sort.Strings(ids)
	selected := map[string]bool{}
	for _, id := range ids {
		if !validFleetID(id) || selected[id] {
			return FleetPlan{}, errors.New("invalid or duplicate retry host")
		}
		selected[id] = true
	}
	f := s.Fleet
	f.mu.Lock()
	defer f.mu.Unlock()
	next := f.snapshot()
	parent, ok := next[id]
	if !ok || parent.Owner != owner {
		return FleetPlan{}, errors.New("fleet plan unavailable")
	}
	if parent.State == "running" {
		return FleetPlan{}, errors.New("pause or finish the plan before retry")
	}
	child := FleetPlan{Owner: owner, Spec: cloneFleet(parent.Spec), ParentPlanID: id, State: "planned", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	var err error
	child.PlanID, err = newFleetID()
	if err != nil {
		return child, err
	}
	for i, r := range parent.Runs {
		if !selected[r.Host.HostID] {
			continue
		}
		delete(selected, r.Host.HostID)
		if r.RetryPlanID != "" {
			return FleetPlan{}, errors.New("host attempt already has a retry plan")
		}
		switch r.State {
		case "failed", "unreachable", "skipped", "canceled":
		default:
			return FleetPlan{}, errors.New("retry requires selected terminal failures; reconcile ambiguous first")
		}
		if r.Attempt >= 100 {
			return FleetPlan{}, errors.New("fleet attempt limit reached")
		}
		_, err := s.FleetDispatchIdentity(r.Host, r.Alias)
		if err != nil {
			return FleetPlan{}, errors.New("retry target changed")
		}
		if !s.fleetHostAllowed(owner, r.Host.HostID) {
			return FleetPlan{}, errors.New("fleet target unavailable")
		}
		op, err := proto.NewOperationID()
		if err != nil {
			return FleetPlan{}, err
		}
		child.Runs = append(child.Runs, HostRun{Host: r.Host, Alias: r.Alias, DispatchDigest: r.DispatchDigest, State: "pending", Attempt: r.Attempt + 1, OperationID: op})
		parent.Runs[i].RetryPlanID = child.PlanID
	}
	if len(selected) > 0 {
		return FleetPlan{}, errors.New("retry target unavailable")
	}
	if child.Spec.Rollout.Canary > len(child.Runs) {
		child.Spec.Rollout.Canary = len(child.Runs)
	}
	child.Digest = fleetPlanDigest(child)
	fleetAggregate(&child)
	parent.UpdatedAt = time.Now()
	if err = f.capacity(next, owner); err != nil {
		return FleetPlan{}, err
	}
	next[id] = parent
	next[child.PlanID] = child
	if err = f.commit(next); err != nil {
		return FleetPlan{}, err
	}
	s.fleetAudit(child, nil, "retry_planned")
	return child, nil
}

// fleetHostAuthorizer captures one policy version for the whole target filter.
// Combining grants from separate decisions could admit a target that no single
// published policy ever authorized. The closure never references mutable maps.
func (s *Service) fleetHostAuthorizer(owner Owner) func(string) bool {
	if owner.Validate() != nil {
		return func(string) bool { return false }
	}
	s.policy.mu.RLock()
	if s.policy.failed != nil {
		s.policy.mu.RUnlock()
		return func(string) bool { return false }
	}
	grants := make(map[string]bool, len(s.policy.grants[owner.Key()]))
	for key, value := range s.policy.grants[owner.Key()] {
		grants[key] = value
	}
	s.policy.mu.RUnlock()
	return func(id string) bool {
		for _, op := range []string{"fleet.plan", proto.OpJobStart, proto.OpJobStatus} {
			capability := CapabilityForOperation(op)
			if !grants[op] && !grants[capabilityKey(capability, op)] && !grants[hostGrantKey(id, capability, op)] {
				return false
			}
		}
		return true
	}
}

func (s *Service) fleetHostAllowed(owner Owner, id string) bool {
	return s.fleetHostAuthorizer(owner)(id)
}
func (s *Service) fleetAudit(p FleetPlan, r *HostRun, result string) {
	event := AuditEvent{Owner: p.Owner.Key(), Operation: "fleet.execute", PlanRef: fleetHash(p.PlanID), RequestDigest: p.Digest, PolicyDigest: p.ApprovalPolicy, ApprovalID: p.ApprovalRef, Decision: "allow", Result: result, DigestScope: "fleet", TargetScope: "fleet"}
	if r != nil {
		event.HostRef = fleetHash(r.Host.HostID)
		event.Attempt = r.Attempt
		event.OperationRef = OperationReference(Request{Wire: &proto.Request{OperationID: r.OperationID}})
		event.TargetDigest = fleetHash(struct {
			ID      string
			Attempt int
			Target  string
		}{r.Host.HostID, r.Attempt, r.Host.TargetDigest})
		event.PolicyDigest = r.PolicyDigest
		event.ApprovalID = r.ApprovalRef
	}
	s.Audit.Append(event)
}

// A single policy version authorizes both Fleet execution and every component
// of this host admission. A reload cannot combine complementary grants.
func (s *Service) fleetHostAdmission(owner Owner, id, policy string) bool {
	s.policy.mu.RLock()
	defer s.policy.mu.RUnlock()
	if s.policy.failed != nil || s.policy.digest != policy {
		return false
	}
	grants := s.policy.grants[owner.Key()]
	for _, op := range []string{"fleet.execute", "fleet.plan", proto.OpJobStart, proto.OpJobStatus} {
		cap := CapabilityForOperation(op)
		hostAllowed := op != "fleet.execute" && grants[hostGrantKey(id, cap, op)]
		if !grants[op] && !grants[capabilityKey(cap, op)] && !hostAllowed {
			return false
		}
	}
	return true
}
