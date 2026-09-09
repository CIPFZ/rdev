package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/CIPFZ/rdev/internal/proto"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"
)

var ErrFleetStorage = errors.New("fleet persistence unavailable; recorded results remain queryable")

type fleetSnapshot struct {
	Schema int         `json:"schema"`
	Plans  []FleetPlan `json:"plans"`
}
type FleetStore struct {
	mu           sync.Mutex // serializes transitions and dispatch admission, never remote I/O
	readMu       sync.RWMutex
	plans        map[string]FleetPlan
	path         string
	failed       bool
	active       map[string]bool
	reconcile    map[string]map[int]bool
	cancelStarts map[string]context.CancelFunc
	lastOwner    string
	lastPlan     map[string]string
	persist      func(string, []byte) error
}

func newFleetStore() *FleetStore {
	return &FleetStore{plans: map[string]FleetPlan{}, active: map[string]bool{}, reconcile: map[string]map[int]bool{}, cancelStarts: map[string]context.CancelFunc{}, lastPlan: map[string]string{}, persist: saveFleetBytes}
}
func cloneFleet[T any](v T) T {
	b, _ := json.Marshal(v)
	var copy T
	_ = json.Unmarshal(b, &copy)
	return copy
}
func (f *FleetStore) read(id string) (FleetPlan, bool) {
	f.readMu.RLock()
	defer f.readMu.RUnlock()
	p, ok := f.plans[id]
	return cloneFleet(p), ok
}
func (f *FleetStore) snapshot() map[string]FleetPlan {
	f.readMu.RLock()
	defer f.readMu.RUnlock()
	return cloneFleet(f.plans)
}
func saveFleetBytes(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".rdev-fleet-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Any write error freezes new admission. Queries retain the last acknowledged
// snapshot; restart reads the authoritative file, including a possible rename.
func (f *FleetStore) commit(next map[string]FleetPlan) error { return f.commitSnapshot(next, false) }
func (f *FleetStore) admit(next map[string]FleetPlan) error  { return f.commitSnapshot(next, true) }
func (f *FleetStore) commitSnapshot(next map[string]FleetPlan, admission bool) error {
	if f.failed {
		return ErrFleetStorage
	}
	plans := make([]FleetPlan, 0, len(next))
	for id, p := range next {
		fleetAggregate(&p)
		next[id] = p
		plans = append(plans, p)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].PlanID < plans[j].PlanID })
	b, err := json.Marshal(fleetSnapshot{FleetSchemaVersion, plans})
	if err != nil {
		return err
	}
	if len(b) > FleetMaxBytes {
		return errors.New("fleet storage budget reached")
	}
	// Admission reserves the worst-case bounded metadata growth of every
	// retained HostRun (job identity, result, retry and approval references).
	// Execution/control commits may use that reserve up to the hard byte limit.
	if admission {
		reserved := 0
		for _, p := range next {
			reserved += 4096 + len(p.Runs)*2048
		}
		if len(b) > FleetMaxBytes-reserved {
			return errors.New("fleet lifecycle storage reserve reached")
		}
	}
	if f.path != "" {
		if err = f.persist(f.path, b); err != nil {
			f.failed = true
			return ErrFleetStorage
		}
	}
	f.readMu.Lock()
	f.plans = next
	f.readMu.Unlock()
	return nil
}

// Result and cancellation commits can retry under storage pressure; even a
// successful retry keeps new dispatch frozen until authoritative restart.
func (f *FleetStore) commitRecorded(next map[string]FleetPlan) error {
	failed := f.failed
	f.failed = false
	err := f.commit(next)
	f.failed = f.failed || failed
	return err
}
func (s *Service) ConfigureFleet(path string) error {
	f := s.Fleet
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := ReadPrivateFile(path, FleetMaxBytes)
	next := map[string]FleetPlan{}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		var snapshot fleetSnapshot
		if err = decodeFleetJSON(b, &snapshot); err != nil {
			return err
		}
		if snapshot.Schema != FleetSchemaVersion || snapshot.Plans == nil || len(snapshot.Plans) > FleetMaxPlans {
			return errors.New("invalid fleet schema or count")
		}
		owners := map[string]int{}
		operations := map[string]bool{}
		for _, p := range snapshot.Plans {
			if _, ok := next[p.PlanID]; ok {
				return errors.New("duplicate fleet plan")
			}
			if err = validateFleetPlan(p); err != nil {
				return err
			}
			owners[p.Owner.Key()]++
			if owners[p.Owner.Key()] > FleetMaxOwnerPlans {
				return errors.New("fleet owner budget exceeded")
			}
			for _, r := range p.Runs {
				if operations[r.OperationID] {
					return errors.New("duplicate fleet operation identity")
				}
				operations[r.OperationID] = true
			}
			next[p.PlanID] = p
		}
	}
	if err := validateFleetLinks(next); err != nil {
		return err
	}
	f.path = path
	f.failed = false
	return f.commit(next)
}

// DecodeFleetJSON is shared by bounded CLI files and durable readers. Duplicate,
// unknown, trailing and future fields cannot silently change a reviewed plan.
func DecodeFleetJSON(b []byte, out any) error { return decodeFleetJSON(b, out) }
func decodeFleetJSON(b []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := rejectDuplicateJSON(dec); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("one JSON document required")
	}
	dec = json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}
func validateFleetPlan(p FleetPlan) error {
	if !validFleetID(p.PlanID) || p.Owner.Validate() != nil || p.CreatedAt.IsZero() || p.UpdatedAt.IsZero() || len(p.Runs) == 0 || len(p.Runs) > FleetMaxTargets {
		return errors.New("invalid fleet plan identity or size")
	}
	spec, err := normalizeFleetSpec(p.Spec)
	if err != nil {
		return err
	}
	if fleetHash(spec) != fleetHash(p.Spec) || fleetPlanDigest(p) != p.Digest {
		return errors.New("fleet plan digest mismatch")
	}
	if p.ParentPlanID != "" && !validFleetID(p.ParentPlanID) {
		return errors.New("invalid fleet parent")
	}
	switch p.State {
	case "planned", "running", "paused", "completed", "failed", "canceled":
	default:
		return errors.New("invalid fleet state")
	}
	if p.WaveEnd < 0 || p.WaveEnd > len(p.Runs) {
		return errors.New("invalid fleet wave")
	}
	if p.ApprovalRef != "" && (!validDigest(p.ApprovalRef) || !validDigest(p.ApprovalPolicy) || p.ApprovalExpiresAt.IsZero()) {
		return errors.New("invalid fleet approval binding")
	}
	if p.ApprovalConsumed && p.ApprovalRef == "" || p.State == "running" && !p.ApprovalConsumed {
		return errors.New("invalid consumed fleet approval")
	}
	if p.NextOffset != 0 {
		return errors.New("persisted fleet page is incomplete")
	}
	aggregate := cloneFleet(p)
	fleetAggregate(&aggregate)
	if fleetHash(p.RiskReasons) != fleetHash(aggregate.RiskReasons) || p.Total != aggregate.Total || p.ExitStatus != aggregate.ExitStatus || fleetHash(p.Counts) != fleetHash(aggregate.Counts) {
		return errors.New("fleet aggregate mismatch")
	}
	if p.Spec.Rollout.Canary > len(p.Runs) {
		return errors.New("fleet canary exceeds targets")
	}
	records := make([]FleetHost, 0, len(p.Runs))
	for _, r := range p.Runs {
		records = append(records, r.Host)
	}
	if err := validateFleetInventory(FleetInventory{Schema: FleetInventorySchemaVersion, Revision: 1, Records: records, RetiredIDs: []string{}}); err != nil {
		return err
	}
	last := ""
	for _, r := range p.Runs {
		if !validFleetID(r.Host.HostID) || r.Host.HostID <= last || !validDigest(r.Host.TargetDigest) || !validDigest(r.DispatchDigest) || r.Attempt < 1 || r.Attempt > 100 || proto.ValidateOperationID(r.OperationID) != nil {
			return errors.New("invalid fleet host identity")
		}
		last = r.Host.HostID
		if !slices.Contains(r.Host.Aliases, r.Alias) || r.Alias == "" || len(r.Alias) > 512 {
			return errors.New("invalid fleet alias")
		}
		switch r.State {
		case "pending", "dispatching", "running", "success", "failed", "skipped", "canceled", "ambiguous", "unreachable":
		default:
			return errors.New("invalid host run state")
		}
		if r.RetryPlanID != "" && !validFleetID(r.RetryPlanID) {
			return errors.New("invalid retry reference")
		}
		if (r.PolicyDigest == "") != (r.ApprovalRef == "") {
			return errors.New("fleet run policy and approval must be paired")
		}
		if r.State == "dispatching" && (!validDigest(r.PolicyDigest) || !validDigest(r.ApprovalRef)) {
			return errors.New("fleet dispatch approval missing")
		}
		if r.ApprovalRef != "" && !validDigest(r.ApprovalRef) {
			return errors.New("invalid fleet run approval")
		}
		if r.PolicyDigest != "" && !validDigest(r.PolicyDigest) {
			return errors.New("invalid fleet run policy")
		}
		if p.State == "planned" && r.State != "pending" || p.State == "completed" && r.State != "success" || p.State == "failed" && (r.State == "pending" || r.State == "running" || r.State == "dispatching" || r.State == "ambiguous") || p.State == "canceled" && r.State == "pending" {
			return errors.New("inconsistent fleet terminal state")
		}
		if r.State == "pending" && (r.JobID != "" || r.JobDigest != "" || r.ExitCode != nil || r.PolicyDigest != "") {
			return errors.New("pending fleet run has execution data")
		}
		if (r.State == "running" || r.State == "success") && (r.JobID == "" || !validDigest(r.JobDigest) || !validDigest(r.PolicyDigest)) {
			return errors.New("fleet job identity missing")
		}
		if r.State == "success" && (r.ExitCode == nil || *r.ExitCode != 0) {
			return errors.New("fleet success without exit zero")
		}
		if r.ExitCode != nil && r.State != "success" && r.State != "failed" {
			return errors.New("fleet exit on nonterminal run")
		}
		if r.JobID == "" && r.JobDigest != "" {
			return errors.New("fleet job digest without job")
		}
		if r.JobID != "" {
			expected, _ := proto.JobIDForOperation(proto.PrincipalID(p.Owner.ClientID, p.Owner.ProjectID), r.OperationID)
			if r.JobID != expected {
				return errors.New("fleet job operation identity mismatch")
			}
		}
		if r.JobID != "" && (!validDigest(r.JobDigest) || r.PolicyDigest == "") {
			return errors.New("invalid fleet job binding")
		}
	}
	return nil
}
func fleetRetentionEligible(p FleetPlan) bool {
	if p.State != "completed" && p.State != "failed" && p.State != "canceled" {
		return false
	}
	for _, r := range p.Runs {
		if r.State == "ambiguous" || r.State == "running" || r.State == "dispatching" || r.State == "pending" {
			return false
		}
	}
	return true
}
func (f *FleetStore) capacity(next map[string]FleetPlan, owner Owner) error {
	// Retry components expire together, retaining all parent/attempt evidence
	// until every member is terminal and past the seven-day retention horizon.
	visited := map[string]bool{}
	for id := range next {
		if visited[id] {
			continue
		}
		queue := []string{id}
		component := []string{}
		eligible := true
		for len(queue) > 0 {
			key := queue[0]
			queue = queue[1:]
			if visited[key] {
				continue
			}
			visited[key] = true
			component = append(component, key)
			p, ok := next[key]
			if !ok {
				eligible = false
				continue
			}
			if !fleetRetentionEligible(p) || time.Since(p.UpdatedAt) <= 7*24*time.Hour {
				eligible = false
			}
			if p.ParentPlanID != "" {
				queue = append(queue, p.ParentPlanID)
			}
			for _, r := range p.Runs {
				if r.RetryPlanID != "" {
					queue = append(queue, r.RetryPlanID)
				}
			}
		}
		if eligible {
			for _, key := range component {
				delete(next, key)
			}
		}
	}

	count := 0
	for _, p := range next {
		if p.Owner == owner {
			count++
		}
	}
	if len(next) >= FleetMaxPlans || count >= FleetMaxOwnerPlans {
		return fmt.Errorf("fleet plan retention budget reached")
	}
	return nil
}

func validateFleetLinks(plans map[string]FleetPlan) error {
	for _, p := range plans {
		if p.ParentPlanID != "" {
			parent, ok := plans[p.ParentPlanID]
			if !ok || parent.Owner != p.Owner || parent.PlanID == p.PlanID || parent.Spec.Operation != p.Spec.Operation || fleetHash(parent.Spec.Job) != fleetHash(p.Spec.Job) {
				return errors.New("invalid fleet parent reference")
			}
			parents := map[string]HostRun{}
			for _, r := range parent.Runs {
				parents[r.Host.HostID] = r
			}
			for _, r := range p.Runs {
				old, ok := parents[r.Host.HostID]
				if !ok || old.RetryPlanID != p.PlanID || r.Attempt != old.Attempt+1 || fleetHash(r.Host) != fleetHash(old.Host) || r.Alias != old.Alias || r.DispatchDigest != old.DispatchDigest {
					return errors.New("invalid fleet retry binding")
				}
			}
		} else {
			for _, r := range p.Runs {
				if r.Attempt != 1 {
					return errors.New("fleet initial attempt mismatch")
				}
			}
		}
		for _, r := range p.Runs {
			if r.RetryPlanID != "" {
				child, ok := plans[r.RetryPlanID]
				if !ok || child.ParentPlanID != p.PlanID || child.Owner != p.Owner {
					return errors.New("invalid fleet child reference")
				}
				found := false
				for _, next := range child.Runs {
					if next.Host.HostID == r.Host.HostID {
						found = true
					}
				}
				if !found {
					return errors.New("fleet retry subset mismatch")
				}
			}
		}
	}
	return nil
}
