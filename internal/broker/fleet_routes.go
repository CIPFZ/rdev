package broker

import (
	"context"
	"errors"
	"sort"
	"strings"
)

func isFleetOperation(op string) bool { return strings.HasPrefix(op, "fleet.") }
func fleetPermission(op string) string {
	switch op {
	case "fleet.status", "fleet.results", "fleet.list":
		return "fleet.plan"
	case "fleet.pause", "fleet.resume", "fleet.cancel", "fleet.retry", "fleet.reconcile":
		return "fleet.execute"
	default:
		return op
	}
}
func validateFleetRoute(req Request) error {
	allowed := Request{ID: req.ID, Owner: req.Owner, Operation: req.Operation, Fleet: req.Fleet, Approval: req.Approval, Capability: req.Capability}
	if fleetHash(allowed) != fleetHash(req) || req.Fleet == nil {
		return errors.New("fleet requires an exact local fleet request without wire, host, risk or approval_spec")
	}
	q := req.Fleet
	a := FleetRequest{}
	switch req.Operation {
	case "fleet.plan":
		a.Spec = q.Spec
		if a.Spec == nil {
			return errors.New("fleet spec required")
		}
	case "fleet.approve":
		a.PlanID = q.PlanID
		a.Digest = q.Digest
		a.TTLSeconds = q.TTLSeconds
	case "fleet.execute":
		a.PlanID = q.PlanID
		a.Digest = q.Digest
	case "fleet.status", "fleet.results":
		a.PlanID = q.PlanID
		a.Offset = q.Offset
		a.Limit = q.Limit
	case "fleet.list":
		a.Offset = q.Offset
		a.Limit = q.Limit
	case "fleet.pause", "fleet.resume", "fleet.cancel", "fleet.reconcile":
		a.PlanID = q.PlanID
	case "fleet.retry":
		a.PlanID = q.PlanID
		a.HostIDs = q.HostIDs
	case "fleet.inventory.list":
	case "fleet.inventory.import":
		a.Revision = q.Revision
		if a.Revision == 0 {
			return errors.New("inventory revision required")
		}
	case "fleet.inventory.update":
		a.Inventory = q.Inventory
		a.Revision = q.Revision
		if a.Inventory == nil || a.Inventory.Records == nil || a.Inventory.RetiredIDs == nil || a.Revision == 0 || a.Inventory.Revision != a.Revision || a.Inventory.Schema != FleetInventorySchemaVersion {
			return errors.New("inventory schema and revision required")
		}
	default:
		return errors.New("unsupported fleet operation")
	}
	if fleetHash(a) != fleetHash(q) {
		return errors.New("unexpected fleet parameters")
	}
	if a.PlanID != "" && !validFleetID(a.PlanID) {
		return errors.New("invalid fleet plan ID")
	}
	if (req.Operation == "fleet.approve" || req.Operation == "fleet.execute") && !validDigest(a.Digest) {
		return errors.New("exact fleet digest required")
	}
	if req.Approval != "" && req.Operation != "fleet.execute" {
		return errors.New("unexpected fleet approval token")
	}
	if a.Offset < 0 || a.Offset > FleetMaxTargets || a.Limit < 0 || a.Limit > 32 {
		return errors.New("fleet pagination requires offset 0..128 and limit 1..32 (zero defaults to 32)")
	}
	return nil
}

// HandleFleet is the authority shared by CLI and MCP. No frontend can resolve
// targets, consume approvals, schedule private SSH or recover its own plan.
func (s *Service) HandleFleet(ctx context.Context, req Request) Response {
	out := Response{ID: req.ID}
	d := s.DecideBrokerRequest(req)
	out.PolicyDigest = d.Digest
	fail := func(err error) Response { out.Error = err.Error(); return out }
	if !d.Allow {
		return fail(errors.New("capability denied"))
	}
	if req.Capability != "" && req.Capability != d.Capability {
		return fail(errors.New("capability mismatch"))
	}
	if err := validateFleetRoute(req); err != nil {
		return fail(err)
	}
	if ctx.Err() != nil {
		return fail(ctx.Err())
	}
	q := req.Fleet
	limit := q.Limit
	if limit == 0 {
		limit = 32
	}
	var p FleetPlan
	var err error
	switch req.Operation {
	case "fleet.plan":
		p, err = s.createFleetPlan(req.Owner, *q.Spec)
	case "fleet.retry":
		p, err = s.fleetRetry(req.Owner, q.PlanID, append([]string{}, q.HostIDs...))
	case "fleet.approve":
		var a Approval
		a, p, err = s.fleetApprove(req.Owner, q.PlanID, q.Digest, q.TTLSeconds)
		if err == nil {
			out.Approval = &a
		}
	case "fleet.status", "fleet.results":
		var ok bool
		p, ok = s.Fleet.read(q.PlanID)
		if !ok || p.Owner != req.Owner {
			return fail(errors.New("fleet plan unavailable"))
		}
	case "fleet.list":
		plans := []FleetPlan{}
		for _, v := range s.Fleet.snapshot() {
			if v.Owner == req.Owner {
				v.Runs = nil
				v.Spec.Job = nil
				plans = append(plans, v)
			}
		}
		sort.Slice(plans, func(i, j int) bool { return plans[i].PlanID < plans[j].PlanID })
		end := min(len(plans), q.Offset+limit)
		offset := min(q.Offset, len(plans))
		page := FleetPage{Plans: plans[offset:end]}
		if end < len(plans) {
			page.NextOffset = end
		}
		out.Fleets = &page
		out.OK = true
		return out
	case "fleet.inventory.list":
		inv := s.FleetInventorySnapshot()
		out.Inventory = &inv
		out.OK = true
		return out
	case "fleet.inventory.import":
		var inv FleetInventory
		inv, err = s.FleetInventoryImport(q.Revision)
		if err == nil {
			out.Inventory = &inv
			out.OK = true
		}
		if err != nil {
			return fail(err)
		}
		return out
	case "fleet.inventory.update":
		for _, host := range q.Inventory.Records {
			if host.Labels == nil || host.Aliases == nil {
				return fail(errors.New("complete inventory records required"))
			}
		}
		current := s.FleetInventorySnapshot()
		if fleetHash(current.RetiredIDs) != fleetHash(q.Inventory.RetiredIDs) {
			return fail(errors.New("inventory retired IDs are server-owned"))
		}
		var inv FleetInventory
		inv, err = s.FleetInventoryUpdate(q.Revision, q.Inventory.Records)
		if err != nil {
			return fail(err)
		}
		out.Inventory = &inv
		out.OK = true
		return out
	default:
		p, err = s.fleetControl(req)
	}
	if err != nil {
		return fail(err)
	}
	p = fleetPage(p, q.Offset, limit)
	out.Fleet = &p
	out.OK = true
	return out
}
