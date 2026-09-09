package mcpsrv

import (
	"context"
	"errors"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type fleetIn struct {
	Action        string              `json:"action" jsonschema:"plan, approve, execute, status, results, list, pause, resume, cancel, retry, reconcile, inventory.import, inventory.update or inventory.list"`
	Request       broker.FleetRequest `json:"request" jsonschema:"Shared broker Fleet request. Plan takes spec; approve and execute take immutable plan_id and digest; retry takes explicit host_ids; query pages use offset and limit (1..32)."`
	ApprovalToken string              `json:"approval_token,omitempty" jsonschema:"Digest-bound token returned by approve; required by execute."`
}

type fleetOut struct {
	Plan      *broker.FleetPlan      `json:"plan,omitempty"`
	Plans     *broker.FleetPage      `json:"plans,omitempty"`
	Approval  *broker.Approval       `json:"approval,omitempty"`
	Inventory *broker.FleetInventory `json:"inventory,omitempty"`
}

func registerFleet(s *mcp.Server, socket string, owner broker.Owner) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_fleet", Description: "Durable broker-owned job_start orchestration. plan persists an immutable preview; inspect every target page and the operation digest, then explicitly approve and execute. Supports waves/canary, pause/resume/cancel, status/results and explicit failed-subset retry. All mutations require approval plus each host's job permissions. Cancel leaves already submitted jobs alive; observers disconnect without canceling the plan. Results have bounded metadata, no raw logs. plan.exit_status: 0 complete success, 1 failed/canceled, 2 unfinished/ambiguous. No standalone SSH fallback."}, func(ctx context.Context, _ *mcp.CallToolRequest, in fleetIn) (*mcp.CallToolResult, fleetOut, error) {
		if socket == "" {
			return nil, fleetOut{}, proto.NewError(proto.CodeUnsupportedFeature, "", proto.StateNotSent)
		}
		switch in.Action {
		case "plan", "approve", "execute", "status", "results", "list", "pause", "resume", "cancel", "retry", "reconcile", "inventory.import", "inventory.update", "inventory.list":
		default:
			return nil, fleetOut{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		r, err := callBroker(ctx, socket, owner, broker.Request{Operation: "fleet." + in.Action, Fleet: &in.Request, Approval: in.ApprovalToken})
		if err != nil {
			return nil, fleetOut{}, err
		}
		out := fleetOut{Plan: r.Fleet, Plans: r.Fleets, Approval: r.Approval, Inventory: r.Inventory}
		if out.Plan == nil && out.Plans == nil && out.Approval == nil && out.Inventory == nil {
			return nil, fleetOut{}, errors.New("broker returned no Fleet result")
		}
		return nil, out, nil
	})
}
