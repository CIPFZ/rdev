package mcpsrv

import (
	"context"
	"errors"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/support"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type supportIn struct {
	Host    string `json:"host,omitempty" jsonschema:"Optional host for a current capability probe; omit for static support and frontend boundaries."`
	Refresh bool   `json:"refresh,omitempty" jsonschema:"Refresh the current target probe. Requires host."`
}

func registerSupport(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_support", Description: "Discover static support, experimental/non-goal boundaries and optional current target capabilities. Runtime probing does not certify a platform. No environment, credentials or other owner state is returned."}, func(ctx context.Context, _ *mcp.CallToolRequest, in supportIn) (*mcp.CallToolResult, support.Discovery, error) {
		out := support.Discover("standalone")
		if in.Host == "" && in.Refresh {
			return nil, out, errors.New("refresh requires host")
		}
		if in.Host != "" {
			probe, err := c.CapabilityProbe(ctx, in.Host, in.Refresh)
			if err != nil {
				return nil, out, err
			}
			out.SetRuntime(probe)
		}
		return nil, out, nil
	})
}

func registerBrokerSupport(s *mcp.Server, socket string, owner broker.Owner) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_support", Description: "Discover shared frontend boundaries and optional current target capabilities. With host, returns only this principal's per-operation grants; denied capability_probe does not connect to SSH. Grants still require runtime support, resource admission and mutation approval."}, func(ctx context.Context, _ *mcp.CallToolRequest, in supportIn) (*mcp.CallToolResult, support.Discovery, error) {
		out := support.Discover("broker")
		if in.Host == "" {
			if in.Refresh {
				return nil, out, errors.New("refresh requires host")
			}
			return nil, out, nil
		}
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, out, err
		}
		defer c.Close()
		out, err = broker.DiscoverSupport(ctx, c, in.Host, in.Refresh)
		return nil, out, err
	})
}

type stateIn struct {
	Host          string `json:"host"`
	Action        string `json:"action" jsonschema:"inspect, migrate or repair; operates on the entire host state root, not only this principal's jobs."`
	DryRun        *bool  `json:"dry_run,omitempty" jsonschema:"Default true. Set false explicitly to apply migration or repair."`
	ApprovalToken string `json:"approval_token,omitempty" jsonschema:"Required by shared broker migration and repair, including previews."`
	OperationID   string `json:"operation_id,omitempty"`
}

func (in stateIn) request() (*proto.Request, error) {
	op := ""
	switch in.Action {
	case "inspect":
		op = proto.OpStateInspect
	case "migrate":
		op = proto.OpStateMigrate
	case "repair":
		op = proto.OpStateRepair
	default:
		return nil, errors.New("state action must be inspect, migrate or repair")
	}
	dry := true
	if in.DryRun != nil {
		dry = *in.DryRun
	}
	return &proto.Request{Op: op, OperationID: in.OperationID, State: &proto.StateParams{DryRun: dry}}, nil
}

func registerState(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_state", Description: "Inspect, migrate or repair the entire remote state root. This is host-wide administration. Migration/repair default to a preview; review findings before setting dry_run=false."}, func(ctx context.Context, _ *mcp.CallToolRequest, in stateIn) (*mcp.CallToolResult, proto.StateResult, error) {
		req, err := in.request()
		if err != nil {
			return nil, proto.StateResult{}, err
		}
		if in.OperationID != "" {
			return nil, proto.StateResult{}, errors.New("explicit state operation_id requires shared broker mode")
		}
		var result *proto.StateResult
		switch req.Op {
		case proto.OpStateInspect:
			result, err = c.StateInspect(ctx, in.Host)
		case proto.OpStateMigrate:
			result, err = c.StateMigrate(ctx, in.Host, req.State.DryRun)
		case proto.OpStateRepair:
			result, err = c.StateRepair(ctx, in.Host, req.State.DryRun)
		}
		if err != nil {
			return nil, proto.StateResult{}, err
		}
		if result == nil {
			return nil, proto.StateResult{}, errors.New("state operation returned no result")
		}
		return nil, *result, nil
	})
}

func registerBrokerState(s *mcp.Server, socket string, owner broker.Owner) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_state", Description: "Host-wide state administration through the broker. Requires a separate state_inspect/state_migrate/state_repair grant; exact-host grants are recommended. Migration and repair, including previews, require digest-bound approval. Reports may include every owner's state record paths; grant only to administrators."}, func(ctx context.Context, _ *mcp.CallToolRequest, in stateIn) (*mcp.CallToolResult, proto.StateResult, error) {
		req, err := in.request()
		if err != nil {
			return nil, proto.StateResult{}, err
		}
		r, err := callBroker(ctx, socket, owner, broker.Request{Operation: req.Op, Host: in.Host, Wire: req, Approval: in.ApprovalToken})
		if err != nil {
			return nil, proto.StateResult{}, err
		}
		if r.Wire == nil || r.Wire.State == nil {
			return nil, proto.StateResult{}, errors.New("broker state returned no result")
		}
		return nil, *r.Wire.State, nil
	})
}
