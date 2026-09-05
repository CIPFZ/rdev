package main

import (
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/state"
)

func stateProtoReport(r state.Report) *proto.StateResult {
	out := &proto.StateResult{Root: "", DryRun: r.DryRun, SchemaVersion: r.SchemaVersion, Changed: append([]string(nil), r.Changed...), Quarantined: append([]string(nil), r.Quarantined...)}
	// Do not expose absolute state paths in responses; all paths in the report
	// are root-relative and the root itself is an opaque label.
	if r.Manifest != nil {
		out.Manifest = &proto.StateManifest{SchemaVersion: r.Manifest.SchemaVersion, WriterVersion: r.Manifest.WriterVersion, AgentIdentity: r.Manifest.AgentIdentity, Namespace: r.Manifest.Namespace, LastMigration: r.Manifest.LastMigration}
	}
	for _, rec := range r.Records {
		out.Records = append(out.Records, proto.StateRecord{Path: rec.Path, SchemaVersion: rec.SchemaVersion, Valid: rec.Valid, Bytes: rec.Bytes})
	}
	for _, f := range r.Findings {
		out.Findings = append(out.Findings, proto.StateFinding{Path: f.Path, Kind: f.Kind, Message: f.Message, Action: f.Action})
	}
	return out
}

func doState(op string, p *proto.StateParams, root string) (*proto.StateResult, error) {
	if p == nil {
		return nil, invalidRequestError("state parameters required")
	}
	switch op {
	case proto.OpStateInspect:
		r, err := state.Inspect(root)
		if err != nil {
			return nil, err
		}
		out := stateProtoReport(r)
		out.Root = "state"
		return out, nil
	case proto.OpStateMigrate:
		r, err := state.Migrate(root, p.DryRun)
		if err != nil {
			return nil, err
		}
		out := stateProtoReport(r)
		out.Root = "state"
		return out, nil
	case proto.OpStateRepair:
		r, err := state.Repair(root, p.DryRun)
		if err != nil {
			return nil, err
		}
		out := stateProtoReport(r)
		out.Root = "state"
		return out, nil
	default:
		return nil, invalidRequestError("unknown state operation")
	}
}
