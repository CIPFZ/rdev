package broker

import (
	"context"
	"encoding/hex"
	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

type ReleaseAudit struct {
	Version  string `json:"version,omitempty"`
	Digest   string `json:"digest,omitempty"`
	Channel  string `json:"channel,omitempty"`
	Unsigned bool   `json:"unsigned"`
	TestRoot bool   `json:"test_root"`
	Result   string `json:"result"`
}

func sanitizeReleaseAudit(in *ReleaseAudit) *ReleaseAudit {
	if in == nil {
		return nil
	}
	out := *in
	if out.Version != "" {
		if _, err := artifact.CompareVersions(out.Version, out.Version); err != nil || len(out.Version) > 64 {
			out.Version = ""
		}
	}
	if b, err := hex.DecodeString(out.Digest); err != nil || len(b) != 32 {
		out.Digest = ""
	}
	if out.Channel != "dev" && out.Channel != "beta" && out.Channel != "stable" {
		out.Channel = ""
	}
	switch out.Result {
	case "ready", "rejected", "ambiguous", "committed":
	default:
		out.Result = "rejected"
	}
	return &out
}
func (s *Service) ObserveRelease(ctx context.Context, base AuditEvent) context.Context {
	return transport.WithReleaseObserver(ctx, func(event transport.ReleaseEvent) {
		d := event.Decision
		e := base
		e.Release = &ReleaseAudit{Version: d.Version, Digest: d.Digest, Channel: d.Channel, Unsigned: d.Unsigned, TestRoot: d.TestRoot, Result: event.Result}
		e.Decision = "release_policy"
		e.Result = event.Result
		s.Audit.Append(e)
	})
}

func (s *Service) observeBackgroundRelease(ctx context.Context, host string, req *proto.Request) context.Context {
	if transport.HasReleaseObserver(ctx) || req == nil {
		return ctx
	}
	base := AuditEvent{Owner: (Owner{ClientID: req.ClientID, ProjectID: req.ProjectID}).Key(), Operation: req.Op, OperationRef: OperationReference(Request{Wire: req})}
	// This is a configured target snapshot, not a new authorization decision.
	_ = s.BindAuditTarget(&base, host)
	return s.ObserveRelease(ctx, base)
}
