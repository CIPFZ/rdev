package broker

import (
	"context"
	"errors"
	"sort"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/support"
)

// Support reads only this authenticated owner's grant decisions. The requested
// host is never resolved here, so denied discovery cannot enumerate inventory.
func (s *Service) Support(owner Owner, host string) support.Discovery {
	out := support.Discover("broker")
	operations := []string{"sync.push", "sync.pull", "sync.delete", "secret.set", "secret.set_from_file", "secret.list", "secret.delete", "secret.use", "status", "job.events", "mutation.status"}
	for _, op := range proto.Operations() {
		if op.Name != proto.OpSyncInspect && op.Name != proto.OpSyncStage && op.Name != proto.OpSyncCommit {
			operations = append(operations, op.Name)
		}
	}
	sort.Strings(operations)
	s.policy.mu.RLock()
	defer s.policy.mu.RUnlock()
	out.PolicyDigest = s.policy.digest
	grants := s.policy.grants[owner.Key()]
	for _, op := range operations {
		capability := CapabilityForOperation(op)
		requestHost, scope := host, "host"
		if op == "status" || op == "mutation.status" {
			// Status and the mutation-status frontends send unscoped requests;
			// a host-only grant cannot authorize those invocations.
			requestHost, scope = "", "broker"
		}
		allowed := grants[op] || grants[capabilityKey(capability, op)] || requestHost != "" && grants[hostGrantKey(requestHost, capability, op)]
		// File import needs both permissions, just like DecideBrokerRequest.
		if op == "secret.set_from_file" {
			allowed = allowed && (grants["read_file"] || grants[capabilityKey("file.read", "read_file")] || host != "" && grants[hostGrantKey(host, "file.read", "read_file")])
		}
		out.Permissions = append(out.Permissions, support.Permission{Operation: op, Capability: capability, Scope: scope, Callable: op != "secret.use" && op != "sync.delete", Allowed: allowed && s.policy.failed == nil, ApprovalRequired: RequiresApproval(Request{Operation: op})})
	}
	if host != "" {
		out.RuntimeStatus = "permission_denied"
		for _, permission := range out.Permissions {
			if permission.Operation == proto.OpCapabilityProbe && permission.Allowed {
				out.RuntimeStatus = "not_probed"
			}
		}
	}
	return out
}

// DiscoverSupport is shared by CLI and MCP. The broker rechecks authorization
// for the subsequent probe, so a discovery snapshot cannot grant access.
func DiscoverSupport(ctx context.Context, c *Client, host string, refresh bool) (support.Discovery, error) {
	r, err := c.DoContext(ctx, Request{Operation: "support", Host: host})
	if err != nil {
		return support.Discovery{}, err
	}
	if !r.OK {
		return support.Discovery{}, errors.New(r.Error)
	}
	if r.Support == nil {
		return support.Discovery{}, errors.New("broker support returned no result")
	}
	out := *r.Support
	if host == "" {
		return out, nil
	}
	allowed := false
	for _, permission := range out.Permissions {
		if permission.Operation == proto.OpCapabilityProbe {
			allowed = permission.Allowed
			break
		}
	}
	if !allowed {
		return out, nil
	}
	r, err = c.DoContext(ctx, Request{Operation: proto.OpCapabilityProbe, Host: host, Wire: &proto.Request{Op: proto.OpCapabilityProbe, Capability: &proto.CapabilityParams{Refresh: refresh}}})
	if err != nil {
		return out, err
	}
	if !r.OK {
		return out, errors.New(r.Error)
	}
	if r.Wire == nil || r.Wire.Capability == nil {
		return out, errors.New("broker capability returned no result")
	}
	out.SetRuntime(r.Wire.Capability)
	return out, nil
}
