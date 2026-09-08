package broker

import (
	"errors"

	"github.com/CIPFZ/rdev/internal/proto"
)

// ValidateRoute runs after authorization but before approval consumption, state
// changes or transport acquisition. A grant cannot create an absent handler or
// turn a host-limited administration grant into global authority.
func ValidateRoute(req Request) error {
	if req.Secret != nil && req.Operation != "secret.set" && req.Operation != "secret.delete" && req.Operation != "secret.list" {
		return errors.New("unexpected secret parameters")
	}
	switch req.Operation {
	case "secret.set", "secret.delete", "secret.list":
		if req.Wire != nil || req.Host == "" {
			return errors.New("secret operation requires an exact host without wire parameters")
		}
		return validateSecretParams(req.Operation, req.Secret)
	case "status", "pool.health", "audit.health", "audit_query":
		if req.Wire != nil || req.Host != "" {
			return errors.New("this broker query requires an unscoped local request")
		}
		return nil
	case "mutation.status", "job.events":
		if req.Wire != nil {
			return errors.New("local broker query cannot contain a wire request")
		}
		return nil
	case "approval.create":
		if req.Wire != nil {
			return errors.New("approval administration cannot contain an outer wire request")
		}
		if req.Host != "" && (req.ApprovalSpec == nil || req.Host != req.ApprovalSpec.Host) {
			return errors.New("approval target exceeds the authorized host scope")
		}
		return nil
	case "policy.grant":
		if req.Wire != nil {
			return errors.New("policy administration cannot contain a wire request")
		}
		if req.Host != "" && req.Host != req.GrantHost {
			return errors.New("policy update exceeds the authorized host scope")
		}
		return nil
	default:
		if _, err := proto.RequireOperation(req.Operation); err != nil {
			return errors.New("unsupported broker operation")
		}
		if req.Wire == nil {
			return errors.New("remote operation requires a wire request")
		}
		if req.Wire.Op != req.Operation {
			return errors.New("operation mismatch")
		}
		if req.Host == "" {
			return errors.New("host required for wire request")
		}
		return nil
	}
}
