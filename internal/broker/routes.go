package broker

import (
	"errors"

	"github.com/CIPFZ/rdev/internal/proto"
)

// ValidateRoute runs after authorization but before approval consumption, state
// changes or transport acquisition. A grant cannot create an absent handler or
// turn a host-limited administration grant into global authority.
func ValidateRoute(req Request) error {
	if req.Wire != nil && (req.Wire.Exec != nil || req.Wire.Job != nil) {
		if _, err := proto.NormalizeTimeouts(req.Wire); err != nil {
			return err
		}
	}
	if req.Wire != nil && req.Wire.Sync != nil {
		return errors.New("internal sync parameters cannot be submitted as wire operations")
	}
	if req.Operation == proto.OpSyncInspect || req.Operation == proto.OpSyncStage || req.Operation == proto.OpSyncCommit {
		return errors.New("internal sync operation cannot be submitted directly")
	}
	if req.Sync != nil && !isSyncOperation(req.Operation) {
		return errors.New("unexpected sync parameters")
	}
	if isSyncOperation(req.Operation) {
		return validateSyncRoute(req)
	}
	if req.Secret != nil && req.Operation != "secret.set" && req.Operation != "secret.delete" && req.Operation != "secret.list" && req.Operation != "secret.set_from_file" {
		return errors.New("unexpected secret parameters")
	}
	switch req.Operation {
	case "support":
		if req.Wire != nil || req.Risk || req.ApprovalSpec != nil || len(req.Host) > 512 {
			return errors.New("support requires a read-only local request with an optional host")
		}
		return nil
	case "secret.set", "secret.delete", "secret.list", "secret.set_from_file":
		if req.Wire != nil || req.Host == "" {
			return errors.New("secret operation requires an exact host without wire parameters")
		}
		if req.Operation == "secret.set_from_file" {
			if req.Secret == nil || req.Secret.Value != "" {
				return errors.New("file import requires source parameters without an inline value")
			}
			validation := *req.Secret
			validation.Value = "validation-only"
			return validateSecretParams(req.Operation, &validation)
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
