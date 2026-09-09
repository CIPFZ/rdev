package broker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// RequestEvent captures the submitted request before expansion or dispatch.
// Instance-private HMACs protect even low-entropy secrets, argv and arbitrary
// target hints from offline guessing. They are comparable only during this
// AuditLog's lifetime: its key is never persisted. RequestRef still correlates
// every retained event after restart, and approvals retain their own digests.
// This method never resolves a host or performs transport/storage work.
func (a *AuditLog) RequestEvent(req Request, requestRef string, decision Decision) (AuditEvent, error) {
	canonical := req
	canonical.ID, canonical.OperationID, canonical.Approval = "", "", ""
	if req.Wire != nil {
		wire := *req.Wire
		wire.ID, wire.OperationID = "", ""
		wire.Replay, wire.StreamWindowBytes = false, 0
		canonical.Wire = &wire
	}
	requestDigest, err := a.digest("request", canonical)
	if err != nil {
		return AuditEvent{}, err
	}
	target := struct {
		Host, Hint, GrantHost, ApprovalHost string
		Owner, GrantOwner, ApprovalOwner    Owner
	}{Host: req.Host, Hint: req.Target, Owner: req.Owner, GrantHost: req.GrantHost, GrantOwner: req.GrantOwner}
	if req.ApprovalSpec != nil {
		target.ApprovalOwner, target.ApprovalHost = req.ApprovalSpec.Owner, req.ApprovalSpec.Host
	}
	targetDigest, err := a.digest("submitted_target", target)
	if err != nil {
		return AuditEvent{}, err
	}
	return AuditEvent{RequestRef: requestRef, Owner: req.Owner.Key(), Operation: req.Operation,
		PolicyDigest: decision.Digest, RequestDigest: requestDigest, TargetDigest: targetDigest,
		DigestScope: "broker_instance", TargetScope: "submitted"}, nil
}

func (a *AuditLog) digest(domain string, value any) (string, error) {
	if a.digestKey == nil {
		return "", errors.New("audit digest identity unavailable")
	}
	mac := hmac.New(sha256.New, a.digestKey[:])
	_, _ = mac.Write([]byte(domain + "\x00"))
	if err := json.NewEncoder(mac).Encode(value); err != nil {
		return "", errors.New("audit request digest unavailable")
	}
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// BindAuditTarget snapshots an already configured host for an authorized route.
// Inspect cannot register an SSH-shaped destination or dial it. Unknown targets
// keep the explicitly marked submitted-target digest; no configuration snapshot
// is claimed for them. Sticky environment values remain protected by the HMAC.
func (s *Service) BindAuditTarget(event *AuditEvent, host string) error {
	snapshot, err := s.client.Hosts.Inspect(host)
	if err != nil {
		return nil
	}
	digest, err := s.Audit.digest("configured_target", snapshot)
	if err != nil {
		return err
	}
	event.TargetDigest, event.TargetScope = digest, "configured"
	return nil
}

func (e *AuditEvent) BindApproval(plan ApprovalPlan, approvalID string) {
	e.RequestDigest, e.TargetDigest, e.ApprovalID = plan.RequestDigest, plan.TargetDigest, approvalID
	e.DigestScope, e.TargetScope = "approval", "approval"
}
