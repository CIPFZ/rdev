package broker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// ApprovalSpec contains the exact remote request reviewed by the administrator.
// Client hints such as Risk and Target are deliberately absent.
type ApprovalSpec struct {
	Secret    *SecretParams  `json:"secret,omitempty"`
	Owner     Owner          `json:"owner"`
	Operation string         `json:"operation"`
	Host      string         `json:"host"`
	Wire      *proto.Request `json:"wire"`
	TTL       time.Duration  `json:"ttl,omitempty"`
}

type ApprovalPlan struct {
	resolvedWire  *proto.Request
	ApprovalID    string `json:"-"`
	Owner         Owner  `json:"owner"`
	Operation     string `json:"operation"`
	Host          string `json:"host"`
	TargetDigest  string `json:"target_digest"`
	RequestDigest string `json:"request_digest"`
	PolicyDigest  string `json:"policy_digest"`
}

func RequiresApproval(req Request) bool {
	if descriptor, ok := proto.LookupOperation(req.Operation); ok {
		// Arbitrary argv, stdin and executable contents cannot be proved harmless
		// from a client-declared risk flag. All mutating wire operations need approval.
		if descriptor.Class == proto.ClassMutating {
			return true
		}
	}
	switch req.Operation {
	case "sync.push", "sync.delete", "secret.set", "secret.delete", "fleet.execute":
		return true
	}
	return req.Risk
}

func (s *Service) PlanApproval(spec ApprovalSpec, decision Decision) (ApprovalPlan, error) {
	if err := spec.Owner.Validate(); err != nil {
		return ApprovalPlan{}, err
	}
	if !decision.Allow {
		return ApprovalPlan{}, errors.New("approval target is not authorized")
	}
	if isSecretMutation(spec.Operation) {
		if spec.Wire != nil || spec.Host == "" {
			return ApprovalPlan{}, errors.New("secret approval requires an exact host and parameters")
		}
		target, err := s.client.ProtocolTargetIdentity(spec.Host)
		if err != nil {
			return ApprovalPlan{}, errors.New("secret host unavailable")
		}
		digest, err := s.Secrets.Plan(spec.Owner.Key(), spec.Host, target, spec.Operation, spec.Secret)
		if err != nil {
			return ApprovalPlan{}, err
		}
		return ApprovalPlan{Owner: spec.Owner, Operation: spec.Operation, Host: spec.Host, TargetDigest: target, RequestDigest: digest, PolicyDigest: decision.Digest}, nil
	}
	if spec.Secret != nil {
		return ApprovalPlan{}, errors.New("unexpected secret approval parameters")
	}
	if spec.Wire == nil || spec.Wire.Op != spec.Operation || spec.Host == "" {
		return ApprovalPlan{}, errors.New("approval requires an exact remote wire request")
	}
	if _, err := proto.RequireOperation(spec.Operation); err != nil {
		return ApprovalPlan{}, err
	}
	if (spec.Wire.ClientID != "" && spec.Wire.ClientID != spec.Owner.ClientID) || (spec.Wire.ProjectID != "" && spec.Wire.ProjectID != spec.Owner.ProjectID) {
		return ApprovalPlan{}, errors.New("approval wire owner mismatch")
	}
	target, err := s.client.ProtocolTargetIdentity(spec.Host)
	if err != nil {
		return ApprovalPlan{}, err
	}
	// Connection IDs, retry metadata and transport flow windows are not operation
	// parameters. Owner and absolute semantic deadline remain in the digest.
	wire := *spec.Wire
	wire.ID, wire.OperationID = "", ""
	wire.ClientID, wire.ProjectID = spec.Owner.ClientID, spec.Owner.ProjectID
	wire.Replay = false
	wire.StreamWindowBytes = 0
	payload, err := json.Marshal(struct {
		Host string
		Wire *proto.Request
	}{spec.Host, &wire})
	if err != nil {
		return ApprovalPlan{}, err
	}
	if wireUsesSecrets(spec.Wire) {
		bound := *spec.Wire
		bound.ClientID, bound.ProjectID = spec.Owner.ClientID, spec.Owner.ProjectID
		resolved, digest, err := s.Secrets.Resolve(spec.Owner.Key(), spec.Host, target, &bound)
		if err != nil {
			return ApprovalPlan{}, err
		}
		return ApprovalPlan{Owner: spec.Owner, Operation: spec.Operation, Host: spec.Host, TargetDigest: target, RequestDigest: digest, PolicyDigest: decision.Digest, resolvedWire: resolved}, nil
	}
	digest := sha256.Sum256(payload)
	return ApprovalPlan{Owner: spec.Owner, Operation: spec.Operation, Host: spec.Host, TargetDigest: target, RequestDigest: hex.EncodeToString(digest[:]), PolicyDigest: decision.Digest}, nil
}

func (s *Service) IssueApproval(spec ApprovalSpec) (Approval, error) {
	decision := s.DecideBrokerRequest(Request{Owner: spec.Owner, Operation: spec.Operation, Host: spec.Host, Wire: spec.Wire})
	plan, err := s.PlanApproval(spec, decision)
	if err != nil {
		return Approval{}, err
	}
	ttl := spec.TTL
	if ttl == 0 {
		ttl = time.Minute
	}
	data, _ := json.Marshal(plan)
	approval, err := NewApproval(spec.Owner.Key(), spec.Operation, string(data), ttl)
	if err != nil {
		return Approval{}, err
	}
	// Retained approvals contain digests only. Expanded credentials belong
	// solely to the request that eventually consumes the token.
	storedPlan := plan
	storedPlan.resolvedWire = nil
	approval.Plan = &storedPlan
	s.approvalMu.Lock()
	defer s.approvalMu.Unlock()
	now := time.Now()
	for token, item := range s.approvalByToken {
		if !now.Before(item.ExpiresAt) {
			delete(s.approvalByToken, token)
		}
	}
	if len(s.approvalByToken) >= 4096 {
		return Approval{}, errors.New("approval capacity reached")
	}
	s.approvalByToken[approval.Token] = approval
	return approval, nil
}

func (s *Service) AuthorizeApproval(req Request, decision Decision) (ApprovalPlan, error) {
	plan, err := s.PlanApproval(ApprovalSpec{Owner: req.Owner, Operation: req.Operation, Host: req.Host, Wire: req.Wire, Secret: req.Secret}, decision)
	if err != nil {
		return ApprovalPlan{}, err
	}
	data, _ := json.Marshal(plan)
	s.approvalMu.Lock()
	defer s.approvalMu.Unlock()
	approval, ok := s.approvalByToken[req.Approval]
	if !ok || approval.Plan == nil || approval.Validate(req.Approval, req.Owner.Key(), req.Operation, string(data), time.Now()) != nil {
		return ApprovalPlan{}, errors.New("approval required for this exact request, principal, target and policy")
	}
	// A different owner or changed request cannot consume somebody else's token.
	delete(s.approvalByToken, req.Approval)
	plan.ApprovalID = ApprovalReference(req.Approval)
	return plan, nil
}

func ApprovalReference(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ApplyApprovedWire freezes the validated secret snapshot before admission.
// Later rotation never changes the already approved command or its job digest.
func ApplyApprovedWire(req *Request, plan ApprovalPlan) {
	if plan.resolvedWire != nil {
		req.Wire = plan.resolvedWire
	}
}

func (p ApprovalPlan) ExpandedRequestBytes() int64 {
	if p.resolvedWire == nil {
		return 0
	}
	data, _ := json.Marshal(p.resolvedWire)
	return int64(len(data))
}
