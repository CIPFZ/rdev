package broker

import (
	"context"
	"errors"

	"github.com/CIPFZ/rdev/internal/proto"
)

func isSecretMutation(operation string) bool {
	return operation == "secret.set" || operation == "secret.delete"
}

func (s *Service) SecretList(req Request) ([]SecretDescriptor, error) {
	target, err := s.client.ProtocolTargetIdentity(req.Host)
	if err != nil {
		return nil, errors.New("secret host unavailable")
	}
	return s.Secrets.List(req.Owner.Key(), req.Host, target)
}

// DispatchSecretMutation records local credentials with the same durable intent
// barrier as remote mutation. A lost ACK never permits silent repeated rotation
// or deletion under an already recorded operation ID.
func (s *Service) DispatchSecretMutation(ctx context.Context, req Request, plan ApprovalPlan) (*MutationIntent, error) {
	if !isSecretMutation(req.Operation) || proto.ValidateOperationID(req.OperationID) != nil {
		return nil, errors.New("invalid secret mutation identity")
	}
	intent := MutationIntent{OperationID: req.OperationID, Owner: req.Owner.Key(), Host: req.Host, Operation: req.Operation, RequestDigest: plan.RequestDigest, TargetDigest: plan.TargetDigest, PolicyDigest: plan.PolicyDigest, ApprovalID: plan.ApprovalID}
	status := func() *MutationIntent {
		m, err := s.Mutations.Get(intent.Owner, intent.OperationID)
		if err != nil {
			return &intent
		}
		return &m
	}
	if err := s.Mutations.Prepare(intent); err != nil {
		if errors.Is(err, ErrMutationRecorded) {
			return status(), err
		}
		return nil, err
	}
	_, err := s.DispatchScheduled(ctx, req.Host, intent.Owner, LaneExec, func(context.Context) (*proto.Response, error) {
		// Hold the registry identity lease across persistence so host replacement
		// cannot publish between the final approval check and the local mutation.
		snapshot, err := s.client.Hosts.Inspect(req.Host)
		if err != nil {
			return nil, errors.New("secret host unavailable")
		}
		release, ok := s.client.Hosts.AcquireIdentity(req.Host, snapshot.Generation, snapshot.Fingerprint)
		if !ok {
			return nil, errors.New("secret host changed")
		}
		defer release()
		target, err := s.client.ProtocolTargetIdentity(req.Host)
		if err != nil || target != plan.TargetDigest {
			return nil, errors.New("approved target changed before dispatch")
		}
		if err := s.Mutations.Transition(intent.Owner, intent.OperationID, "dispatched", false); err != nil {
			return nil, err
		}
		return nil, s.Secrets.Apply(intent.Owner, req.Host, target, req.Operation, req.Secret, plan.RequestDigest)
	})
	if err != nil {
		state := "ambiguous"
		if status().State == "prepared" {
			state = "not_sent"
		}
		if e := s.Mutations.Transition(intent.Owner, intent.OperationID, state, false); e != nil {
			return status(), ErrMutationStorage
		}
		return status(), err
	}
	if err := s.Mutations.Transition(intent.Owner, intent.OperationID, "completed", true); err != nil {
		return status(), ErrMutationStorage
	}
	return status(), nil
}
