package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
)

func OperationReference(req Request) string {
	id := req.MutationID
	if req.Operation != "mutation.status" {
		if isSecretMutation(req.Operation) || isSyncOperation(req.Operation) {
			id = req.OperationID
		} else if req.Wire == nil {
			return ""
		}
		if req.Wire != nil {
			id = req.Wire.OperationID
		}
	}
	if proto.ValidateOperationID(id) != nil {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

func IsWireMutation(req Request) bool {
	if req.Wire == nil {
		return false
	}
	d, ok := proto.LookupOperation(req.Wire.Op)
	return ok && d.Class == proto.ClassMutating
}

// DispatchMutation persists intent before admission and the possibly-executed
// boundary before remote I/O. A repeated identity returns its recorded status;
// it never silently executes again, even when the old remote result is unknown.
func (s *Service) DispatchMutation(ctx context.Context, req Request, plan ApprovalPlan) (*proto.Response, *MutationIntent, error) {
	normalized, err := proto.NormalizeTimeouts(req.Wire)
	if err != nil {
		return nil, nil, err
	}
	// Approval binds the original frontend request; durable execution and its
	// recovery digest bind this one effective private wire snapshot.
	wire := *normalized
	if wire.OperationID == "" {
		id, err := proto.NewOperationID()
		if err != nil {
			return nil, nil, err
		}
		wire.OperationID = id
	}
	if proto.ValidateOperationID(wire.OperationID) != nil {
		return nil, nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	intent := MutationIntent{OperationID: wire.OperationID, Owner: req.Owner.Key(), Host: req.Host, Operation: wire.Op, RequestDigest: plan.RequestDigest, TargetDigest: plan.TargetDigest, PolicyDigest: plan.PolicyDigest, ApprovalID: plan.ApprovalID}
	if wire.Op == proto.OpJobStart {
		if wire.Job == nil || wire.Job.ID != "" || len(wire.Job.IDs) > 0 || wire.Job.DurableStart {
			return nil, nil, errors.New("broker job start requires a spec without caller-selected job identity")
		}
		params := *wire.Job
		params.DurableStart = true
		wire.Job = &params
		var err error
		intent.JobID, err = proto.JobIDForOperation(proto.PrincipalID(req.Owner.ClientID, req.Owner.ProjectID), wire.OperationID)
		if err != nil {
			return nil, nil, err
		}
		intent.JobDigest, err = proto.DurableJobDigest(&wire)
		if err != nil {
			return nil, nil, err
		}
	}
	if wire.Replay {
		old, err := s.Mutations.Get(req.Owner.Key(), wire.OperationID)
		if err != nil {
			return nil, nil, proto.NewError(proto.CodeAmbiguousOutcome, wire.OperationID, proto.StatePossiblyExecuted)
		}
		if !sameMutationBinding(old, intent) {
			return nil, nil, proto.NewError(proto.CodeOperationIDConflict, wire.OperationID, proto.StateNotSent)
		}
		return nil, &old, ErrMutationRecorded
	}
	if err := s.Mutations.Prepare(intent); err != nil {
		if old, getErr := s.Mutations.Get(intent.Owner, intent.OperationID); getErr == nil && errors.Is(err, ErrMutationRecorded) {
			return nil, &old, err
		}
		return nil, nil, err
	}
	status := func() *MutationIntent {
		m, err := s.Mutations.Get(intent.Owner, intent.OperationID)
		if err != nil {
			return &intent
		}
		return &m
	}
	resp, err := s.DispatchScheduled(ctx, req.Host, intent.Owner, LaneForOperation(wire.Op), func(dispatchCtx context.Context) (*proto.Response, error) {
		if intent.JobID != "" {
			if err := s.Jobs.Put(JobRef{ID: intent.JobID, Host: intent.Host, Owner: intent.Owner}); err != nil {
				return nil, err
			}
		}
		if err := s.Mutations.Transition(intent.Owner, intent.OperationID, "dispatched", false); err != nil {
			return nil, err
		}
		return s.DispatchApproved(dispatchCtx, req.Host, &wire, plan.TargetDigest)
	})
	if err != nil {
		m := status()
		state := "ambiguous"
		var before *client.BeforeDispatchError
		// The local marker proves no business dispatch occurred. Otherwise only
		// a registry-valid, correlated terminal response resolves uncertainty;
		// a bare not_sent error from a later reconnect is insufficient.
		beforeDispatch := m.State == "dispatched" && resp == nil && errors.As(err, &before)
		terminal := resp != nil && !resp.OK && resp.Terminal && resp.OperationID == intent.OperationID && resp.Error != nil && resp.Error.Validate() == nil && resp.Error.Terminal && resp.Error.OperationID == intent.OperationID
		if m.State == "prepared" || beforeDispatch || terminal && resp.Execution == proto.StateNotSent && resp.Error.ExecutionState == proto.StateNotSent {
			state = "not_sent"
		} else if terminal && resp.Execution == proto.StateFailed && resp.Error.ExecutionState == proto.StateFailed {
			// The remote handler reached a terminal failure. Record that known
			// outcome, while retaining the identity to prevent any redispatch.
			state = "completed"
		}
		if persistErr := s.Mutations.Transition(intent.Owner, intent.OperationID, state, false); persistErr != nil {
			return nil, status(), fmt.Errorf("mutation outcome not persisted; query mutation.status")
		}
		return resp, status(), err
	}
	if err := s.RecordJobResponse(req.Host, intent.Owner, &wire, resp); err != nil {
		_ = s.Mutations.Transition(intent.Owner, intent.OperationID, "ambiguous", false)
		return nil, status(), errors.New("mutation completed but broker state was not persisted; query mutation.status")
	}
	if err := s.Mutations.Transition(intent.Owner, intent.OperationID, "completed", resp != nil && resp.OK); err != nil {
		return nil, status(), errors.New("mutation completed but outcome was not persisted; query mutation.status")
	}
	return resp, status(), nil
}

// Pending starts reserve ownership before execution, so deletion must not race
// a start that can still create its remote record. Status/recovery resolves an
// ambiguous start using its durable remote identity before removal is allowed.
func (s *Service) ValidateJobRemoval(host, owner string, req *proto.Request) error {
	if req.Op != proto.OpJobRm || req.Job == nil {
		return nil
	}
	for _, m := range s.Mutations.Snapshot() {
		if m.Owner == owner && m.Host == host && m.JobID == req.Job.ID && (m.State == "prepared" || m.State == "dispatched" || m.State == "ambiguous") {
			return errors.New("job start outcome unresolved; query owned job status before removal")
		}
	}
	return nil
}

func mutationMatchesJob(m MutationIntent, info *proto.JobInfo) bool {
	if info == nil {
		return false
	}
	ownerClient, ownerProject := splitOwner(m.Owner)
	return info.ID == m.JobID && info.StartOperationID == m.OperationID && info.StartPrincipalID == proto.PrincipalID(ownerClient, ownerProject) && info.StartDigest == m.JobDigest
}
func splitOwner(owner string) (string, string) {
	for i, c := range owner {
		if c == 0 {
			return owner[:i], owner[i+1:]
		}
	}
	return "", ""
}

func (s *Service) ResolveMutationJob(host, owner string, info *proto.JobInfo) error {
	if info == nil || info.StartOperationID == "" {
		return nil
	}
	m, err := s.Mutations.Get(owner, info.StartOperationID)
	if err != nil {
		return nil
	}
	if m.Host != host || m.State != "ambiguous" {
		return nil
	}
	target, targetErr := s.client.ProtocolTargetIdentity(host)
	if targetErr != nil || target != m.TargetDigest {
		return errors.New("mutation recovery target changed")
	}
	if !mutationMatchesJob(m, info) {
		return errors.New("recovered job identity conflicts with mutation intent")
	}
	if err := s.Mutations.Transition(owner, m.OperationID, "completed", true); err != nil {
		// Concurrent status/wait callers can all observe the same ambiguous
		// snapshot. The first durable resolution also satisfies its peers;
		// never turn that successful commit into a spurious client failure.
		current, getErr := s.Mutations.Get(owner, m.OperationID)
		if getErr == nil && current.State == "completed" && current.RemoteOK && sameMutationBinding(current, m) {
			return nil
		}
		return err
	}
	return nil
}

func (s *Service) RecoverMutationJobs(ctx context.Context) error {
	for _, m := range s.Mutations.Snapshot() {
		if m.JobID == "" || m.State != "ambiguous" {
			continue
		}
		if err := s.Jobs.Put(JobRef{ID: m.JobID, Host: m.Host, Owner: m.Owner}); err != nil {
			return err
		}
		if ctx.Err() != nil {
			continue
		}
		target, err := s.client.ProtocolTargetIdentity(m.Host)
		if err != nil || target != m.TargetDigest {
			continue
		}
		client, project := splitOwner(m.Owner)
		r, err := s.Dispatch(ctx, m.Host, &proto.Request{Op: proto.OpJobStatus, ClientID: client, ProjectID: project, Job: &proto.JobParams{ID: m.JobID}})
		if err != nil || r == nil || !r.OK || r.Job == nil || r.Job.Info == nil {
			continue
		}
		if !mutationMatchesJob(m, r.Job.Info) {
			return errors.New("recovered job identity conflicts with mutation intent")
		}
		if err := s.RecordJobResponse(m.Host, m.Owner, &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: m.JobID}}, r); err != nil {
			return err
		}
		if err := s.ResolveMutationJob(m.Host, m.Owner, r.Job.Info); err != nil {
			return err
		}
	}
	return nil
}
