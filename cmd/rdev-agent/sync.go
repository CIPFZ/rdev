package main

import (
	"context"
	"errors"
	"github.com/CIPFZ/rdev/internal/proto"
	statepkg "github.com/CIPFZ/rdev/internal/state"
	"github.com/CIPFZ/rdev/internal/synctree"
	"path/filepath"
)

func doSync(ctx context.Context, request *proto.Request, state string) (*proto.SyncResult, error) {
	params := request.Sync
	if params == nil || request.ClientID == "" || request.ProjectID == "" {
		return nil, proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
	}
	result := &proto.SyncResult{}
	if request.Op == proto.OpSyncInspect {
		if params.Path == "" || params.Action != "" || params.Manifest != nil || params.Plan != nil || len(params.Data) != 0 {
			return nil, proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		}
		snap, err := synctree.Inspect(ctx, expandHome(params.Path), synctree.StageLimits)
		if err != nil {
			return nil, syncError(err, request.OperationID)
		}
		result.Snapshot = &snap
		return result, nil
	}
	lease, err := statepkg.AcquireWriter(state)
	if err != nil {
		return nil, stateWriteError(err)
	}
	defer lease.Close()
	store, err := synctree.NewStore(filepath.Join(state, ".sync"))
	if err != nil {
		return nil, syncError(err, request.OperationID)
	}
	if request.Op == proto.OpSyncCommit {
		if params.Plan == nil || params.Path == "" || proto.ValidateOperationID(request.OperationID) != nil {
			return nil, proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		}
		outcome, err := store.Execute(ctx, request.ClientID, params.ID, expandHome(params.Path), request.OperationID, *params.Plan)
		result.Outcome = &outcome
		if err != nil {
			return result, syncError(err, request.OperationID)
		}
		return result, nil
	}
	switch params.Action {
	case "capture":
		var stage synctree.Stage
		stage, err = store.Capture(ctx, request.ClientID, params.ID, expandHome(params.Path), params.Policy)
		result.Stage = &stage
	case "begin":
		if params.Manifest == nil {
			return nil, proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		}
		var stage synctree.Stage
		stage, err = store.Begin(ctx, request.ClientID, params.ID, *params.Manifest)
		result.Stage = &stage
	case "put":
		err = store.Put(ctx, request.ClientID, params.ID, params.Index, params.Offset, params.Data)
	case "read":
		result.Data, err = store.Read(ctx, request.ClientID, params.ID, params.Index, params.Offset)
	case "seal":
		var stage synctree.Stage
		stage, err = store.Seal(ctx, request.ClientID, params.ID)
		result.Stage = &stage
	case "remove":
		err = store.Remove(ctx, request.ClientID, params.ID)
	case "outcome":
		if proto.ValidateOperationID(params.OutcomeID) != nil {
			return nil, proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		}
		var outcome synctree.Outcome
		outcome, err = store.Outcome(ctx, request.ClientID, params.OutcomeID)
		result.Outcome = &outcome
	default:
		return nil, proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
	}
	if err != nil {
		return nil, syncError(err, request.OperationID)
	}
	return result, nil
}
func syncError(err error, id string) error {
	code := proto.CodeInvalidRequest
	state := proto.StateFailed
	if errors.Is(err, synctree.ErrLimit) {
		code = proto.CodeLimitExceeded
	}
	if errors.Is(err, synctree.ErrRecorded) {
		code = proto.CodeAmbiguousOutcome
		state = proto.StatePossiblyExecuted
	}
	if errors.Is(err, context.Canceled) {
		code = proto.CodeCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		code = proto.CodeDeadlineExceeded
	}
	return proto.NewError(code, id, state)
}
