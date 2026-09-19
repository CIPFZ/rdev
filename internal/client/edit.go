package client

import (
	"context"
	"github.com/CIPFZ/rdev/internal/fileedit"
	"github.com/CIPFZ/rdev/internal/proto"
)

func (c *Client) EditFile(ctx context.Context, host string, p proto.EditParams, operationID string) (*proto.EditResult, error) {
	if err := fileedit.Validate(&p); err != nil {
		return nil, err
	}
	if operationID != "" && proto.ValidateOperationID(operationID) != nil {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	response, _, err := c.doBuilt(ctx, host, func(operationIdentity) (*builtRequest, error) {
		return &builtRequest{Request: &proto.Request{Op: proto.OpEditFile, Edit: &p}, StableOperationID: operationID}, nil
	})
	if err != nil {
		return nil, err
	}
	if response == nil || response.Edit == nil {
		return nil, missingResultError(response)
	}
	return response.Edit, nil
}

// EditRollback restores a backup created by EditFile with Backup=true.
func (c *Client) EditRollback(ctx context.Context, host string, p proto.EditRollbackParams, operationID string) (*proto.EditRollbackResult, error) {
	if p.Path == "" || proto.ValidateOperationID(p.BackupID) != nil {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if operationID != "" && proto.ValidateOperationID(operationID) != nil {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	response, _, err := c.doBuilt(ctx, host, func(_ operationIdentity) (*builtRequest, error) {
		return &builtRequest{Request: &proto.Request{Op: proto.OpEditRollback, EditRollback: &p}, StableOperationID: operationID}, nil
	})
	if err != nil {
		return nil, err
	}
	if response == nil || response.EditRollback == nil {
		return nil, missingResultError(response)
	}
	return response.EditRollback, nil
}

// ReadFileSnapshot returns a slice and a digest of the same bounded whole-file
// snapshot. A redacted response deliberately has no edit digest.
func (c *Client) ReadFileSnapshot(ctx context.Context, host, path string, offset, limit int64) (*proto.ReadResult, error) {
	response, err := c.do(ctx, host, &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: path, Offset: offset, Limit: limit, IncludeDigest: true}})
	if err != nil {
		return nil, err
	}
	if response == nil || response.Read == nil {
		return nil, missingResultError(response)
	}
	if response.Read.Digest == "" && !response.Read.Redacted {
		return nil, proto.NewError(proto.CodeUnsupportedFeature, response.OperationID, proto.StateCompleted)
	}
	return response.Read, nil
}

// OperationStatus returns the cached outcome for a prior operation in this
// client session. It is safe after an ambiguous transport result and never
// replays the original request.
func (c *Client) OperationStatus(ctx context.Context, host, targetOperationID string) (*proto.OperationStatusResult, error) {
	if proto.ValidateOperationID(targetOperationID) != nil {
		return nil, proto.NewError(proto.CodeInvalidRequest, targetOperationID, proto.StateNotSent)
	}
	response, err := c.do(ctx, host, &proto.Request{Op: proto.OpOperationStatus, OperationStatus: &proto.OperationStatusParams{OperationID: targetOperationID}})
	if err != nil {
		return nil, err
	}
	if response == nil || response.OperationStatus == nil {
		return nil, missingResultError(response)
	}
	return response.OperationStatus, nil
}
