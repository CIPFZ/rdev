package main

import (
	"context"
	"github.com/CIPFZ/rdev/internal/proto"
)

// Directory flock and fd-relative publication need a native Windows equivalent
// before editing can claim the same concurrency contract on that platform.
func doEdit(_ context.Context, _ *proto.EditParams) (*proto.EditResult, error) {
	return nil, proto.NewError(proto.CodeUnsupportedPlatform, "", proto.StateNotSent)
}

func doEditRollback(_ context.Context, _ *proto.EditRollbackParams) (*proto.EditRollbackResult, error) {
	return nil, proto.NewError(proto.CodeUnsupportedPlatform, "", proto.StateNotSent)
}
