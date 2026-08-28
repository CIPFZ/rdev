package main

import (
	"errors"
	"os"

	"github.com/CIPFZ/rdev/internal/proto"
)

type agentErrorKind uint8

const (
	agentInvalid agentErrorKind = iota + 1
	agentLimit
	agentNotFound
	agentProcessStart
	agentProcessState
)

type agentError struct {
	kind  agentErrorKind
	cause error
}

func (e *agentError) Error() string {
	if e == nil || e.cause == nil {
		return "agent operation failed"
	}
	return e.cause.Error()
}

func (e *agentError) Unwrap() error { return e.cause }

func invalidRequestError(message string) error {
	return &agentError{kind: agentInvalid, cause: errors.New(message)}
}

func limitExceededError(message string) error {
	return &agentError{kind: agentLimit, cause: errors.New(message)}
}

func objectNotFoundError(cause error) error {
	return &agentError{kind: agentNotFound, cause: cause}
}

func processStartError(cause error) error {
	return &agentError{kind: agentProcessStart, cause: cause}
}

func processStateError(message string) error {
	return &agentError{kind: agentProcessState, cause: errors.New(message)}
}

// classifyAgentError is the sole agent error-to-wire boundary. Leaf errors keep
// private diagnostics for local control flow, while the returned envelope is
// registry-backed and never includes a path, argv, or raw OS message.
func classifyAgentError(err error, operationID string) *proto.ErrorEnvelope {
	if err == nil {
		return nil
	}
	var envelope *proto.ErrorEnvelope
	if errors.As(err, &envelope) {
		if envelope.OperationID == operationID {
			return envelope
		}
		copy := *envelope
		copy.OperationID = operationID
		return &copy
	}
	var typed *agentError
	if errors.As(err, &typed) {
		switch typed.kind {
		case agentInvalid:
			return proto.NewError(proto.CodeInvalidRequest, operationID, proto.StateNotSent)
		case agentLimit:
			return proto.NewError(proto.CodeLimitExceeded, operationID, proto.StateNotSent)
		case agentNotFound:
			return proto.NewError(proto.CodeObjectNotFound, operationID, proto.StateFailed)
		case agentProcessStart:
			return proto.NewError(proto.CodeProcessStartFailure, operationID, proto.StateFailed)
		case agentProcessState:
			return proto.NewError(proto.CodeProcessInvalidState, operationID, proto.StateFailed)
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return proto.NewError(proto.CodeObjectNotFound, operationID, proto.StateFailed)
	}
	return proto.NewError(proto.CodeInternalFailure, operationID, proto.StateFailed)
}
