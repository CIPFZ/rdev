package client

import (
	"errors"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
)

// BeforeDispatchError proves that connection acquisition or local request validation failed before this
// operation reached the business transport. A broker may use this local type
// to resolve its pending intent as not_sent. A reconnect error after an earlier
// dispatch must never carry this marker, even when that reconnect was refused.
// Cause has already passed the client's secret redaction boundary.
type BeforeDispatchError struct {
	Cause error
}

func (e *BeforeDispatchError) Error() string { return e.Cause.Error() }
func (e *BeforeDispatchError) Unwrap() error { return e.Cause }

func (c *Client) redactSetupErrWith(snapshot *secrets.Store, err error) error {
	if canonical := canonicalSetupRejection(err); canonical != nil {
		// Only connection setup may discard metadata in favor of this local
		// registry decision. Business errors retain operation identity/state.
		return canonical
	}
	return c.redactErrWith(snapshot, err)
}

// Local setup decisions must retain their machine-readable meaning without
// leaking wrappers written before declared secrets were available. Reconstruct
// only a validated registry error; never forward raw diagnostics or metadata.
func canonicalSetupRejection(err error) *proto.ErrorEnvelope {
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Validate() != nil || envelope.ExecutionState != proto.StateNotSent {
		return nil
	}
	switch envelope.Code {
	case proto.CodeReleasePolicy, proto.CodeReleaseUntrusted, proto.CodeReleaseChannel, proto.CodeReleaseVersion, proto.CodeUnsupportedPlatform, proto.CodeStateIncompatible,
		proto.CodeUnsupportedFeature, proto.CodeLimitExceeded, proto.CodeProcessInvalidState:
		return proto.NewError(envelope.Code, "", proto.StateNotSent)
	default:
		return nil
	}
}
