package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/CIPFZ/rdev/internal/proto"
)

var errInvalidBrokerResponse = errors.New("invalid broker response")

// WithError adds the shared typed error without removing the v1 text field.
// Only registry-validated data is forwarded. Durable uncertainty takes priority
// over an error from a later connection attempt or a malformed local envelope.
func (r Response) WithError(err error, operationID string) Response {
	if err == nil {
		return r
	}
	r.OK = false
	if r.Error == "" {
		r.Error = err.Error()
	}
	if proto.ValidateOperationID(operationID) != nil {
		operationID = ""
	}
	if r.Mutation != nil {
		operationID = r.Mutation.OperationID
		if r.Mutation.State == "ambiguous" || r.Mutation.State == "dispatched" {
			r.ErrorEnvelope = proto.NewError(proto.CodeAmbiguousOutcome, operationID, proto.StatePossiblyExecuted)
		}
	}
	var envelope *proto.ErrorEnvelope
	if r.ErrorEnvelope == nil && errors.As(err, &envelope) {
		if envelope.Validate() != nil {
			r.ErrorEnvelope = proto.NewError(proto.CodeInternalFailure, operationID, proto.StatePossiblyExecuted)
		} else {
			copy := *envelope
			if operationID != "" {
				copy.OperationID = operationID
			}
			if copy.Truncation != nil {
				truncation := *copy.Truncation
				copy.Truncation = &truncation
			}
			r.ErrorEnvelope = &copy
		}
	}
	if r.ErrorEnvelope != nil {
		if r.Mutation != nil {
			switch r.Mutation.State {
			case "prepared", "not_sent":
				r.ErrorEnvelope.ExecutionState = proto.StateNotSent
			case "completed":
				if r.ErrorEnvelope.ExecutionState == proto.StateNotSent {
					r.ErrorEnvelope = proto.NewError(proto.CodeInternalFailure, operationID, proto.StateFailed)
				}
			}
		}
		// Do not expose wrappers, paths or diagnostics accompanying a typed
		// error. Its fixed registry message also serves old v1 frontends.
		r.Error = r.ErrorEnvelope.Message
	}
	return r
}

// Failure is shared by CLI and MCP. Old brokers may only return text; they
// remain supported without guessing a code from arbitrary strings.
func (r Response) Failure() error {
	if err := r.validateErrors(); err != nil {
		return proto.NewError(proto.CodeInvalidFrame, "", proto.StatePossiblyExecuted)
	}
	if r.ErrorEnvelope != nil {
		return r.ErrorEnvelope
	}
	if !r.OK {
		if r.Error != "" {
			return errors.New(r.Error)
		}
		return errors.New("broker request failed")
	}
	if r.Wire != nil {
		if r.Wire.Error != nil {
			return r.Wire.Error
		}
		if r.Wire.Err != "" {
			return errors.New(r.Wire.Err)
		}
		if !r.Wire.OK {
			return errors.New("remote request failed")
		}
	}
	return nil
}

func (r Response) validateErrors() error {
	if e := r.ErrorEnvelope; e != nil {
		if e.Validate() != nil || r.OK || r.Wire != nil || r.Error != "" && r.Error != e.Message {
			return errors.New("invalid broker error envelope")
		}
		if m := r.Mutation; m != nil {
			if e.OperationID != m.OperationID {
				return errors.New("broker error mutation identity mismatch")
			}
			switch m.State {
			case "prepared", "not_sent":
				if e.ExecutionState != proto.StateNotSent {
					return errors.New("broker error outcome mismatch")
				}
			case "ambiguous", "dispatched":
				if e.ExecutionState != proto.StatePossiblyExecuted {
					return errors.New("broker error outcome mismatch")
				}
			case "completed":
				if e.ExecutionState == proto.StateNotSent {
					return errors.New("broker error outcome mismatch")
				}
			default:
				return errors.New("invalid broker mutation outcome")
			}
		}
	}
	if r.Wire != nil && r.Wire.Error != nil {
		e := r.Wire.Error
		if e.Validate() != nil || r.Wire.OK || e.OperationID != r.Wire.OperationID || e.ExecutionState != r.Wire.Execution || e.Terminal != r.Wire.Terminal {
			return errors.New("invalid broker wire error envelope")
		}
		if m := r.Mutation; m != nil {
			// A later wire rejection must not contradict the broker's durable
			// intent, even if the standalone envelope is registry-valid.
			check := Response{ErrorEnvelope: e, Mutation: m}
			if err := check.validateErrors(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Read the top-level fields without allowing case aliases or duplicate keys to
// hide an earlier envelope. Unknown outer response fields remain v1-compatible;
// an envelope itself is deliberately closed and strictly registry-versioned.
func responseFields(data []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return nil, errors.New("invalid response object")
	}
	fields := make(map[string]json.RawMessage)
	seen := make(map[string]bool)
	for d.More() {
		t, err := d.Token()
		key, ok := t.(string)
		if err != nil || !ok || seen[strings.ToLower(key)] {
			return nil, errors.New("duplicate response field")
		}
		seen[strings.ToLower(key)] = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil {
			return nil, errors.New("invalid response field")
		}
		fields[key] = raw
	}
	if _, err := d.Token(); err != nil {
		return nil, errors.New("invalid response object")
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errors.New("trailing response data")
	}
	return fields, nil
}

func strictResponseEnvelope(data []byte) error {
	if len(data) > 16<<10 {
		return errors.New("oversized error envelope")
	}
	fields, err := responseFields(data)
	if err != nil {
		return err
	}
	for key := range fields {
		switch key {
		case "code", "category", "message", "retry", "retryable", "execution_state", "operation_id", "terminal", "truncation":
		default:
			return errors.New("unknown error envelope field")
		}
	}
	for _, key := range []string{"code", "category", "message", "retry", "retryable", "execution_state", "terminal"} {
		if _, ok := fields[key]; !ok {
			return errors.New("missing error envelope field")
		}
	}
	var envelope proto.ErrorEnvelope
	if decodeFleetJSON(data, &envelope) != nil || envelope.Validate() != nil {
		return errors.New("invalid error envelope")
	}
	return nil
}

func (r *Response) UnmarshalJSON(data []byte) (err error) {
	defer func() {
		if err != nil {
			err = errInvalidBrokerResponse
		}
	}()
	fields, err := responseFields(data)
	if err != nil {
		return err
	}
	for key, raw := range fields {
		if strings.EqualFold(key, "error_envelope") {
			if key != "error_envelope" || strictResponseEnvelope(raw) != nil {
				return errors.New("invalid broker error envelope")
			}
		}
		if strings.EqualFold(key, "wire") && string(raw) != "null" {
			wire, err := responseFields(raw)
			if err != nil {
				return err
			}
			for field, value := range wire {
				if strings.EqualFold(field, "error") && (field != "error" || strictResponseEnvelope(value) != nil) {
					return errors.New("invalid wire error envelope")
				}
			}
		}
	}
	type wireResponse Response
	var decoded wireResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if err := Response(decoded).validateErrors(); err != nil {
		return err
	}
	*r = Response(decoded)
	return nil
}

func requestOperationID(req Request) string {
	id := req.OperationID
	if req.Wire != nil {
		id = req.Wire.OperationID
	}
	if proto.ValidateOperationID(id) != nil {
		return ""
	}
	return id
}

func (r Response) validateErrorBinding(req Request) error {
	if err := r.validateErrors(); err != nil {
		return err
	}
	e := r.ErrorEnvelope
	if e == nil && r.Wire != nil {
		e = r.Wire.Error
	}
	if e == nil {
		return nil
	}
	if r.ID != req.ID {
		return errors.New("broker response identity mismatch")
	}
	id := requestOperationID(req)
	if id != "" && e.OperationID != id {
		return errors.New("broker error operation identity mismatch")
	}
	return nil
}
