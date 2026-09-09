package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestBrokerErrorProjectionPreservesBindingAndUncertainty(t *testing.T) {
	const id = "op_error_binding"
	for _, state := range []string{"not_sent", "ambiguous", "dispatched", "completed"} {
		t.Run(state, func(t *testing.T) {
			source := proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent)
			r := (Response{Mutation: &MutationIntent{OperationID: id, State: state}}).WithError(fmt.Errorf("private-path: %w", source), id)
			e := r.ErrorEnvelope
			if e == nil || e.Validate() != nil || e.OperationID != id || r.OK || strings.Contains(r.Error, "private-path") || source.OperationID != "" {
				t.Fatalf("bad projection: %+v", r)
			}
			if state == "not_sent" && (e.Code != proto.CodeReleaseUntrusted || e.ExecutionState != proto.StateNotSent) {
				t.Fatal("setup refusal lost identity")
			}
			if (state == "ambiguous" || state == "dispatched") && (e.Code != proto.CodeAmbiguousOutcome || e.ExecutionState != proto.StatePossiblyExecuted) {
				t.Fatal("later rejection erased uncertainty")
			}
			if state == "completed" && e.ExecutionState == proto.StateNotSent {
				t.Fatal("completed operation reported not_sent")
			}
		})
	}
	for _, change := range []func(*proto.ErrorEnvelope){func(e *proto.ErrorEnvelope) { e.Message = "secret-value" }, func(e *proto.ErrorEnvelope) { e.Code = "unknown" }} {
		e := proto.NewError(proto.CodeReleaseUntrusted, id, proto.StateNotSent)
		change(e)
		r := (Response{}).WithError(e, id)
		if r.ErrorEnvelope.Code != proto.CodeInternalFailure || strings.Contains(r.Error, "secret-value") || r.ErrorEnvelope.ExecutionState != proto.StatePossiblyExecuted {
			t.Fatal("forged envelope projected", r)
		}
	}
}

func TestBrokerEnvelopeStrictJSON(t *testing.T) {
	e := proto.NewError(proto.CodeReleaseUntrusted, "op_json_error_binding", proto.StateNotSent)
	raw, err := json.Marshal(Response{ID: "request", Error: e.Message, ErrorEnvelope: e})
	if err != nil {
		t.Fatal(err)
	}
	var got Response
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	var envelope *proto.ErrorEnvelope
	if !errors.As(got.Failure(), &envelope) || envelope.OperationID != e.OperationID {
		t.Fatal("typed envelope lost")
	}
	for name, edit := range map[string]func(string) string{
		"unknown_code": func(s string) string { return strings.Replace(s, "release.untrusted", "release.unknown", 1) },
		"wrong_message": func(s string) string {
			return strings.Replace(s, `"message":`, `"message":"private-value","ignored":`, 1)
		},
		"duplicate":     func(s string) string { return strings.Replace(s, `"code":`, `"code":"release.untrusted","code":`, 1) },
		"case_field":    func(s string) string { return strings.Replace(s, `"code":`, `"Code":`, 1) },
		"unknown_field": func(s string) string { return strings.Replace(s, `"code":`, `"future":true,"code":`, 1) },
		"missing_false": func(s string) string { return strings.Replace(s, `"retryable":false,`, "", 1) },
		"null": func(s string) string {
			return strings.Replace(s, `"error_envelope":{`, `"error_envelope":null,"ignored":{`, 1)
		},
		"case_outer":      func(s string) string { return strings.Replace(s, `"error_envelope":`, `"ERROR_ENVELOPE":`, 1) },
		"ok_with_error":   func(s string) string { return strings.Replace(s, `"ok":false`, `"ok":true`, 1) },
		"legacy_mismatch": func(s string) string { return strings.Replace(s, `"error":`, `"error":"private-value","ignored":`, 1) },
	} {
		t.Run(name, func(t *testing.T) {
			if json.Unmarshal([]byte(edit(string(raw))), &Response{}) == nil {
				t.Fatal("malformed envelope accepted")
			}
		})
	}
	// Older brokers return only text. No code is inferred from those words.
	if err := json.Unmarshal([]byte(`{"id":"old","ok":false,"error":"release.untrusted"}`), &got); err != nil {
		t.Fatal(err)
	}
	if errors.As(got.Failure(), &envelope) || got.Failure().Error() != "release.untrusted" {
		t.Fatal("legacy error guessed a registry identity")
	}
}

func FuzzBrokerErrorResponse(f *testing.F) {
	seed, _ := json.Marshal(Response{ID: "request", ErrorEnvelope: proto.NewError(proto.CodeReleaseUntrusted, "op_fuzz_error_binding", proto.StateNotSent)})
	f.Add(seed)
	f.Add([]byte(`{"id":"legacy","ok":false,"error":"old broker refusal"}`))
	f.Add([]byte(`{"ok":false,"error_envelope":{"code":"unknown","code":"release.untrusted"}}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			t.Skip()
		}
		var r Response
		if json.Unmarshal(data, &r) != nil {
			return
		}
		if err := r.validateErrors(); err != nil {
			t.Fatal("decoder accepted invalid envelope")
		}
		if r.ErrorEnvelope != nil && r.Failure() == nil {
			t.Fatal("typed refusal became success")
		}
	})
}

func TestBrokerClientRejectsUncorrelatedEnvelope(t *testing.T) {
	for _, bad := range []string{"", "request", "operation", "mutation", "forged"} {
		t.Run(bad, func(t *testing.T) {
			a, b := net.Pipe()
			c := &Client{conn: a, owner: Owner{ClientID: "a", ProjectID: "p"}}
			defer c.Close()
			defer b.Close()
			done := make(chan error, 1)
			go func() {
				var req Request
				if err := json.NewDecoder(b).Decode(&req); err != nil {
					done <- err
					return
				}
				e := proto.NewError(proto.CodeReleaseUntrusted, req.Wire.OperationID, proto.StateNotSent)
				r := Response{ID: req.ID, Error: e.Message, ErrorEnvelope: e}
				switch bad {
				case "request":
					r.ID = "other"
				case "operation":
					e.OperationID = "op_other_error_binding"
				case "mutation":
					r.Mutation = &MutationIntent{OperationID: e.OperationID, State: "ambiguous"}
				case "forged":
					e.Message = "private-value"
				}
				done <- json.NewEncoder(b).Encode(r)
			}()
			r, err := c.Do(Request{Operation: proto.OpExec, Wire: &proto.Request{Op: proto.OpExec, OperationID: "op_client_error_binding", Exec: &proto.ExecParams{Argv: []string{"true"}}}})
			c.Close()
			if serverErr := <-done; serverErr != nil {
				t.Fatal(serverErr)
			}
			var e *proto.ErrorEnvelope
			if bad == "" {
				if err != nil || !errors.As(r.Failure(), &e) || e.Code != proto.CodeReleaseUntrusted {
					t.Fatalf("valid error lost: %+v %v", r, err)
				}
			} else if !errors.As(err, &e) || e.Code != proto.CodeInvalidFrame || e.ExecutionState != proto.StatePossiblyExecuted || e.OperationID != "op_client_error_binding" || strings.Contains(err.Error(), "private-value") {
				t.Fatalf("invalid broker error not fail-closed: %v", err)
			}
		})
	}
}

func TestMutationOnlyLocalBeforeDispatchProofIsNotSent(t *testing.T) {
	for _, kind := range []string{"before", "bare", "reconnect", "forged_remote", "remote_rejected"} {
		t.Run(kind, func(t *testing.T) {
			s := NewService(nil)
			defer s.Close(context.Background())
			if err := s.Client().Hosts.Add(transport.Host{Name: "h", Addr: "u@h"}); err != nil {
				t.Fatal(err)
			}
			m := testMutation("a\x00p", "op_setup_mutation")
			m.TargetDigest, _ = s.Client().ProtocolTargetIdentity("h")
			calls := 0
			s.SetDispatcher(func(context.Context, string, *proto.Request) (*proto.Response, error) {
				calls++
				e := proto.NewError(proto.CodeReleaseUntrusted, m.OperationID, proto.StateNotSent)
				switch kind {
				case "before":
					return nil, &client.BeforeDispatchError{Cause: e}
				case "bare":
					return nil, e
				case "reconnect":
					return nil, fmt.Errorf("first response lost (reconnect: %v)", e)
				case "forged_remote":
					e.Message = "invalid-registry-message"
				}
				return &proto.Response{OperationID: m.OperationID, Execution: proto.StateNotSent, Terminal: true, Error: e}, e
			})
			req := Request{Owner: Owner{ClientID: "a", ProjectID: "p"}, Host: "h", Wire: &proto.Request{Op: proto.OpExec, OperationID: m.OperationID, Exec: &proto.ExecParams{Argv: []string{"true"}}}}
			plan := ApprovalPlan{RequestDigest: m.RequestDigest, TargetDigest: m.TargetDigest, PolicyDigest: m.PolicyDigest, ApprovalID: m.ApprovalID}
			_, got, err := s.DispatchMutation(t.Context(), req, plan)
			want := "ambiguous"
			if kind == "before" || kind == "remote_rejected" {
				want = "not_sent"
			}
			if err == nil || got == nil || got.State != want {
				t.Fatalf("outcome %v %v, want %s", got, err, want)
			}
			_, again, err := s.DispatchMutation(t.Context(), req, plan)
			if !errors.Is(err, ErrMutationRecorded) || calls != 1 || again.State != want {
				t.Fatal("recorded intent was replayed", calls, again, err)
			}
		})
	}
}

func TestBrokerClientTransportLossRemainsUncertain(t *testing.T) {
	a, b := net.Pipe()
	c := &Client{conn: a, owner: Owner{ClientID: "a", ProjectID: "p"}}
	defer c.Close()
	done := make(chan error, 1)
	go func() {
		defer b.Close()
		var req Request
		done <- json.NewDecoder(b).Decode(&req)
	}()
	const id = "op_transport_loss_uncertain"
	_, err := c.Do(Request{Operation: proto.OpExec, Wire: &proto.Request{Op: proto.OpExec, OperationID: id, Exec: &proto.ExecParams{Argv: []string{"true"}}}})
	if readErr := <-done; readErr != nil {
		t.Fatal(readErr)
	}
	var envelope *proto.ErrorEnvelope
	if !errors.Is(err, io.EOF) || errors.As(err, &envelope) || !strings.Contains(err.Error(), id) {
		t.Fatalf("transport loss became a definitive protocol outcome: %v", err)
	}
}
