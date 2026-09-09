package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestReleaseRejectionWithDeclaredSecretsKeepsCodeAndCanReconnect(t *testing.T) {
	for _, code := range []proto.ErrorCode{proto.CodeReleasePolicy, proto.CodeReleaseUntrusted, proto.CodeReleaseChannel, proto.CodeReleaseVersion, proto.CodeUnsupportedPlatform, proto.CodeStateIncompatible, proto.CodeUnsupportedFeature, proto.CodeLimitExceeded, proto.CodeProcessInvalidState} {
		t.Run(string(code), func(t *testing.T) {
			c := newTestClient()
			defer c.Close()
			if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
				t.Fatal(err)
			}
			c.Hosts.Update("dev", func(st *session.State) { st.Secrets = map[string]string{"token": "~/token"} })
			if err := c.Secrets.Set(secrets.OutputKey("registry-word"), "release"); err != nil {
				t.Fatal(err)
			}
			calls := 0
			fixed := false
			c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
				calls++
				if !fixed {
					return nil, fmt.Errorf("unloaded-private-value: %w", proto.NewError(code, "", proto.StateNotSent))
				}
				return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
					if req.Op == proto.OpReadFile {
						return &proto.Response{OK: true, Read: &proto.ReadResult{Content: "protected-token-value", Size: 21, EOF: true}}, nil
					}
					return &proto.Response{OK: true, Exec: &proto.ExecResult{}}, nil
				}}, nil
			}
			_, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}})
			var envelope *proto.ErrorEnvelope
			if !errors.As(err, &envelope) || envelope.Code != code || envelope.Validate() != nil || envelope.ExecutionState != proto.StateNotSent || strings.Contains(err.Error(), "private-value") {
				t.Fatalf("release rejection changed or leaked: %v", err)
			}
			if c.ConnectionSecurity("dev").State != observe.SecurityCold {
				t.Fatal("release policy refusal poisoned secret initialization")
			}
			fixed = true // policy repair does not require a host/secret generation change
			if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}}); err != nil {
				t.Fatalf("fixed release policy cannot reconnect: %v", err)
			}
			if calls != 2 || c.ConnectionSecurity("dev").State != observe.SecurityReady {
				t.Fatalf("reconnection calls=%d security=%s", calls, c.ConnectionSecurity("dev").State)
			}
		})
	}
}

func TestMalformedReleaseEnvelopeDoesNotBypassSecretProtection(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@h"}); err != nil {
		t.Fatal(err)
	}
	c.Hosts.Update("dev", func(st *session.State) { st.Secrets = map[string]string{"token": "~/token"} })
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		envelope := proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent)
		envelope.Message = "private-value-not-in-registry"
		return nil, envelope
	}
	_, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}})
	if err == nil || strings.Contains(err.Error(), "private-value") || c.ConnectionSecurity("dev").State != observe.SecurityFailed {
		t.Fatalf("malformed envelope escaped fail-closed protection: %v", err)
	}
}

func TestInitialSetupFailureIsBeforeBusinessDispatch(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(fmt.Sprintf("bulk=%t", bulk), func(t *testing.T) {
			c := newTestClient()
			defer c.Close()
			host := transport.Host{Name: "h", Addr: "u@h"}
			if err := c.Hosts.Add(host); err != nil {
				t.Fatal(err)
			}
			const token = "setup-error-secret-value"
			if err := c.Secrets.Set(secrets.OutputKey("token"), token); err != nil {
				t.Fatal(err)
			}
			dials, dispatched := 0, 0
			c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
				dials++
				if bulk && dials == 1 {
					return &fakeRemoteConn{host: host, handler: func(*proto.Request) (*proto.Response, error) {
						dispatched++
						return nil, errors.New("unexpected business request")
					}}, nil
				}
				return nil, errors.New("dial failed: " + token)
			}
			built := false
			_, _, err := c.doBuiltForLane(t.Context(), "h", "", bulk, func(operationIdentity) (*builtRequest, error) {
				built = true
				return &builtRequest{Request: &proto.Request{Op: proto.OpPing}}, nil
			})
			var before *BeforeDispatchError
			if !errors.As(err, &before) || strings.Contains(err.Error(), token) || built || dispatched != 0 || before.Unwrap() == nil {
				t.Fatalf("initial setup dispatch marker or redaction invalid: built=%t dispatched=%d err=%v", built, dispatched, err)
			}
			if !errors.As(c.redactErr(err), &before) {
				t.Fatal("public method redaction discarded before-dispatch marker")
			}
		})
	}
}

func TestReconnectSetupRejectionDoesNotClaimBeforeDispatch(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(fmt.Sprintf("bulk=%t", bulk), func(t *testing.T) {
			c := newTestClient()
			defer c.Close()
			host := transport.Host{Name: "h", Addr: "u@h"}
			if err := c.Hosts.Add(host); err != nil {
				t.Fatal(err)
			}
			const token = "reconnect-error-secret-value"
			if err := c.Secrets.Set(secrets.OutputKey("token"), token); err != nil {
				t.Fatal(err)
			}
			dials, dispatched := 0, 0
			const operationID = "op_setup_reconnect_uncertain"
			c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
				dials++
				if dispatched > 0 {
					return nil, fmt.Errorf("%s: %w", token, proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent))
				}
				return &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
					dispatched++
					if req.OperationID != operationID || req.Replay {
						t.Fatalf("unexpected first request identity: %+v", req)
					}
					return nil, io.EOF
				}}, nil
			}
			req := &proto.Request{Op: proto.OpJobStart, ClientID: "owner", ProjectID: "project", OperationID: operationID,
				Job: &proto.JobParams{DurableStart: true, Spec: &proto.ExecParams{Argv: []string{"true"}}}}
			_, err := c.doProtocolLane(t.Context(), "h", req, "", bulk)
			var before *BeforeDispatchError
			var envelope *proto.ErrorEnvelope
			if err == nil || errors.As(err, &before) || errors.As(err, &envelope) || !errors.Is(err, io.EOF) || strings.Contains(err.Error(), token) || dispatched != 1 {
				t.Fatalf("reconnect erased prior uncertainty: dispatched=%d dials=%d err=%v", dispatched, dials, err)
			}
		})
	}
}
