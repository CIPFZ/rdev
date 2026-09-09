package client

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestBrokerOperationIdentitySurvivesClientRetry(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	host := transport.Host{Name: "h", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	var seen []proto.Request
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: host, handler: func(r *proto.Request) (*proto.Response, error) {
			seen = append(seen, *r)
			if len(seen) == 1 {
				return nil, io.EOF
			}
			return &proto.Response{OK: true, OperationID: r.OperationID}, nil
		}}, nil
	}
	id := "op_stable_broker_identity"
	_, err := c.DoProtocol(t.Context(), "h", &proto.Request{Op: proto.OpJobStart, ClientID: "a", ProjectID: "p", OperationID: id, Job: &proto.JobParams{DurableStart: true, Spec: &proto.ExecParams{Argv: []string{"true"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0].OperationID != id || seen[1].OperationID != id || seen[0].Replay || !seen[1].Replay || seen[0].ClientID != proto.PrincipalID("a", "p") {
		t.Fatalf("unstable broker retry: %+v", seen)
	}
}

type noDurableJobConn struct{ *fakeRemoteConn }

func (c *noDurableJobConn) SupportsFeature(feature proto.Feature) bool {
	return feature != proto.FeatureDurableJobStart && proto.IsKnownFeature(feature)
}

func TestDurableStartRequiresNegotiatedFeatureBeforeSending(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	base := &fakeRemoteConn{host: transport.Host{Name: "h", Addr: "u@h"}}
	if err := c.Hosts.Add(base.host); err != nil {
		t.Fatal(err)
	}
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &noDurableJobConn{base}, nil
	}
	_, err := c.DoProtocol(t.Context(), "h", &proto.Request{Op: proto.OpJobStart, ClientID: "a", ProjectID: "p", OperationID: "op_unsupported_durable", Job: &proto.JobParams{DurableStart: true, Spec: &proto.ExecParams{Argv: []string{"true"}}}})
	var envelope *proto.ErrorEnvelope
	var before *BeforeDispatchError
	if !errors.As(err, &before) {
		t.Fatalf("initial feature rejection lost pre-send proof: %v", err)
	}
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeUnsupportedFeature {
		t.Fatalf("unsupported durable agent: %v", err)
	}
	base.mu.Lock()
	defer base.mu.Unlock()
	if len(base.ops) != 0 {
		t.Fatal("job sent to agent without durable-start support")
	}
}

func TestDurableRetryRejectionCannotEraseFirstAttemptUncertainty(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	host := transport.Host{Name: "h", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, io.EOF
			}
			err := proto.NewError(proto.CodeQueueFull, req.OperationID, proto.StateNotSent)
			return &proto.Response{OperationID: req.OperationID, Terminal: true, Execution: proto.StateNotSent, Error: err}, err
		}}, nil
	}
	resp, err := c.DoProtocol(t.Context(), "h", &proto.Request{Op: proto.OpJobStart, ClientID: "a", ProjectID: "p", OperationID: "op_ambiguous_retry", Job: &proto.JobParams{DurableStart: true, Spec: &proto.ExecParams{Argv: []string{"true"}}}})
	var envelope *proto.ErrorEnvelope
	if attempts != 2 || resp != nil || !errors.As(err, &envelope) || envelope.Code != proto.CodeAmbiguousOutcome || envelope.ExecutionState != proto.StatePossiblyExecuted {
		t.Fatalf("later rejection erased uncertain first attempt: response=%v err=%v", resp, err)
	}
}

func TestDurableReconnectFeatureRejectionPreservesUncertainty(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	host := transport.Host{Name: "h", Addr: "u@h"}
	if err := c.Hosts.Add(host); err != nil {
		t.Fatal(err)
	}
	dials, sent := 0, 0
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		dials++
		base := &fakeRemoteConn{host: host, handler: func(req *proto.Request) (*proto.Response, error) { sent++; return nil, io.EOF }}
		if dials == 1 {
			return base, nil
		}
		return &noDurableJobConn{base}, nil
	}
	_, err := c.DoProtocol(t.Context(), "h", &proto.Request{Op: proto.OpJobStart, ClientID: "a", ProjectID: "p", OperationID: "op_review_reconnect_feature", Job: &proto.JobParams{DurableStart: true, Spec: &proto.ExecParams{Argv: []string{"true"}}}})
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeAmbiguousOutcome || envelope.ExecutionState != proto.StatePossiblyExecuted || envelope.OperationID != "op_review_reconnect_feature" {
		t.Fatalf("first attempt uncertainty lost: dials=%d sent=%d error=%+v", dials, sent, envelope)
	}
	if sent != 1 || dials != 2 || err == nil {
		t.Fatalf("bad fixture dials=%d sent=%d err=%v", dials, sent, err)
	}
}
