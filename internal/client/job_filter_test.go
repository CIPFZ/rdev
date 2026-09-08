package client

import (
	"context"
	"errors"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

type noJobFilterConn struct{ *fakeRemoteConn }

func (c *noJobFilterConn) SupportsFeature(feature proto.Feature) bool {
	return feature != proto.FeatureJobFilterIDs && proto.IsKnownFeature(feature)
}

func TestScopedJobListRejectsAgentWithoutFilterBeforeSending(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	base := &fakeRemoteConn{host: transport.Host{Name: "host", Addr: "u@h"}}
	if err := c.Hosts.Add(base.host); err != nil {
		t.Fatal(err)
	}
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &noJobFilterConn{base}, nil
	}
	_, err := c.DoProtocol(t.Context(), "host", &proto.Request{
		Op: proto.OpJobList, ClientID: "a", ProjectID: "p",
		Job: &proto.JobParams{FilterIDs: true, IDs: []string{"owned"}, Limit: 1},
	})
	var envelope *proto.ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != proto.CodeUnsupportedFeature {
		t.Fatalf("missing feature error = %v", err)
	}
	base.mu.Lock()
	defer base.mu.Unlock()
	if len(base.ops) != 0 {
		t.Fatal("scoped listing sent to an agent that ignores scope")
	}
}
