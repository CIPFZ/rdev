package main

import (
	"bytes"
	"context"
	"github.com/CIPFZ/rdev/internal/proto"
	"testing"
)

// The initial hello precedes feature negotiation and must retain its unary
// shape. Subsequent ping requests use the negotiated v3 event stream.
func TestHelloUnaryThenV3PingEvents(t *testing.T) {
	var output bytes.Buffer
	writer := testResponseWriter(&output)
	server := newAgentServer(context.Background(), t.TempDir(), writer)
	defer server.close()
	hello := proto.CurrentHello()
	server.process(&proto.Request{ID: "hello", Op: proto.OpPing, Hello: &hello})
	server.process(&proto.Request{ID: "ping", Op: proto.OpPing, OperationID: "op_0000000000000991", ClientID: "client_0123456789abcdef"})
	responses := decodeResponses(t, &output)
	if len(responses) != 4 {
		t.Fatalf("responses: %+v", responses)
	}
	if r := responses[0]; !r.OK || r.Type != "" || r.Terminal || r.Ping == nil || r.Ping.NegotiatedVersion != 3 {
		t.Fatalf("invalid initial hello: %+v", r)
	}
	if r := responses[1]; r.ID != "ping" || r.Type != proto.EventAccepted || r.Seq != 1 {
		t.Fatalf("invalid accepted: %+v", r)
	}
	if r := responses[2]; r.ID != "ping" || r.Type != proto.EventProgress || r.Seq != 2 {
		t.Fatalf("invalid progress: %+v", r)
	}
	if r := responses[3]; r.ID != "ping" || r.Type != proto.EventFinal || !r.Terminal || r.Execution != proto.StateCompleted || r.Seq != 3 || r.Ping == nil {
		t.Fatalf("invalid final: %+v", r)
	}
}
