package transport

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"testing"
	"time"
)

func TestTrafficKeepsCanceledStreamsWithOriginalOwner(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)
	c.protocolVersion = 3
	c.features = map[proto.Feature]bool{proto.FeatureCancel: true, proto.FeatureStreaming: true}
	a, b := &observe.Traffic{}, &observe.Traffic{}
	ctx, cancel := context.WithCancel(observe.WithTraffic(context.Background(), a))
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.Do(ctx, &proto.Request{Op: proto.OpExec, OperationID: "op_0123456789abcdef", ClientID: "client_0123456789abcdef", Exec: &proto.ExecParams{Argv: []string{"synthetic"}}})
		done <- err
	}()
	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	size := func(v any) uint64 {
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return uint64(len(data) + 1)
	}
	tx := size(req)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	var stop proto.Request
	if err := requests.Decode(&stop); err != nil {
		t.Fatal(err)
	}
	if stop.Op != proto.OpCancel || stop.ClientID != req.ClientID {
		t.Fatal("wrong automatic cancellation")
	}
	tx += size(stop)
	canceled := proto.NewError(proto.CodeCanceled, req.OperationID, proto.StateCanceled)
	frames := []*proto.Response{
		{ID: req.ID, OperationID: req.OperationID, Type: proto.EventAccepted, Seq: 1, OK: true, Execution: proto.StateAccepted},
		{ID: req.ID, OperationID: req.OperationID, Type: proto.EventData, Seq: 2, OK: true, Execution: proto.StateAccepted, Data: &proto.DataFrame{Stream: "stdout", Content: "eA==", ContentB64: true}},
		{ID: req.ID, OperationID: req.OperationID, Type: proto.EventError, Seq: 3, Terminal: true, Execution: proto.StateCanceled, Error: canceled, Err: canceled.Message},
		{ID: stop.ID, OperationID: stop.OperationID, Type: proto.EventAccepted, Seq: 1, OK: true, Execution: proto.StateAccepted},
		{ID: stop.ID, OperationID: stop.OperationID, Type: proto.EventFinal, Seq: 2, Terminal: true, OK: true, Execution: proto.StateCompleted},
	}
	var rx uint64
	for _, frame := range frames {
		rx += size(frame)
		sendReply(t, replies, frame)
	}
	healthy := make(chan error, 1)
	go func() {
		_, err := c.Do(observe.WithTraffic(context.Background(), b), &proto.Request{Op: proto.OpPing, ClientID: "client_bbbbbbbbbbbbbbbb", OperationID: "op_bbbbbbbbbbbbbbbb"})
		healthy <- err
	}()
	var ping proto.Request
	if err := requests.Decode(&ping); err != nil {
		t.Fatal(err)
	}
	accepted := &proto.Response{ID: ping.ID, OperationID: ping.OperationID, Type: proto.EventAccepted, Seq: 1, OK: true, Execution: proto.StateAccepted}
	final := &proto.Response{ID: ping.ID, OperationID: ping.OperationID, Type: proto.EventFinal, Seq: 2, Terminal: true, OK: true, Execution: proto.StateCompleted}
	sendReply(t, replies, accepted)
	sendReply(t, replies, final)
	if err := <-healthy; err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for a.Sent.Load() != tx && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := a.Snapshot(); got.SentBytes != tx || got.ReceivedBytes != rx {
		t.Fatalf("canceled stream meter=%+v want tx=%d rx=%d", got, tx, rx)
	}
	if got := b.Snapshot(); got.SentBytes != size(ping) || got.ReceivedBytes != size(accepted)+size(final) {
		t.Fatalf("healthy owner meter=%+v", got)
	}
}
