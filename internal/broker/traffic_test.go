package broker

import (
	"context"
	"fmt"
	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
	"testing"
)

func TestTrafficHistoryEvictionCannotReassignLateFrames(t *testing.T) {
	s := NewScheduler(QoSConfig{}, 128)
	defer s.Close(context.Background())
	var old *observe.Traffic
	work := func(owner string) *observe.Traffic {
		t.Helper()
		var meter *observe.Traffic
		_, err := s.Do(context.Background(), "host", owner, LaneControl, func(ctx context.Context) (*proto.Response, error) {
			meter = observe.TrafficFromContext(ctx)
			meter.Sent.Add(11)
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return meter
	}
	old = work("old-owner")
	work("other-owner")
	old.Received.Add(23)
	if s.Snapshot("other-owner").Traffic[LaneControl].ReceivedBytes != 0 || s.Snapshot("old-owner").Traffic[LaneControl].ReceivedBytes != 23 {
		t.Fatal("late traffic crossed owners")
	}
	for i := 0; i < 1024; i++ {
		work(fmt.Sprintf("new-owner-%d", i))
	}
	current := work("old-owner")
	if old == current {
		t.Fatal("expired history retained old stream meter")
	}
	old.Received.Add(100)
	if s.Snapshot("old-owner").Traffic[LaneControl].ReceivedBytes != 0 {
		t.Fatal("old stream changed replacement history")
	}
	s.mu.Lock()
	count := len(s.stats)
	s.mu.Unlock()
	if count > 1024 {
		t.Fatal("traffic history exceeded bounded owner budget")
	}
}
