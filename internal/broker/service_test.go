package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestServiceDispatchFairKeepsControlLaneResponsive(t *testing.T) {
	s := NewService(nil)
	bulkStarted := make(chan struct{})
	bulkDone := make(chan struct{})
	go func() {
		_, _ = s.DispatchFair(context.Background(), "bulk", LaneBulk, func() (*proto.Response, error) {
			close(bulkStarted)
			time.Sleep(150 * time.Millisecond)
			close(bulkDone)
			return &proto.Response{}, nil
		})
	}()
	select {
	case <-bulkStarted:
	case <-time.After(time.Second):
		t.Fatal("bulk dispatch did not start")
	}
	start := time.Now()
	if _, err := s.DispatchFair(context.Background(), "control", LaneControl, func() (*proto.Response, error) {
		return &proto.Response{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("control lane blocked by bulk work: %s", elapsed)
	}
	<-bulkDone
}

func TestServiceLeaseAndPolicy(t *testing.T) {
	s := NewService(nil)
	owner := Owner{ClientID: "c", ProjectID: "p"}
	if s.Decide(owner, "exec").Allow {
		t.Fatal("default allow")
	}
	if err := s.Grant(owner, "exec"); err != nil {
		t.Fatal(err)
	}
	if !s.Decide(owner, "exec").Allow {
		t.Fatal("grant missing")
	}
	s.AttachClient()
	s.DetachClient()
	if !s.Reapable(time.Now().Add(time.Minute)) {
		t.Fatal("lease not reapable")
	}
}

func TestServiceDispatchSharedCoalesces(t *testing.T) {
	s := NewService(nil)
	var mu sync.Mutex
	calls := 0
	fn := func() (*proto.Response, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		return &proto.Response{ID: "shared"}, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.DispatchShared(context.Background(), "job", fn); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("dispatch calls=%d", calls)
	}
}

func TestServiceReloadConfigAppliesOwnerWeights(t *testing.T) {
	s := NewService(nil)
	if err := s.ReloadConfig(Config{MaxHosts: 1, IdleTTL: time.Second, OwnerWeights: map[string]int{"owner": 3}}); err != nil {
		t.Fatal(err)
	}
	s.weightMu.RLock()
	weight := s.weights["owner"]
	s.weightMu.RUnlock()
	if weight != 3 {
		t.Fatalf("weight=%d", weight)
	}
}

func TestServiceRejectsAttachAfterClose(t *testing.T) {
	s := NewService(nil)
	if err := s.Close(nil); err != nil {
		t.Fatal(err)
	}
	if s.AttachClient() {
		t.Fatal("attach after close accepted")
	}
}
