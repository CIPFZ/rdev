package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

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

func TestServiceRejectsAttachAfterClose(t *testing.T) {
	s := NewService(nil)
	if err := s.Close(nil); err != nil {
		t.Fatal(err)
	}
	if s.AttachClient() {
		t.Fatal("attach after close accepted")
	}
}
