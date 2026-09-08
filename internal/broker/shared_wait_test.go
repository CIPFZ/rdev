package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestSharedWaitDisconnectReleasesSubscriberButPreservesObservationLease(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	fn := func(ctx context.Context) (*proto.Response, error) {
		calls.Add(1)
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return &proto.Response{OK: true, OperationID: "one-observation"}, nil
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := s.DispatchShared(ctx, "a", "job", fn); done <- err }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnected initiator retained handler")
	}
	if got := s.SharedWaitStatus("a"); got.Observers != 1 || got.Subscribers != 0 {
		t.Fatal(got)
	}
	if s.Reapable(time.Now().Add(time.Hour)) {
		t.Fatal("detached observation released transport lease")
	}
	if got := s.SharedWaitStatus("b"); got != (SharedWaitStatus{}) {
		t.Fatal("other owner saw observation")
	}
	joined := make(chan *proto.Response, 1)
	go func() {
		r, err := s.DispatchShared(t.Context(), "a", "job", fn)
		if err != nil {
			t.Error(err)
		}
		joined <- r
	}()
	deadline := time.Now().Add(time.Second)
	for s.SharedWaitStatus("a").Subscribers != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.SharedWaitStatus("a").Subscribers != 1 {
		t.Fatal("reconnected subscriber missing")
	}
	close(release)
	if r := <-joined; r == nil || r.OperationID != "one-observation" || calls.Load() != 1 {
		t.Fatal("reconnect duplicated observation")
	}
}

func TestSharedWaitOwnerBoundaryAndShutdownCancelObservers(t *testing.T) {
	s := NewService(nil)
	entered := make(chan struct{}, 2)
	done := make(chan error, 2)
	for _, owner := range []string{"a", "b"} {
		go func() {
			_, err := s.DispatchShared(t.Context(), owner, "same-key", func(ctx context.Context) (*proto.Response, error) {
				entered <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			})
			done <- err
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("different owners shared observation")
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if s.SharedWaitStatus("a") != (SharedWaitStatus{}) || s.SharedWaitStatus("b") != (SharedWaitStatus{}) {
		t.Fatal("shutdown stranded observer")
	}
}

func TestSharedWaitAdmissionBoundIncludesDisconnectedObservers(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	if err := s.ReloadConfig(Config{MaxHosts: 1, IdleTTL: time.Second, QoS: QoSConfig{MaxActive: 4, PerHost: 4, PerOwner: 2, MaxQueued: 8, PerOwnerQueued: 2}}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "c", "d"} {
		ctx, cancel := context.WithCancel(t.Context())
		entered := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := s.DispatchShared(ctx, "owner", key, func(ctx context.Context) (*proto.Response, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			})
			done <- err
		}()
		<-entered
		cancel()
		<-done
	}
	if _, err := s.DispatchShared(t.Context(), "owner", "overflow", func(context.Context) (*proto.Response, error) {
		t.Fatal("excess observation dispatched")
		return nil, nil
	}); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.DispatchShared(ctx, "other", "cancelled", nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if got := s.SharedWaitStatus("owner"); got.Observers != 4 || got.Subscribers != 0 {
		t.Fatal(got)
	}
}
