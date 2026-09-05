package broker

import (
	"context"
	"testing"
)

func TestQuotaPerClientAndHost(t *testing.T) {
	q := NewQuota(2, 1, 2)
	if err := q.Acquire(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if err := q.Acquire(context.Background(), "a"); err != ErrQueueFull {
		t.Fatalf("got %v", err)
	}
	if err := q.Acquire(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	if err := q.Acquire(context.Background(), "c"); err != ErrQueueFull {
		t.Fatalf("got %v", err)
	}
	q.Release("a")
	if err := q.Acquire(context.Background(), "c"); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaTracksHostShares(t *testing.T) {
	q := NewQuota(2, 2, 2)
	if err := q.AcquireHost(context.Background(), "h1", "a"); err != nil {
		t.Fatal(err)
	}
	if err := q.AcquireHost(context.Background(), "h1", "b"); err != nil {
		t.Fatal(err)
	}
	if err := q.AcquireHost(context.Background(), "h1", "c"); err != ErrQueueFull {
		t.Fatalf("got %v", err)
	}
	q.ReleaseHost("h1", "a")
	if err := q.AcquireHost(context.Background(), "h1", "c"); err != nil {
		t.Fatal(err)
	}
}
