package framewriter

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type partialTrafficWriter struct{}

func (partialTrafficWriter) Write(p []byte) (int, error) { return min(3, len(p)), io.ErrClosedPipe }

func TestTrafficCountsPartialAndCanceledWrites(t *testing.T) {
	var count atomic.Uint64
	partial := New(partialTrafficWriter{}, nil, Config{}, nil)
	defer partial.Close()
	if err := partial.WriteCounted(context.Background(), []byte("abcdef"), Control, &count); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	<-partial.Done()
	if count.Load() != 3 {
		t.Fatalf("partial write charged %d", count.Load())
	}
	if err := partial.EnqueueCounted([]byte("denied"), Data, &count); err == nil {
		t.Fatal("closed writer accepted data")
	}
	if count.Load() != 3 {
		t.Fatal("rejected frame was charged")
	}

	out := newGatedRecorder()
	w := New(out, out.Close, Config{MaxBytes: 10, WriteTimeout: time.Second}, nil)
	defer w.Close()
	var late, dropped atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.WriteCounted(ctx, []byte("later"), Control, &late) }()
	<-out.entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if late.Load() != 0 {
		t.Fatal("blocked bytes counted before write")
	}
	if err := w.EnqueueCounted([]byte("too-large"), Data, &dropped); !errors.Is(err, ErrDropped) {
		t.Fatal(err)
	}
	close(out.release)
	deadline := time.Now().Add(time.Second)
	for late.Load() != 5 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if late.Load() != 5 || dropped.Load() != 0 {
		t.Fatal("canceled completion/rejected frame accounting drift")
	}
}
