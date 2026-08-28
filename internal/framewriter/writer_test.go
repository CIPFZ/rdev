package framewriter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingCloser struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingCloser() *blockingCloser {
	return &blockingCloser{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingCloser) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return 0, io.ErrClosedPipe
}

func (w *blockingCloser) Close() error {
	select {
	case <-w.release:
	default:
		close(w.release)
	}
	return nil
}

func TestBlockedWriteTimeoutWakesWaitersAndStopsFixedLoops(t *testing.T) {
	out := newBlockingCloser()
	var failures atomic.Int32
	w := New(out, out.Close, Config{MaxFrames: 4, MaxBytes: 64, WriteTimeout: 10 * time.Millisecond}, func(error) {
		failures.Add(1)
	})
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- w.Write(context.Background(), []byte("first"), Data) }()
	<-out.entered
	go func() { second <- w.Write(context.Background(), []byte("terminal"), Critical) }()
	for name, result := range map[string]<-chan error{"first": first, "second": second} {
		select {
		case err := <-result:
			if !errors.Is(err, ErrWriteTimeout) && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("%s write error = %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s waiter remained blocked", name)
		}
	}
	select {
	case <-w.workerDone:
	case <-time.After(time.Second):
		t.Fatal("writer loop leaked after closable output teardown")
	}
	select {
	case <-w.watchDone:
	case <-time.After(time.Second):
		t.Fatal("watchdog loop leaked after timeout")
	}
	if failures.Load() != 1 {
		t.Fatalf("failure callbacks = %d, want 1", failures.Load())
	}
}

type gatedRecorder struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	writes  [][]byte
	once    sync.Once
	count   chan struct{}
}

func newGatedRecorder() *gatedRecorder {
	return &gatedRecorder{entered: make(chan struct{}), release: make(chan struct{}), count: make(chan struct{}, 8)}
}

func (w *gatedRecorder) Write(p []byte) (int, error) {
	w.once.Do(func() {
		close(w.entered)
		<-w.release
	})
	w.mu.Lock()
	w.writes = append(w.writes, append([]byte(nil), p...))
	w.mu.Unlock()
	w.count <- struct{}{}
	return len(p), nil
}

func (w *gatedRecorder) Close() error {
	select {
	case <-w.release:
	default:
		close(w.release)
	}
	return nil
}

func TestPriorityAndTotalBudgetAreBounded(t *testing.T) {
	out := newGatedRecorder()
	w := New(out, out.Close, Config{MaxFrames: 4, MaxBytes: 12, WriteTimeout: time.Second}, nil)
	defer w.Close()
	if err := w.Enqueue([]byte("AAAA"), Data); err != nil {
		t.Fatal(err)
	}
	<-out.entered
	if err := w.Enqueue([]byte("data"), Data); err != nil {
		t.Fatal(err)
	}
	if err := w.Enqueue([]byte("kill"), Critical); err != nil {
		t.Fatal(err)
	}
	if err := w.Enqueue([]byte("drop"), Data); !errors.Is(err, ErrDropped) {
		t.Fatalf("over-budget data = %v, want drop", err)
	}
	if w.DroppedBytes() != 4 {
		t.Fatalf("dropped bytes = %d", w.DroppedBytes())
	}
	close(out.release)
	for i := 0; i < 3; i++ {
		select {
		case <-out.count:
		case <-time.After(time.Second):
			t.Fatalf("received %d/3 writes", i)
		}
	}
	out.mu.Lock()
	joined := bytes.Join(out.writes, []byte("/"))
	out.mu.Unlock()
	if string(joined) != "AAAA/kill/data" {
		t.Fatalf("write order = %q", joined)
	}
}
