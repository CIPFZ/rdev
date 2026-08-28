//go:build !windows

package framewriter

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// A real anonymous pipe has a finite kernel buffer. A frame larger than that
// enters a blocking write when nobody drains the read end. Closing the writer
// descriptor from the watchdog is not assumed to interrupt that in-flight
// syscall: the owner must be prepared to terminate its process, while this unit
// test releases the kernel write by closing the peer and verifies both fixed
// goroutines are then gone.
func TestRealPipeBlockedWriteHasBoundedWaitersAndFixedLoops(t *testing.T) {
	reader, output, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	w := New(output, output.Close, Config{
		MaxFrames: 2, MaxBytes: 4 << 20, WriteTimeout: 20 * time.Millisecond,
	}, nil)
	result := make(chan error, 1)
	go func() {
		result <- w.Write(context.Background(), make([]byte, 2<<20), Critical)
	}()
	select {
	case err := <-result:
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("blocked real-pipe write = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked real-pipe waiter exceeded watchdog budget")
	}
	select {
	case <-w.watchDone:
	case <-time.After(time.Second):
		t.Fatal("real-pipe watchdog goroutine leaked")
	}

	// On some Unix kernels output.Close above interrupts the active write; on
	// others it does not. Closing the peer is the deterministic test cleanup.
	_ = reader.Close()
	select {
	case <-w.workerDone:
	case <-time.After(time.Second):
		t.Fatal("real-pipe writer goroutine remained after peer teardown")
	}
}
