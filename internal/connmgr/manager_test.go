package connmgr

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeConn struct{ closed atomic.Int32 }

func (f *fakeConn) Close() error { f.closed.Add(1); return nil }

func TestAcquireSingleflightAndLeaseCounts(t *testing.T) {
	m := New(Config{})
	var dials atomic.Int32
	gate := make(chan struct{})
	var wg sync.WaitGroup
	leases := make(chan *Lease, 2)
	dial := func(context.Context) (Connection, error) { dials.Add(1); <-gate; return &fakeConn{}, nil }
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := m.Acquire(context.Background(), "k", dial)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			leases <- l
		}()
	}
	time.Sleep(10 * time.Millisecond)
	close(gate)
	wg.Wait()
	close(leases)
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials=%d, want 1", got)
	}
	s := m.Snapshot("k")
	if s.State != Warm || s.Lease != 2 {
		t.Fatalf("snapshot=%+v", s)
	}
	for l := range leases {
		l.Release()
	}
	if m.Snapshot("k").Lease != 0 {
		t.Fatal("lease leak")
	}
}

func TestEvictionNeverClosesBusyAndDrainsRace(t *testing.T) {
	m := New(Config{})
	c := &fakeConn{}
	l, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	if m.Evict("k", "idle") {
		t.Fatal("evicted leased connection")
	}
	if s := m.Snapshot("k"); s.State != Draining {
		t.Fatalf("state=%v", s.State)
	}
	// A new request cancels draining and safely shares the connection.
	l2, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { t.Fatal("redial"); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	l.Release()
	l2.Release()
	if c.closed.Load() != 0 {
		t.Fatal("busy connection closed")
	}
	if err := m.Begin("k"); err != nil {
		t.Fatal(err)
	}
	if err := m.End("k"); err != nil {
		t.Fatal(err)
	}
	if !m.Evict("k", "idle") {
		t.Fatal("idle eviction failed")
	}
	if c.closed.Load() != 1 {
		t.Fatalf("close count=%d", c.closed.Load())
	}
	if s := m.Snapshot("k"); s.State != Cold {
		t.Fatalf("state=%v", s.State)
	}
}

func TestQueueBoundedAndInflightAccounting(t *testing.T) {
	m := New(Config{MaxQueue: 1})
	_, err := m.Acquire(context.Background(), "k", func(context.Context) (Connection, error) { return &fakeConn{}, nil })
	if err != nil {
		t.Fatal(err)
	}
	release, err := m.Queue("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.Queue("k"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v", err)
	}
	if err := m.Begin("k"); err != nil {
		t.Fatal(err)
	}
	release()
	if s := m.Snapshot("k"); s.Queued != 0 || s.Inflight != 1 {
		t.Fatalf("snapshot=%+v", s)
	}
	_ = m.End("k")
}
