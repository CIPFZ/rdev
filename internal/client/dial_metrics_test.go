package client

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestDialAdmissionCountsContendedWaitsWithoutInventingDials(t *testing.T) {
	for _, kind := range []string{"alias", "endpoint", "global"} {
		t.Run(kind, func(t *testing.T) {
			d := newDialControl(make(chan struct{}, maxConcurrentDials))
			clock := installDialClock(d)
			var held []*dialPermit
			defer func() {
				for _, p := range held {
					p.finish(false, nil)
				}
			}()
			host := transport.Host{Addr: "private-host.invalid"}
			lock := make(chan struct{}, 1)
			switch kind {
			case "alias":
				lock <- struct{}{}
			case "endpoint", "global":
				n := 1
				if kind == "global" {
					n = maxConcurrentDials
				}
				for i := 0; i < n; i++ {
					h := host
					if i > 0 {
						h.Addr = strings.Repeat("a", i) + ".invalid"
					}
					p, err := d.reserve(t.Context(), h, d.changes())
					if err != nil {
						t.Fatal(err)
					}
					held = append(held, p)
				}
				if kind == "global" {
					host.Addr = "seventh.invalid"
				}
			}
			started := make(chan bool, 1)
			d.testWaitStarted = func(backoff bool) { started <- backoff }
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if kind == "alias" {
					done <- d.waitSetupLock(ctx, lock)
					return
				}
				p, err := d.reserve(ctx, host, d.changes())
				if err == nil {
					p.finish(false, nil)
				}
				done <- err
			}()
			select {
			case backoff := <-started:
				if backoff {
					t.Fatal("queue reported backoff")
				}
			case <-time.After(time.Second):
				t.Fatal("queue barrier not reached")
			}
			waiting := d.snapshot()
			if waiting.Waiting != 1 || waiting.WaitingPeak != 1 || waiting.QueueWaits != 1 || waiting.StateLimit != 1024 || waiting.GlobalSlotLimit != 6 || waiting.GlobalSlotsInUse != len(held) {
				t.Fatalf("bad active queue snapshot: %+v", waiting)
			}
			clock.nanos.Add(int64(10 * time.Millisecond))
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			got := d.snapshot()
			if got.Waiting != 0 || got.WaitingPeak != 1 || got.QueueDurationNS != uint64(10*time.Millisecond) || got.QueueBuckets[1] != 1 || got.Connection.DialStarted != 0 || got.Connection.DialCanceled != 0 {
				t.Fatalf("canceled wait invented a dial or lost elapsed time: %+v", got)
			}
			for i, count := range got.QueueBuckets {
				if i != 1 && count != 0 {
					t.Fatal("histogram is not disjoint")
				}
			}
			raw, err := json.Marshal(got)
			if err != nil || strings.Contains(string(raw), "private-host") || strings.Contains(string(raw), "seventh.invalid") || strings.Contains(string(raw), "retry_attempts") {
				t.Fatal("snapshot exposed identities or unrelated business retry telemetry")
			}
		})
	}
}

func TestDialAdmissionBackoffDurationIncludesCanceledPartialWait(t *testing.T) {
	d := newDialControl(make(chan struct{}, maxConcurrentDials))
	clock := installDialClock(d)
	host := transport.Host{Addr: "private-host.invalid"}
	p, err := d.reserve(t.Context(), host, d.changes())
	if err != nil {
		t.Fatal(err)
	}
	p.finish(true, errors.New("unavailable"))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := d.reserve(ctx, host, d.changes()); done <- err }()
	awaitDialWait(t, clock)
	if got := d.snapshot(); got.Waiting != 1 || got.BackoffWaits != 1 || got.QueueWaits != 0 {
		t.Fatalf("backoff not separated from admission queue: %+v", got)
	}
	clock.nanos.Add(int64(37 * time.Millisecond))
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	got := d.snapshot()
	if got.Waiting != 0 || got.BackoffDurationNS != uint64(37*time.Millisecond) || got.Connection.DialStarted != 0 {
		t.Fatalf("partial backoff was not measured accurately: %+v", got)
	}
}

func TestDialAdmissionConcurrentPeakAndPartialCancellation(t *testing.T) {
	d := newDialControl(make(chan struct{}, maxConcurrentDials))
	var held []*dialPermit
	defer func() {
		for _, p := range held {
			p.finish(false, nil)
		}
	}()
	for i := 0; i < maxConcurrentDials; i++ {
		p, err := d.reserve(t.Context(), transport.Host{Addr: strings.Repeat("a", i+1) + ".invalid"}, d.changes())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, p)
	}
	started := make(chan bool, 3)
	d.testWaitStarted = func(backoff bool) { started <- backoff }
	var cancels []context.CancelFunc
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()
	var done []chan error
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		cancels = append(cancels, cancel)
		result := make(chan error, 1)
		done = append(done, result)
		h := transport.Host{Addr: strings.Repeat("b", i+1) + ".invalid"}
		go func() {
			p, err := d.reserve(ctx, h, d.changes())
			if err == nil {
				p.finish(false, nil)
			}
			result <- err
		}()
	}
	for range 3 {
		select {
		case backoff := <-started:
			if backoff {
				t.Fatal("unexpected backoff")
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent queue barrier not reached")
		}
	}
	if got := d.snapshot(); got.Waiting != 3 || got.WaitingPeak != 3 || got.QueueWaits != 3 || got.GlobalSlotsInUse != 6 {
		t.Fatalf("concurrent queue snapshot: %+v", got)
	}
	for i, cancel := range cancels {
		cancel()
		if err := <-done[i]; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if got := d.snapshot(); got.Waiting != 2-i || got.WaitingPeak != 3 || got.GlobalSlotsInUse != 6 {
			t.Fatalf("one cancellation affected other admission: %+v", got)
		}
	}
	got := d.snapshot()
	var completed uint64
	for _, count := range got.QueueBuckets {
		completed += count
	}
	if completed != 3 || got.Connection.DialCanceled != 0 {
		t.Fatal("queued cancellation invented a connection attempt")
	}
}

func TestBaseAndBulkCarryRealDialAggregateAlongsideOwner(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		name := "base"
		if bulk {
			name = "bulk"
		}
		t.Run(name, func(t *testing.T) {
			c := newTestClient()
			defer c.Close()
			host := transport.Host{Name: "dev", Addr: "private-host.invalid"}
			if err := c.Hosts.Add(host); err != nil {
				t.Fatal(err)
			}
			calls := 0
			c.dial = func(ctx context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
				calls++
				if bulk && calls == 1 {
					return &fakeRemoteConn{host: h}, nil
				}
				// Exercise real transport validation and its observer boundary,
				// without requiring or pretending to run an SSH connection.
				return transport.Dial(ctx, transport.Host{Addr: "-invalid"}, nil)
			}
			owner := &observe.ConnectionActivity{}
			ctx := observe.WithConnectionActivity(t.Context(), owner)
			var err error
			if bulk {
				_, _, _, err = c.leasedBulkConn(ctx, "dev", "")
			} else {
				_, err = c.conn(ctx, "dev")
			}
			if err == nil {
				t.Fatal("invalid actual dial succeeded")
			}
			got, principal := c.DialAdmission(), owner.Snapshot()
			if got.Connection.DialStarted != 1 || got.Connection.DialFailed != 1 || got.Connection.DialCanceled != 0 || got.Connection.DialInFlight != 0 || got.Connection.DialFailures["validation"] != 1 || principal.DialStarted != 1 || principal.DialFailed != 1 {
				t.Fatalf("owner/aggregate dial observations changed: %+v %+v", got, principal)
			}
		})
	}
}
