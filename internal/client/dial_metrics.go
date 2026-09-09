package client

import (
	"context"
	"time"
)

var dialQueueUpperNS = [7]int64{int64(time.Millisecond), int64(10 * time.Millisecond), int64(100 * time.Millisecond), int64(time.Second), int64(10 * time.Second), int64(30 * time.Second), -1}

// DialAdmissionSnapshot is an administrative aggregate across this Client's
// endpoints and lanes, without principal/host/request labels. Wait counters
// count started contended alias, endpoint and global-slot episodes, including
// canceled waits; they are distinct from scheduler request latency. Durations
// and bucket counts include completed episodes only. Buckets are non-cumulative,
// upper bounds inclusive; the last -1 bound denotes +Inf. StateLimit bounds retained
// endpoint bookkeeping; the process slot limit is shared by all Client objects.
type DialAdmissionSnapshot struct {
	Waiting            int                 `json:"waiting"`
	WaitingPeak        int                 `json:"waiting_peak"`
	BackoffWaits       uint64              `json:"backoff_waits"`
	BackoffDurationNS  uint64              `json:"backoff_duration_ns"`
	QueueWaits         uint64              `json:"queue_waits"`
	QueueDurationNS    uint64              `json:"queue_duration_ns"`
	QueueBuckets       [7]uint64           `json:"queue_buckets"`
	QueueBucketUpperNS [7]int64            `json:"queue_bucket_upper_ns"`
	States             int                 `json:"states"`
	StateLimit         int                 `json:"state_limit"`
	GlobalSlotsInUse   int                 `json:"global_slots_in_use"`
	GlobalSlotLimit    int                 `json:"global_slot_limit"`
	Connection         DialOutcomeSnapshot `json:"connection"`
}

// DialOutcomeSnapshot projects the existing transport observer without its
// unrelated business retry counter. Failure stages are a fixed vocabulary.
type DialOutcomeSnapshot struct {
	DialStarted       uint64            `json:"dial_started"`
	DialSucceeded     uint64            `json:"dial_succeeded"`
	DialFailed        uint64            `json:"dial_failed"`
	DialCanceled      uint64            `json:"dial_canceled"`
	DialInFlight      int64             `json:"dial_inflight"`
	DialDurationNS    uint64            `json:"dial_duration_ns"`
	MaxDialDurationNS uint64            `json:"max_dial_duration_ns"`
	DialFailures      map[string]uint64 `json:"dial_failures"`
}

type dialWaitMetrics struct {
	waiting, peak                                int
	backoffWaits, backoffNS, queueWaits, queueNS uint64
	buckets                                      [7]uint64
}

func (d *dialControl) beginWait(backoff bool) func() {
	started := d.now()
	d.mu.Lock()
	d.metrics.waiting++
	d.metrics.peak = max(d.metrics.peak, d.metrics.waiting)
	if backoff {
		d.metrics.backoffWaits++
	} else {
		d.metrics.queueWaits++
	}
	d.mu.Unlock()
	if d.testWaitStarted != nil {
		d.testWaitStarted(backoff)
	}
	return func() {
		ns := uint64(max(0, d.now().Sub(started).Nanoseconds()))
		d.mu.Lock()
		defer d.mu.Unlock()
		d.metrics.waiting--
		if backoff {
			d.metrics.backoffNS += ns
			return
		}
		d.metrics.queueNS += ns
		for i, upper := range dialQueueUpperNS {
			if upper < 0 || ns <= uint64(upper) {
				d.metrics.buckets[i]++
				break
			}
		}
	}
}

// waitSetupLock measures contention only; warm uncontended lookups add neither
// a queue event nor a dial. Identity leases are managed by the caller.
func (d *dialControl) waitSetupLock(ctx context.Context, lock chan struct{}) error {
	select {
	case lock <- struct{}{}:
		return nil
	default:
	}
	finished := d.beginWait(false)
	defer finished()
	select {
	case lock <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DialAdmission is global administrative data, never an owner status view.
func (c *Client) DialAdmission() DialAdmissionSnapshot { return c.dialControl.snapshot() }

func (d *dialControl) snapshot() DialAdmissionSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	m := d.metrics
	activity := d.activity.Snapshot()
	return DialAdmissionSnapshot{
		Waiting: m.waiting, WaitingPeak: m.peak, BackoffWaits: m.backoffWaits, BackoffDurationNS: m.backoffNS,
		QueueWaits: m.queueWaits, QueueDurationNS: m.queueNS, QueueBuckets: m.buckets, QueueBucketUpperNS: dialQueueUpperNS,
		States: len(d.states), StateLimit: maxDialStates, GlobalSlotsInUse: len(d.slots), GlobalSlotLimit: maxConcurrentDials,
		Connection: DialOutcomeSnapshot{DialStarted: activity.DialStarted, DialSucceeded: activity.DialSucceeded,
			DialFailed: activity.DialFailed, DialCanceled: activity.DialCanceled, DialInFlight: activity.DialInFlight,
			DialDurationNS: activity.DialDurationNS, MaxDialDurationNS: activity.MaxDialDurationNS, DialFailures: activity.DialFailures},
	}
}
