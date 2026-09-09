package observe

import (
	"testing"
	"time"
)

func TestConnectionAggregatePreservesPrincipalAndDeduplicatesObserver(t *testing.T) {
	owner, aggregate := &ConnectionActivity{}, &ConnectionActivity{}
	ctx := WithConnectionAggregate(WithConnectionActivity(t.Context(), owner), aggregate)
	end := BeginConnectionDial(ctx)
	if owner.Snapshot().DialInFlight != 1 || aggregate.Snapshot().DialInFlight != 1 {
		t.Fatal("observer missed in-flight connection")
	}
	end(DialNegotiation, true, 2*time.Millisecond)
	end = BeginConnectionDial(ctx)
	end(DialCanceled, false, time.Millisecond)
	owner.Retry() // Existing scheduler retry observations remain owner-scoped.
	a, b := owner.Snapshot(), aggregate.Snapshot()
	if a.DialStarted != 2 || b.DialStarted != 2 || a.DialSucceeded != 1 || b.DialSucceeded != 1 || a.DialCanceled != 1 || b.DialCanceled != 1 || a.DialFailed != 0 || b.DialFailed != 0 || a.RetryAttempts != 1 || b.RetryAttempts != 0 {
		t.Fatalf("aggregate changed existing observation semantics: %+v %+v", a, b)
	}
	ctx = WithConnectionAggregate(WithConnectionActivity(t.Context(), owner), owner)
	end = BeginConnectionDial(ctx)
	end(DialProbe, false, time.Millisecond)
	if got := owner.Snapshot(); got.DialStarted != 3 || got.DialFailed != 1 || got.DialInFlight != 0 {
		t.Fatalf("same observer counted twice: %+v", got)
	}
}
