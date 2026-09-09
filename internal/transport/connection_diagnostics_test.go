package transport

import (
	"context"
	"testing"

	"github.com/CIPFZ/rdev/internal/observe"
)

func TestInvalidDialDiagnosticNeverCallsLookup(t *testing.T) {
	activity := &observe.ConnectionActivity{}
	ctx := observe.WithConnectionActivity(context.Background(), activity)
	_, err := Dial(ctx, Host{Addr: "-unsafe-destination"}, func(string, string) (*AgentBinary, error) { t.Fatal("invalid host reached lookup"); return nil, nil })
	if err == nil {
		t.Fatal("invalid host accepted")
	}
	got := activity.Snapshot()
	if got.DialStarted != 1 || got.DialSucceeded != 0 || got.DialFailures["validation"] != 1 || got.DialInFlight != 0 {
		t.Fatalf("invalid-destination diagnostic: %+v", got)
	}
	if got.DialDurationNS == 0 || got.MaxDialDurationNS > got.DialDurationNS {
		t.Fatal("dial duration missing or inconsistent")
	}
}

func TestCanceledActualDialHasSeparateAggregateOutcome(t *testing.T) {
	owner, aggregate := &observe.ConnectionActivity{}, &observe.ConnectionActivity{}
	ctx, cancel := context.WithCancel(observe.WithConnectionAggregate(observe.WithConnectionActivity(t.Context(), owner), aggregate))
	cancel()
	if _, err := Dial(ctx, Host{Addr: "-invalid"}, nil); err == nil {
		t.Fatal("invalid canceled dial succeeded")
	}
	for _, activity := range []*observe.ConnectionActivity{owner, aggregate} {
		got := activity.Snapshot()
		if got.DialStarted != 1 || got.DialCanceled != 1 || got.DialFailed != 0 || got.DialInFlight != 0 {
			t.Fatalf("canceled actual dial misclassified: %+v", got)
		}
	}
}
