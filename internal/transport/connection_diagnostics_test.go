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
