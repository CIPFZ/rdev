package observe

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

type eventSink struct {
	mu     sync.Mutex
	events []Event
}

func (s *eventSink) Log(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func TestSecurityMetricsHaveFixedCardinalityAndSafeEvents(t *testing.T) {
	sink := &eventSink{}
	r := New(sink)
	for i := 0; i < 1000; i++ {
		r.Reject(ReasonProjectUntrusted, fmt.Sprintf("/secret/project/%d", i))
	}
	snapshot := r.Snapshot()
	if snapshot.SchemaVersion != SchemaVersion {
		t.Fatalf("schema = %d", snapshot.SchemaVersion)
	}
	if len(snapshot.SecurityRejects) != len(securityReasons) {
		t.Fatalf("metric series = %d, want fixed %d", len(snapshot.SecurityRejects), len(securityReasons))
	}
	if snapshot.SecurityRejects[string(ReasonProjectUntrusted)] != 1000 {
		t.Errorf("untrusted count = %d", snapshot.SecurityRejects[string(ReasonProjectUntrusted)])
	}
	for _, event := range sink.events {
		if strings.Contains(event.TargetHash, "/secret/project") || len(event.TargetHash) != 16 {
			t.Errorf("event leaked target or used unstable hash: %+v", event)
		}
		if event.Name != "security.config_rejected" || event.Level != "warn" {
			t.Errorf("unstable event vocabulary: %+v", event)
		}
	}
}

func TestUnknownMetricReasonIsIgnored(t *testing.T) {
	r := New(nil)
	r.Reject(SecurityReason("attacker-selected-label"), "target")
	for reason, count := range r.Snapshot().SecurityRejects {
		if count != 0 {
			t.Errorf("unexpected count %s=%d", reason, count)
		}
	}
}
