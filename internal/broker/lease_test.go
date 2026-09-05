package broker

import (
	"testing"
	"time"
)

func TestLeaseGraceAndInflight(t *testing.T) {
	l := NewLease(time.Second)
	l.Attach()
	l.Begin()
	l.Detach()
	if l.Reapable(time.Now().Add(2 * time.Second)) {
		t.Fatal("inflight lease reapable")
	}
	l.End()
	if !l.Reapable(time.Now().Add(2 * time.Second)) {
		t.Fatal("lease should be reapable")
	}
}
