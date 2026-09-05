package broker

import "testing"

func TestLanesReserveControl(t *testing.T) {
	l := NewLanes(1, 1, 1)
	if !l.Acquire(LaneBulk) {
		t.Fatal("bulk")
	}
	if !l.Acquire(LaneControl) {
		t.Fatal("control blocked by bulk")
	}
	if l.Acquire(LaneControl) {
		t.Fatal("control limit ignored")
	}
	l.Release(LaneBulk)
	if l.Active(LaneBulk) != 0 {
		t.Fatal("bulk not released")
	}
}
