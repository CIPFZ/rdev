package broker

import "testing"

func TestReadiness(t *testing.T) {
	var r Readiness
	if r.Ready() {
		t.Fatal("initially ready")
	}
	r.SetReady(true)
	if !r.Ready() {
		t.Fatal("not ready")
	}
}
