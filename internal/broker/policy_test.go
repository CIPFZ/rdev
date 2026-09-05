package broker

import "testing"

func TestPolicyDefaultDeny(t *testing.T) {
	p := NewPolicy()
	if p.Decide("c", "exec").Allow {
		t.Fatal("default must deny")
	}
	p.Grant("c", "exec")
	if !p.Decide("c", "exec").Allow {
		t.Fatal("grant ignored")
	}
}
