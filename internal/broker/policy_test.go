package broker

import (
	"path/filepath"
	"testing"
)

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

func TestPolicyPersistsGrantsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	a := NewPolicy()
	a.Grant("client/project", "exec")
	if err := a.Save(path); err != nil {
		t.Fatal(err)
	}
	b := NewPolicy()
	if err := b.Load(path); err != nil {
		t.Fatal(err)
	}
	if !b.Decide("client/project", "exec").Allow {
		t.Fatal("grant not restored")
	}
}

func TestPolicyCapabilityDecision(t *testing.T) {
	p := NewPolicy()
	p.GrantCapability("c", "operator", "exec")
	if !p.DecideCapability("c", "operator", "exec").Allow {
		t.Fatal("capability grant ignored")
	}
	if p.DecideCapability("c", "operator", "delete").Allow {
		t.Fatal("capability broadened operation")
	}
}
