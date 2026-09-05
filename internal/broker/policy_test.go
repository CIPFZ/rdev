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
