package broker

import (
	"testing"
	"time"
)

func TestServiceLeaseAndPolicy(t *testing.T) {
	s := NewService(nil)
	owner := Owner{ClientID: "c", ProjectID: "p"}
	if s.Decide(owner, "exec").Allow {
		t.Fatal("default allow")
	}
	if err := s.Grant(owner, "exec"); err != nil {
		t.Fatal(err)
	}
	if !s.Decide(owner, "exec").Allow {
		t.Fatal("grant missing")
	}
	s.AttachClient()
	s.DetachClient()
	if !s.Reapable(time.Now().Add(time.Minute)) {
		t.Fatal("lease not reapable")
	}
}
