package broker

import (
	"testing"
	"time"
)

func TestApprovalBindsDigestAndExpiry(t *testing.T) {
	a, err := NewApproval("o", "rm", "h", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Validate(a.Token, "o", "rm", "h", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := a.Validate(a.Token, "o", "rm", "other", time.Now()); err == nil {
		t.Fatal("target swap accepted")
	}
	if err := a.Validate(a.Token, "o", "rm", "h", time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expired accepted")
	}
	store := NewApprovalStore()
	if err := store.Consume(a, a.Token, "o", "rm", "h", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Consume(a, a.Token, "o", "rm", "h", time.Now()); err == nil {
		t.Fatal("token replay accepted")
	}
}
