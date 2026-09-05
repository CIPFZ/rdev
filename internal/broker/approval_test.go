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
	if err := a.Validate("o", "rm", "h", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := a.Validate("o", "rm", "other", time.Now()); err == nil {
		t.Fatal("target swap accepted")
	}
	if err := a.Validate("o", "rm", "h", time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expired accepted")
	}
}
