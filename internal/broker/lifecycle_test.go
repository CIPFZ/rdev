package broker

import (
	"context"
	"testing"
)

func TestRequestRegistryDisconnectCancelsOwnedOnly(t *testing.T) {
	r := NewRequestRegistry()
	a, _ := r.Add("a", context.Background())
	b, _ := r.Add("b", context.Background())
	r.Remove("a")
	select {
	case <-a.Done():
	default:
		t.Fatal("a not canceled")
	}
	select {
	case <-b.Done():
		t.Fatal("unowned request canceled")
	default:
	}
}
