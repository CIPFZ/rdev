package broker

import "testing"

func TestWatchHubSharesOnePublication(t *testing.T) {
	h := NewWatchHub()
	a, stopA := h.Subscribe("j")
	defer stopA()
	b, stopB := h.Subscribe("j")
	defer stopB()
	h.Publish("j", "done")
	if <-a != "done" || <-b != "done" {
		t.Fatal("event not shared")
	}
	if h.Watching("j") != 2 {
		t.Fatal("watch count")
	}
}
