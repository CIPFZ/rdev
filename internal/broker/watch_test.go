package broker

import (
	"testing"
	"time"
)

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

func TestWatchHubReplaysLatestEventToReconnect(t *testing.T) {
	h := NewWatchHub()
	h.Publish("job", "terminal")
	ch, cancel := h.Subscribe("job")
	defer cancel()
	select {
	case got := <-ch:
		if got != "terminal" {
			t.Fatalf("event=%v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("latest event was not replayed")
	}
}
