package broker

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestWatchHubBoundsCacheAndSubscriptions(t *testing.T) {
	h := NewWatchHub()
	for i := 0; i < maxWatchKeys+1; i++ {
		h.Publish(fmt.Sprint(i), "complete")
	}
	if len(h.latest) != maxWatchKeys || len(h.order) != maxWatchKeys {
		t.Fatal("unbounded completion history")
	}
	old, stopOld := h.Subscribe("0")
	defer stopOld()
	select {
	case <-old:
		t.Fatal("old completion was not evicted")
	default:
	}
	h.Publish("oversize", strings.Repeat("x", maxWatchEventBytes+1))
	if _, ok := h.latest["oversize"]; ok {
		t.Fatal("oversize event retained")
	}
	stopOld()
	for range maxWatchSubscribers {
		_, cancel := h.Subscribe("bounded")
		defer cancel()
	}
	denied, cancel := h.Subscribe("excess")
	defer cancel()
	if _, ok := <-denied; ok {
		t.Fatal("subscriber cap did not reject excess watcher")
	}
}

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
