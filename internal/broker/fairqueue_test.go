package broker

import "testing"

func TestFairQueueRoundRobin(t *testing.T) {
	q := NewFairQueue()
	q.Enqueue("a", "a1", 1)
	q.Enqueue("a", "a2", 1)
	q.Enqueue("b", "b1", 1)
	if v, _ := q.Next(); v != "a1" {
		t.Fatal(v)
	}
	if v, _ := q.Next(); v != "b1" {
		t.Fatal(v)
	}
	if v, _ := q.Next(); v != "a2" {
		t.Fatal(v)
	}
}
