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

func TestFairQueueWeightedOwners(t *testing.T) {
	q := NewFairQueue()
	for i := 0; i < 40; i++ {
		q.Enqueue("heavy", "h", 3)
		q.Enqueue("light", "l", 1)
	}
	heavy, light := 0, 0
	for i := 0; i < 40; i++ {
		value, ok := q.Next()
		if !ok {
			t.Fatal("queue ended early")
		}
		if value == "h" {
			heavy++
		} else {
			light++
		}
	}
	if heavy != 30 || light != 10 {
		t.Fatalf("weighted counts heavy=%d light=%d", heavy, light)
	}
}
