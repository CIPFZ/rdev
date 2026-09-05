package broker

import "testing"

func BenchmarkFairQueueMultiOwner(b *testing.B) {
	q := NewFairQueue()
	for i := 0; i < 32; i++ {
		q.Enqueue("owner-a", i, 1)
		q.Enqueue("owner-b", i, 1)
		q.Enqueue("owner-c", i, 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Enqueue("owner-a", i, 1)
		_, _ = q.Next()
	}
}
