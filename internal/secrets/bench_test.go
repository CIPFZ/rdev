package secrets

import (
	"strings"
	"testing"
)

// Redact runs over every byte of command output, so its cost on ordinary
// (secret-free) text is what matters most.
func BenchmarkRedactCleanOutput(b *testing.B) {
	s := New()
	s.Set(OutputKey("tok"), "ghp_A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8")
	// 64 KB of typical build output.
	text := strings.Repeat("compiling package foo/bar/baz ... ok 0.12s\n", 1500)
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	for i := 0; i < b.N; i++ {
		s.Redact(text)
	}
}

// Worst case for the whitespace-tolerant scan: output full of the secret's first
// byte, so the cheap gate fails to reject.
func BenchmarkRedactAdversarialFirstByte(b *testing.B) {
	s := New()
	s.Set(OutputKey("tok"), "ghp_A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8")
	text := strings.Repeat("g", 64<<10)
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	for i := 0; i < b.N; i++ {
		s.Redact(text)
	}
}

func BenchmarkRedactNoSecrets(b *testing.B) {
	s := New()
	text := strings.Repeat("compiling package foo/bar/baz ... ok 0.12s\n", 1500)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Redact(text)
	}
}

// Single-line output skips the whitespace-tolerant scan entirely.
func BenchmarkRedactSingleLine(b *testing.B) {
	s := New()
	s.Set(OutputKey("tok"), "ghp_A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8")
	text := strings.Repeat("abcdefghij", 6500) // 65 KB, no whitespace
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(text)))
	for i := 0; i < b.N; i++ {
		s.Redact(text)
	}
}
