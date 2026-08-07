package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func benchLog(b *testing.B, lines int) string {
	b.Helper()
	state := b.TempDir()
	dir := filepath.Join(state, "jobs", "big")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "big", Argv: []string{"x"}, PID: 1})
	f, _ := os.Create(filepath.Join(dir, "stdout"))
	buf := make([]byte, 100)
	for i := range buf {
		buf[i] = 'x'
	}
	buf[99] = '\n'
	for i := 0; i < lines; i++ {
		f.Write(buf)
	}
	f.Close()
	return state
}

func BenchmarkJobLogsTail(b *testing.B) {
	state := benchLog(b, 500_000) // ~50 MB
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := jobLogs(&proto.JobParams{ID: "big", TailLines: 20}, state); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadTail(b *testing.B) {
	state := benchLog(b, 500_000)
	path := filepath.Join(state, "jobs", "big", "stdout")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := readTail(path, 20); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJobLogsGrep(b *testing.B) {
	state := benchLog(b, 500_000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := jobLogs(&proto.JobParams{ID: "big", Grep: "zzz-no-match", TailLines: 20}, state); err != nil {
			b.Fatal(err)
		}
	}
}
