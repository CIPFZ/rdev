package storage

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestSinkPoliciesAndAccounting(t *testing.T) {
	tests := []struct {
		name, policy, want string
	}{
		{"oldest", "truncate_oldest", "efgh"},
		{"new", "discard_new", "abcd"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSink(4, tt.policy)
			if n, err := s.Write([]byte("abcdefgh")); err != nil || n != 8 {
				t.Fatalf("Write = %d, %v", n, err)
			}
			if got := string(s.Bytes()); got != tt.want {
				t.Fatalf("bytes = %q, want %q", got, tt.want)
			}
			l := s.Ledger()
			if l.OriginalBytes != 8 || l.RetainedBytes != 4 || l.DroppedBytes != 4 || !l.Truncated || !s.LimitReached() {
				t.Fatalf("bad ledger: %+v", l)
			}
		})
	}
}

func TestSinkStopJobAndLargeWriteRemainBounded(t *testing.T) {
	s := NewSink(3, "stop_job")
	if _, err := s.Write(bytes.Repeat([]byte{'x'}, 1<<20)); err != nil {
		t.Fatal(err)
	}
	if got := len(s.Bytes()); got != 3 || !s.LimitReached() {
		t.Fatalf("retained=%d reached=%v", got, s.LimitReached())
	}
	if got := s.Ledger().DroppedBytes; got != (1<<20)-3 {
		t.Fatalf("dropped=%d", got)
	}
}

func TestPolicyLoadRetentionAndRejectsNonFiniteWatermark(t *testing.T) {
	d := t.TempDir()
	p := Default()
	p.Local.Retention = "2h"
	p.Local.RetentionSec = 0
	path := filepath.Join(d, "policy.json")
	if err := Save(path, p); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil || loaded.Local.RetentionSec != 7200 {
		t.Fatalf("load retention=%+v err=%v", loaded.Local, err)
	}
	var raw map[string]any
	b, _ := os.ReadFile(path)
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	bad := Default()
	bad.Local.HighWatermark = math.NaN()
	if err := bad.Validate(); err == nil {
		t.Fatal("NaN watermark accepted")
	}
}
