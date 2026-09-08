package secrets

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestRedactionMutationAndSnapshot(t *testing.T) {
	s := New()
	if got := s.Redact("old-credential"); got != "old-credential" {
		t.Fatal("empty store changed output")
	}
	key := HostKey("project", testHost, "key")
	if err := s.Set(key, "old-credential"); err != nil {
		t.Fatal(err)
	}
	if s.Redact("old-credential") != "<redacted:key>" {
		t.Fatal("new secret missing")
	}
	snapshot := s.Snapshot()
	if err := s.Set(key, "new-credential"); err != nil {
		t.Fatal(err)
	}
	if s.Redact("new-credential") != "<redacted:key>" || snapshot.Redact("old-credential") != "<redacted:key>" {
		t.Fatal("rotation lost live or in-flight protection")
	}
	if snapshot.Redact("new-credential") != "new-credential" {
		t.Fatal("snapshot changed after publication")
	}
	if !s.Delete(key) || s.Redact("new-credential") != "new-credential" {
		t.Fatal("deleted value retained in live store")
	}
	for _, declarative := range []bool{false, true} {
		values := map[Key]string{key: "batch-credential"}
		var err error
		if declarative {
			err = s.SetDeclarativeBatch(values)
		} else {
			err = s.SetBatch(values)
		}
		if err != nil {
			t.Fatal(err)
		}
		if s.Redact("batch-credential") != "<redacted:key>" {
			t.Fatal("batch not published")
		}
		if declarative {
			next := testHost
			next.Generation++
			if s.DeleteStaleHost("project", next) != 1 {
				t.Fatal("stale generation not deleted")
			}
		} else if s.DeleteHost(testHost.Alias) != 1 {
			t.Fatal("host not deleted")
		}
		if s.Redact("batch-credential") != "batch-credential" {
			t.Fatal("retired alias retained live redactor")
		}
	}
}

func TestRedactionCandidateFilterBoundaries(t *testing.T) {
	for _, value := range []string{"      ", strings.Repeat("\t", 16), " \t\n\r\v\f", "abc def ghijklmnop", "雪quoted-credential", "abcdefghijklmnop", "\x00\x00\x00\x00\x00\x00"} {
		s := New()
		if err := s.Set(testKey("key"), value); err != nil {
			t.Fatal(err)
		}
		if s.Redact(value) != "<redacted:key>" {
			t.Fatal("exact candidate was skipped")
		}
		if len(value) >= wrapTolerantMinLen {
			wrapped := strings.Join(strings.Split(value, ""), "\n")
			if s.Redact(wrapped) != "<redacted:key>" {
				t.Fatal("wrapped candidate was skipped")
			}
		}
	}
}

func TestRedactionConcurrentRotationKeepsSnapshots(t *testing.T) {
	s := New()
	key := testKey("key")
	if err := s.Set(key, "original-credential"); err != nil {
		t.Fatal(err)
	}
	s.Redact("original-credential")
	old := s.Snapshot()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.RedactValue(map[string]string{"result": "ordinary remote output\n"})
				if old.Redact("original-credential") != "<redacted:key>" {
					t.Error("in-flight snapshot lost protection")
					return
				}
			}
		}()
	}
	for j := 0; j < 100; j++ {
		value := fmt.Sprintf("rotated-credential-%03d", j)
		if err := s.Set(key, value); err != nil {
			t.Fatal(err)
		}
		if s.Redact(value) != "<redacted:key>" {
			t.Fatal("published rotation not protected")
		}
	}
	wg.Wait()
}

// Kept as a reproducible local cost measurement, not runtime QoS evidence.
func BenchmarkRedactRetainedArchive(b *testing.B) {
	s := New()
	for j := 0; j < 512; j++ {
		if err := s.Set(testKey(fmt.Sprintf("key-%d", j)), fmt.Sprintf("qos-credential-%03d-\"quoted\"-雪\nend", j)); err != nil {
			b.Fatal(err)
		}
	}
	fixture := strings.Repeat("phase5-qos-remote-io\n", 16384)
	s.Redact(fixture)
	b.SetBytes(int64(len(fixture)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if s.Redact(fixture) != fixture {
			b.Fatal("unrelated output changed")
		}
	}
}
