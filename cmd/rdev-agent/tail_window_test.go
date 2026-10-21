package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A complete log ending in newlines is not truncated: the trimmed trailing
// newlines are part of what the window represents.
func TestReadTailWindowCountsTrimmedTrailingNewlines(t *testing.T) {
	path := filepath.Join(privateTempDir(t), "log")
	for _, test := range []struct {
		body                     string
		n                        int
		logs                     string
		truncated                bool
		retained, dropped, total int64
	}{
		{"a\nb\n", 10, "a\nb", false, 4, 0, 4},
		{"a\nb\n\n\n", 10, "a\nb", false, 6, 0, 6},
		{"a\nb", 10, "a\nb", false, 3, 0, 3},
		{"first\nsecond\nlast\n", 1, "last", true, 5, 13, 18},
		{"", 5, "", false, 0, 0, 0},
	} {
		if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
			t.Fatal(err)
		}
		w, err := readTailWindow(path, test.n)
		if err != nil {
			t.Fatal(err)
		}
		tr := w.truncation()
		if w.logs != test.logs || tr.Truncated != test.truncated || tr.RetainedBytes != test.retained || tr.DroppedBytes != test.dropped || tr.OriginalBytes != test.total {
			t.Errorf("body %q n=%d: logs=%q truncation=%+v", test.body, test.n, w.logs, tr)
		}
	}
}
