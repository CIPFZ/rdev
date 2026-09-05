package storage

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"
)

type Ledger struct {
	OriginalBytes    int64  `json:"original_bytes"`
	RetainedBytes    int64  `json:"retained_bytes"`
	DroppedBytes     int64  `json:"dropped_bytes"`
	Truncated        bool   `json:"truncated"`
	FirstTruncatedAt string `json:"first_truncated_at,omitempty"`
	Policy           string `json:"policy"`
	LimitBytes       int64  `json:"limit_bytes"`
}

// Sink continuously consumes a pipe while retaining at most limit bytes.
// truncate_oldest keeps the newest bytes; discard_new keeps the prefix.
type Sink struct {
	mu      sync.Mutex
	limit   int64
	policy  string
	buf     bytes.Buffer
	ledger  Ledger
	reached bool
}

func NewSink(limit int64, policy string) *Sink {
	if limit < 0 {
		limit = 0
	}
	if policy == "" {
		policy = "truncate_oldest"
	}
	return &Sink{limit: limit, policy: policy, ledger: Ledger{LimitBytes: limit, Policy: policy}}
}

func (s *Sink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ledger.OriginalBytes += int64(len(p))
	if s.limit == 0 {
		s.ledger.DroppedBytes += int64(len(p))
		s.markTruncated()
		s.reached = true
		return len(p), nil
	}
	if s.policy == "discard_new" || s.policy == "stop_job" {
		avail := s.limit - int64(s.buf.Len())
		if avail > 0 {
			n := int64(len(p))
			if n > avail {
				n = avail
			}
			s.buf.Write(p[:n])
			if n < int64(len(p)) {
				s.ledger.DroppedBytes += int64(len(p)) - n
				s.markTruncated()
				s.reached = true
			}
		} else {
			s.ledger.DroppedBytes += int64(len(p))
			s.markTruncated()
			s.reached = true
		}
		return len(p), nil
	}
	// truncate_oldest: retain only the suffix that fits. Do not append the
	// whole input first: io.Writer callers are allowed to pass very large
	// slices, and doing so would defeat the bounded-memory guarantee.
	if int64(len(p)) >= s.limit {
		drop := int64(s.buf.Len()) + int64(len(p)) - s.limit
		s.buf.Reset()
		s.buf.Write(p[len(p)-int(s.limit):])
		s.ledger.DroppedBytes += drop
		s.markTruncated()
		s.reached = true
		return len(p), nil
	}
	if int64(s.buf.Len())+int64(len(p)) > s.limit {
		drop := int64(s.buf.Len()) + int64(len(p)) - s.limit
		b := s.buf.Bytes()
		s.buf.Reset()
		s.buf.Write(b[drop:])
		s.buf.Write(p)
		s.ledger.DroppedBytes += drop
		s.markTruncated()
		s.reached = true
		return len(p), nil
	}
	s.buf.Write(p)
	return len(p), nil
}

func (s *Sink) markTruncated() {
	if !s.ledger.Truncated {
		s.ledger.Truncated = true
		s.ledger.FirstTruncatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
}
func (s *Sink) Ledger() Ledger {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.ledger
	l.RetainedBytes = int64(s.buf.Len())
	return l
}
func (s *Sink) LimitReached() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.reached }
func (s *Sink) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.buf.Bytes()...)
}
func (s *Sink) Drain(r io.Reader) error { _, err := io.Copy(s, r); return err }
func (s *Sink) Flush(path string) error {
	b := s.Bytes()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
func (l Ledger) MarshalJSON() ([]byte, error) { type alias Ledger; return json.Marshal(alias(l)) }
