package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const auditQueueCapacity = 1024
const auditFlushInterval = 25 * time.Millisecond
const maxAuditSegment = 64 << 20

type AuditSinkStatus struct {
	Incomplete        bool   `json:"incomplete"`
	UncleanRecoveries uint64 `json:"unclean_recoveries"`
	Rotations         uint64 `json:"rotations"`
	State             string `json:"state"`
	Accepted          uint64 `json:"accepted"`
	Written           uint64 `json:"written"`
	Dropped           uint64 `json:"dropped"`
	Errors            uint64 `json:"errors"`
	Recovered         uint64 `json:"recovered"`
	Pending           int    `json:"pending"`
	LastError         string `json:"last_error,omitempty"`
}
type auditWrite struct {
	event AuditEvent
	ack   chan error
}
type auditSink struct {
	continuity                                      auditContinuity // immutable during writer lifetime
	requireIncomplete                               atomic.Bool
	rotations                                       atomic.Uint64
	pending                                         atomic.Int64
	path                                            string
	maxBytes, size                                  int64
	syncFile                                        func(*os.File) error
	file                                            *os.File // only the writer goroutine owns file/size after initialization
	queue                                           chan auditWrite
	done                                            chan struct{}
	accepted, written, dropped, failures, recovered atomic.Uint64
	lastError                                       atomic.Value
	closed                                          atomic.Bool
}

func (s *auditSink) status() AuditSinkStatus {
	st := AuditSinkStatus{Incomplete: s.continuity.Incomplete || s.requireIncomplete.Load() || s.dropped.Load() > 0 || s.failures.Load() > 0 || s.recovered.Load() > 0, UncleanRecoveries: s.continuity.UncleanRecoveries, Rotations: s.rotations.Load(), State: "ok", Accepted: s.accepted.Load(), Written: s.written.Load(), Dropped: s.dropped.Load(), Errors: s.failures.Load(), Recovered: s.recovered.Load(), Pending: int(s.pending.Load())}
	if code, ok := s.lastError.Load().(string); ok {
		st.LastError = code
	}
	if st.Dropped+st.Errors+st.Recovered > 0 {
		st.State = "degraded"
	}
	if s.closed.Load() {
		st.State = "closed"
	}
	return st
}
func (s *auditSink) err() error {
	if code, ok := s.lastError.Load().(string); ok && code != "" {
		return errors.New(code)
	}
	return nil
}
func (s *auditSink) failure(code string) {
	s.lastError.Store(code)
	s.failures.Add(1)
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
}

func readAuditSegment(path string, maxBytes int64) ([]byte, error) {
	data, err := ReadPrivateFile(path, maxBytes)
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}
func recoverAuditEvents(data []byte, limit int) ([]AuditEvent, uint64) {
	var events []AuditEvent
	var invalid uint64
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var event AuditEvent
		if len(line) > 2048 || json.Unmarshal(line, &event) != nil {
			invalid++
			continue
		}
		if len(events) == limit {
			copy(events, events[1:])
			events = events[:limit-1]
		}
		events = append(events, event)
	}
	return events, invalid
}

// open repairs only a torn trailing record in the active segment. Complete
// malformed lines remain on disk and count as recovery omissions. Symlinks,
// unsafe permissions, oversize files and I/O errors fail closed.
func (s *auditSink) open() ([]byte, error) {
	f, err := openPrivateAppend(s.path)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, s.maxBytes+1))
	if err != nil || int64(len(data)) > s.maxBytes {
		f.Close()
		if err == nil {
			err = errors.New("audit segment exceeds budget")
		}
		return nil, err
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		end := bytes.LastIndexByte(data, '\n') + 1
		if err := f.Truncate(int64(end)); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		data = data[:end]
		s.recovered.Add(1)
	}
	s.file, s.size = f, int64(len(data))
	return data, nil
}
func newAuditSink(path string, maxBytes int64, history int) (*auditSink, []AuditEvent, error) {
	if path == "" || maxBytes < 1024 || maxBytes > maxAuditSegment {
		return nil, nil, errors.New("audit segment budget must be between 1024 and 67108864 bytes")
	}
	continuity, known, err := readAuditContinuity(path + ".continuity")
	if err != nil {
		return nil, nil, err
	}
	if continuity.Active {
		if continuity.UncleanRecoveries == 1<<63-1 {
			return nil, nil, errors.New("audit continuity recovery counter exhausted")
		}
		continuity.UncleanRecoveries++
		continuity.Incomplete = true
	}
	s := &auditSink{path: path, maxBytes: maxBytes, syncFile: func(f *os.File) error { return f.Sync() }, queue: make(chan auditWrite, auditQueueCapacity), done: make(chan struct{})}
	rotated, err := readAuditSegment(path+".1", maxBytes)
	if err != nil {
		return nil, nil, err
	}
	prior, err := readAuditSegment(path, maxBytes)
	if err != nil {
		return nil, nil, err
	}
	// A predecessor daemon does not understand the continuity marker. Detect
	// segment changes after the last seal instead of trusting its clean flag.
	if known && !continuity.Active && *continuity.Seal != *sealAuditBytes(prior, rotated) {
		continuity.Incomplete = true
	}
	existing := len(prior) > 0 || len(rotated) > 0
	torn := len(prior) > 0 && prior[len(prior)-1] != '\n'
	if torn {
		prior = prior[:bytes.LastIndexByte(prior, '\n')+1]
	}
	events, invalid := recoverAuditEvents(rotated, history)
	next, invalidNext := recoverAuditEvents(prior, history)
	events = append(events, next...)
	if len(events) > history {
		events = events[len(events)-history:]
	}
	s.recovered.Add(invalid + invalidNext)
	if (!known && existing) || torn || s.recovered.Load() > 0 {
		continuity.Incomplete = true
	}
	continuity.Active = true
	continuity.Seal = nil
	// Publish uncertainty before repairing the audit tail. Even a subsequent
	// startup/storage failure must not erase the only evidence of that gap.
	if err := saveAuditContinuity(path+".continuity", continuity); err != nil {
		return nil, nil, err
	}
	if _, err := s.open(); err != nil {
		return nil, nil, err
	}
	s.continuity = continuity
	return s, events, nil
}
func (s *auditSink) rotate() error {
	if err := s.syncFile(s.file); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		s.file = nil
		return err
	}
	s.file = nil
	// Validate an existing segment before replacement, including symlink and
	// directory negatives. Rename errors leave the active segment intact.
	if _, err := readAuditSegment(s.path+".1", s.maxBytes); err != nil {
		return err
	}
	if err := os.Rename(s.path, s.path+".1"); err != nil {
		return err
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	s.file, s.size = f, 0
	s.rotations.Add(1)
	dir, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
func (s *auditSink) writeBatch(batch []AuditEvent) {
	defer s.pending.Add(-int64(len(batch)))
	if len(batch) == 0 {
		return
	}
	if s.file == nil {
		if _, err := s.open(); err != nil {
			s.failure("open_failed")
			s.dropped.Add(uint64(len(batch)))
			return
		}
	}
	for i, event := range batch {
		data, err := json.Marshal(event)
		if err != nil || len(data)+1 > int(s.maxBytes) || len(data) > 2048 {
			s.dropped.Add(1)
			continue
		}
		data = append(data, '\n')
		if s.size+int64(len(data)) > s.maxBytes {
			if err := s.rotate(); err != nil {
				s.failure("rotation_failed")
				s.dropped.Add(uint64(len(batch) - i))
				return
			}
		}
		n, err := s.file.Write(data)
		s.size += int64(n)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			s.failure("write_failed")
			s.dropped.Add(uint64(len(batch) - i))
			return
		}
		s.written.Add(1)
	}
	if err := s.syncFile(s.file); err != nil {
		s.failure("sync_failed")
	}
}
func (s *auditSink) run() {
	defer close(s.done)
	defer s.closed.Store(true)
	ticker := time.NewTicker(auditFlushInterval)
	defer ticker.Stop()
	batch := make([]AuditEvent, 0, 64)
	flush := func() { s.writeBatch(batch); clear(batch); batch = batch[:0] }
	for {
		select {
		case item, ok := <-s.queue:
			if !ok {
				flush()
				if s.file != nil {
					if err := s.file.Close(); err != nil {
						s.failure("close_failed")
					}
					s.file = nil
				}
				continuity := s.continuity
				seal, err := readAuditSeal(s.path, s.maxBytes)
				if err != nil {
					s.failure("continuity_seal_failed")
					return
				}
				continuity.Active = false
				continuity.Seal = seal
				continuity.Incomplete = s.status().Incomplete
				if err := saveAuditContinuity(s.path+".continuity", continuity); err != nil {
					s.failure("continuity_commit_failed")
				}
				return
			}
			if item.ack != nil {
				flush()
				item.ack <- s.err()
				continue
			}
			batch = append(batch, item.event)
			if len(batch) == cap(batch) {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}
