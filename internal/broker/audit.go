package broker

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

type AuditEvent struct {
	At        time.Time `json:"at"`
	Owner     string    `json:"owner,omitempty"`
	Operation string    `json:"operation,omitempty"`
	Decision  string    `json:"decision,omitempty"`
	Result    string    `json:"result,omitempty"`
}
type AuditLog struct {
	mu       sync.RWMutex
	max      int
	events   []AuditEvent
	file     *os.File
	path     string
	maxBytes int64
}

func NewAuditLog(max int) *AuditLog {
	if max < 1 {
		max = 256
	}
	return &AuditLog{max: max}
}

// ConfigureFile enables append-only JSONL persistence with bounded rotation.
func (a *AuditLog) ConfigureFile(path string, maxBytes int64) error {
	if path == "" || maxBytes < 1 {
		return os.ErrInvalid
	}
	prior, _ := os.ReadFile(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	loaded := make([]AuditEvent, 0)
	for _, line := range bytes.Split(prior, []byte{'\n'}) {
		var event AuditEvent
		if len(line) > 0 && json.Unmarshal(line, &event) == nil {
			loaded = append(loaded, event)
		}
	}
	a.mu.Lock()
	if len(loaded) > a.max {
		loaded = loaded[len(loaded)-a.max:]
	}
	a.events = append(a.events, loaded...)
	if a.file != nil {
		_ = a.file.Close()
	}
	a.file, a.path, a.maxBytes = f, path, maxBytes
	a.mu.Unlock()
	return nil
}
func (a *AuditLog) Append(e AuditEvent) {
	e.Owner = auditField(e.Owner)
	e.Operation = auditField(e.Operation)
	e.Decision = auditField(e.Decision)
	e.Result = auditField(e.Result)
	e.At = e.At.UTC()
	a.mu.Lock()
	a.events = append(a.events, e)
	if len(a.events) > a.max {
		a.events = a.events[len(a.events)-a.max:]
	}
	if a.file != nil {
		if data, err := json.Marshal(e); err == nil {
			data = append(data, '\n')
			if st, err := a.file.Stat(); err == nil && st.Size()+int64(len(data)) > a.maxBytes {
				_ = a.file.Close()
				_ = os.Rename(a.path, a.path+".1")
				if f, openErr := os.OpenFile(a.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600); openErr == nil {
					a.file = f
				}
			}
			if a.file != nil {
				_, _ = a.file.Write(data)
				_ = a.file.Sync()
			}
		}
	}
	a.mu.Unlock()
}

func auditField(value string) string {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, value)
	if len(value) > 512 {
		return value[:512]
	}
	return value
}
func (a *AuditLog) Query(since time.Time) []AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]AuditEvent, 0)
	for _, e := range a.events {
		if e.At.After(since) {
			out = append(out, e)
		}
	}
	return out
}

func (a *AuditLog) QueryOwner(since time.Time, owner string) []AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]AuditEvent, 0)
	for _, event := range a.events {
		if event.Owner == owner && event.At.After(since) {
			out = append(out, event)
		}
	}
	return out
}
