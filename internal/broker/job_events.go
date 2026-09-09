package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const maxJobEvents = 8192
const maxOwnerJobEvents = 1024
const maxEventsPerJob = 64
const maxJobEventBytes = 16 << 20

// JobEvent deliberately excludes argv, cwd, labels, environment and output.
// Stream and Sequence identify an observation history, not a remote execution.
type JobEvent struct {
	Host         string    `json:"host"`
	JobID        string    `json:"job_id"`
	Stream       string    `json:"stream"`
	Sequence     uint64    `json:"sequence"`
	At           time.Time `json:"at"`
	State        string    `json:"state"`
	PID          int       `json:"pid,omitempty"`
	ExitCode     int       `json:"exit_code,omitempty"`
	Operation    string    `json:"operation"`
	OperationRef string    `json:"operation_ref,omitempty"`
}

type JobEventCursor struct {
	Stream   string `json:"stream,omitempty"`
	Sequence uint64 `json:"sequence,omitempty"`
}

type JobEventPage struct {
	Events    []JobEvent     `json:"events"`
	Cursor    JobEventCursor `json:"cursor"`
	Truncated bool           `json:"truncated"`
	More      bool           `json:"more"`
}

type ownedJobEvent struct {
	Owner string `json:"owner"`
	JobEvent
}
type jobEventSnapshot struct {
	Schema int             `json:"schema"`
	Events []ownedJobEvent `json:"events"`
}

var ErrJobHistoryUnavailable = errors.New("job event durability uncertain; restart required")

type jobHistoryUncertain struct{ error }

// JobHistory serializes durable updates while allowing queries to read the
// last committed snapshot during a disk stall. Retention is explicit in cursors.
type JobHistory struct {
	updateMu sync.Mutex
	mu       sync.RWMutex
	events   []ownedJobEvent
	path     string
	failed   bool
	persist  func(string, []ownedJobEvent) error
}

func NewJobHistory() *JobHistory {
	return &JobHistory{events: make([]ownedJobEvent, 0), persist: saveJobEvents}
}

func validateJobEvent(e ownedJobEvent) error {
	if validateJobRef(JobRef{ID: e.JobID, Host: e.Host, Owner: e.Owner}) != nil || proto.ValidateOperationID(e.Stream) != nil || e.Sequence == 0 || e.Sequence > 1<<63-1 || e.At.IsZero() || e.PID < 0 {
		return errors.New("invalid job event identity")
	}
	switch e.State {
	case proto.JobRunning, proto.JobExited, proto.JobKilled, proto.JobUnknown, "removed":
	default:
		return errors.New("invalid job event state")
	}
	if e.OperationRef != "" && !validDigest(e.OperationRef) {
		return errors.New("invalid job event operation reference")
	}
	switch e.Operation {
	case proto.OpJobStart, proto.OpJobStatus, proto.OpJobList, proto.OpJobWait, proto.OpJobStop, proto.OpJobLogs, proto.OpJobRm:
	default:
		return errors.New("invalid job event operation")
	}
	return nil
}

func sameJobStream(a, b ownedJobEvent) bool {
	return a.Owner == b.Owner && a.Host == b.Host && a.JobID == b.JobID
}

func terminalJobEvent(state string) bool {
	return state == proto.JobExited || state == proto.JobKilled || state == "removed"
}

// Record publishes a batch atomically. Repeated observations, including N
// subscribers receiving one shared wait, do not create N copies of an event.
func (h *JobHistory) Record(observations []ownedJobEvent) ([]JobEvent, error) {
	if len(observations) == 0 {
		return nil, nil
	}
	h.updateMu.Lock()
	defer h.updateMu.Unlock()
	h.mu.RLock()
	if h.failed {
		h.mu.RUnlock()
		return nil, ErrJobHistoryUnavailable
	}
	next := append([]ownedJobEvent(nil), h.events...)
	path := h.path
	h.mu.RUnlock()
	var added []JobEvent
	for _, event := range observations {
		var previous *ownedJobEvent
		for i := len(next) - 1; i >= 0; i-- {
			if sameJobStream(next[i], event) {
				previous = &next[i]
				break
			}
		}
		if previous != nil {
			if previous.State == event.State && previous.PID == event.PID && previous.ExitCode == event.ExitCode {
				continue
			}
			// A late status reply must not reverse an observed terminal state.
			if previous.State == "removed" || terminalJobEvent(previous.State) && !terminalJobEvent(event.State) {
				continue
			}
			event.Stream, event.Sequence = previous.Stream, previous.Sequence+1
		} else {
			var err error
			event.Stream, err = proto.NewOperationID()
			if err != nil {
				return nil, err
			}
			event.Sequence = 1
		}
		event.At = time.Now().UTC()
		if err := validateJobEvent(event); err != nil {
			return nil, err
		}
		next = append(next, event)
		ownerCount, jobCount := 0, 0
		// Retain newest records within each budget. A single owner's hot job
		// cannot fill the global history with repeated observations.
		keep := make([]bool, len(next))
		for i := len(next) - 1; i >= 0; i-- {
			if next[i].Owner == event.Owner {
				ownerCount++
			}
			if sameJobStream(next[i], event) {
				jobCount++
			}
			keep[i] = !(next[i].Owner == event.Owner && ownerCount > maxOwnerJobEvents || sameJobStream(next[i], event) && jobCount > maxEventsPerJob)
		}
		retained := next[:0]
		for i, keepEvent := range keep {
			if keepEvent {
				retained = append(retained, next[i])
			}
		}
		next = retained
		if len(next) > maxJobEvents {
			next = next[len(next)-maxJobEvents:]
		}
		added = append(added, event.JobEvent)
	}
	if len(added) == 0 {
		return nil, nil
	}
	if path != "" {
		if err := h.persist(path, next); err != nil {
			var uncertain *jobHistoryUncertain
			if errors.As(err, &uncertain) {
				h.mu.Lock()
				h.failed = true
				h.mu.Unlock()
			}
			return nil, err
		}
	}
	h.mu.Lock()
	h.events = next
	h.mu.Unlock()
	return added, nil
}

func (h *JobHistory) Query(owner, host, job string, cursor JobEventCursor, limit int) (JobEventPage, error) {
	if validateJobRef(JobRef{ID: job, Host: host, Owner: owner}) != nil || limit < 0 || limit > maxEventsPerJob || cursor.Stream != "" && proto.ValidateOperationID(cursor.Stream) != nil || cursor.Stream == "" && cursor.Sequence != 0 {
		return JobEventPage{}, errors.New("invalid job event query")
	}
	if limit == 0 {
		limit = maxEventsPerJob
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.failed {
		return JobEventPage{}, ErrJobHistoryUnavailable
	}
	var selected []JobEvent
	for _, event := range h.events {
		if event.Owner == owner && event.Host == host && event.JobID == job {
			selected = append(selected, event.JobEvent)
		}
	}
	if len(selected) == 0 {
		return JobEventPage{}, errors.New("job history unknown or expired for principal")
	}
	first, last := selected[0], selected[len(selected)-1]
	page := JobEventPage{Events: make([]JobEvent, 0), Cursor: JobEventCursor{Stream: last.Stream, Sequence: cursor.Sequence}}
	if cursor.Stream != "" && cursor.Stream != last.Stream {
		page.Truncated = true
		cursor.Sequence = 0
		page.Cursor.Sequence = 0
	}
	if cursor.Sequence > last.Sequence {
		return JobEventPage{}, errors.New("job event cursor is ahead of history")
	}
	page.Truncated = page.Truncated || cursor.Sequence+1 < first.Sequence
	for _, event := range selected {
		if event.Sequence <= cursor.Sequence {
			continue
		}
		if len(page.Events) == limit {
			page.More = true
			break
		}
		page.Events = append(page.Events, event)
		page.Cursor.Sequence = event.Sequence
	}
	return page, nil
}

func saveJobEvents(path string, events []ownedJobEvent) error {
	data, err := json.Marshal(jobEventSnapshot{Schema: JobEventSchemaVersion, Events: events})
	if err != nil {
		return err
	}
	if len(data) > maxJobEventBytes {
		return errors.New("job history exceeds size bound")
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".rdev-job-events-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return &jobHistoryUncertain{err}
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return &jobHistoryUncertain{err}
	}
	return nil
}

func (h *JobHistory) ConfigurePersistence(path string) error {
	h.updateMu.Lock()
	defer h.updateMu.Unlock()
	events := make([]ownedJobEvent, 0)
	data, err := ReadPrivateFile(path, maxJobEventBytes)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		dec := json.NewDecoder(bytes.NewReader(data))
		if err := rejectDuplicateJSON(dec); err != nil {
			return err
		}
		if _, err := dec.Token(); err != io.EOF {
			return errors.New("job history requires one document")
		}
		var snapshot jobEventSnapshot
		dec = json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&snapshot); err != nil {
			return err
		}
		if snapshot.Schema != JobEventSchemaVersion || snapshot.Events == nil || len(snapshot.Events) > maxJobEvents {
			return errors.New("invalid job history schema or size")
		}
		owners := make(map[string]int)
		type key struct{ owner, host, job string }
		counts := make(map[key]int)
		latest := make(map[key]JobEvent)
		for _, event := range snapshot.Events {
			if err := validateJobEvent(event); err != nil {
				return err
			}
			k := key{event.Owner, event.Host, event.JobID}
			owners[event.Owner]++
			counts[k]++
			if owners[event.Owner] > maxOwnerJobEvents || counts[k] > maxEventsPerJob {
				return errors.New("job history retention bound exceeded")
			}
			if previous, ok := latest[k]; ok && (event.Stream != previous.Stream || event.Sequence != previous.Sequence+1) {
				return errors.New("job history cursor sequence is not contiguous")
			}
			latest[k] = event.JobEvent
		}
		events = snapshot.Events
	}
	if err := h.persist(path, events); err != nil {
		return err
	}
	h.mu.Lock()
	h.events, h.path, h.failed = events, path, false
	h.mu.Unlock()
	return nil
}
