package storage

// Metrics contains bounded, low-cardinality storage telemetry.  The types in
// this file deliberately use fixed scope names and bounded pressure history;
// callers must never turn job ids, paths, request ids, or error strings into
// metric labels.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	MetricsSchemaVersion = 1
	MaxPressureEvents    = 32
	MaxMetricsBytes      = 1 << 20
)

type LogMetrics struct {
	OriginalBytes uint64 `json:"original_bytes"`
	RetainedBytes uint64 `json:"retained_bytes"`
	DroppedBytes  uint64 `json:"dropped_bytes"`
}

type GCMetrics struct {
	ScannedJobs    uint64            `json:"scanned_jobs"`
	RemovedJobs    uint64            `json:"removed_jobs"`
	FreedBytes     uint64            `json:"freed_bytes"`
	Errors         uint64            `json:"errors"`
	DurationMS     uint64            `json:"duration_ms"`
	Runs           map[string]uint64 `json:"runs"`
	FailureReasons map[string]uint64 `json:"failure_reasons"`
}

type ScopeMetrics struct {
	UsedBytes     int64  `json:"used_bytes"`
	FreeBytes     int64  `json:"free_bytes,omitempty"`
	BudgetBytes   int64  `json:"budget_bytes"`
	Pressure      bool   `json:"pressure"`
	PressureLevel string `json:"pressure_level"`
}

type PressureEvent struct {
	Scope string `json:"scope"`
	State string `json:"state"` // entered or cleared
	At    string `json:"at"`
}

type MetricsSnapshot struct {
	SchemaVersion  int             `json:"schema_version"`
	Local          ScopeMetrics    `json:"local"`
	RemoteState    ScopeMetrics    `json:"remote_state"`
	Logs           LogMetrics      `json:"logs"`
	GC             GCMetrics       `json:"gc"`
	QuotaHits      uint64          `json:"quota_hits_total"`
	PressureEvents []PressureEvent `json:"pressure_events,omitempty"`
}

// Observer is safe for concurrent writers and keeps a fixed-size event ring.
// A nil observer is intentionally supported by all methods for optional
// instrumentation on hot paths.
type Observer struct {
	mu     sync.Mutex
	snap   MetricsSnapshot
	events []PressureEvent
	now    func() time.Time
}

func NewObserver() *Observer {
	return &Observer{snap: newMetricsSnapshot(), now: func() time.Time { return time.Now().UTC() }}
}

func newMetricsSnapshot() MetricsSnapshot {
	return MetricsSnapshot{SchemaVersion: MetricsSchemaVersion, GC: GCMetrics{Runs: map[string]uint64{"success": 0, "error": 0, "dry_run": 0}, FailureReasons: map[string]uint64{"io": 0, "unsafe": 0, "budget": 0}}}
}

// NewMetricsSnapshot returns a schema-valid empty snapshot with every fixed
// counter vocabulary initialized. It is useful to callers that expose a
// metrics response before the first GC has been persisted.
func NewMetricsSnapshot() MetricsSnapshot { return newMetricsSnapshot() }

func (o *Observer) scope(name string) *ScopeMetrics {
	if name == "local" {
		return &o.snap.Local
	}
	if name == "remote_state" {
		return &o.snap.RemoteState
	}
	return nil
}

func (o *Observer) ensureLocked() {
	if o.snap.SchemaVersion == 0 {
		o.snap = newMetricsSnapshot()
	}
}

// ObserveUsage records the latest usage and emits an event only on a pressure
// state transition. Unknown scope names are ignored to preserve cardinality.
func (o *Observer) ObserveUsage(scope string, used, free, budget int64, pressure bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ensureLocked()
	s := o.scope(scope)
	if s == nil {
		return
	}
	if s.Pressure != pressure {
		state := "cleared"
		if pressure {
			state = "entered"
		}
		now := time.Now().UTC()
		if o.now != nil {
			now = o.now()
		}
		e := PressureEvent{Scope: scope, State: state, At: now.Format(time.RFC3339Nano)}
		if len(o.events) == MaxPressureEvents {
			copy(o.events, o.events[1:])
			o.events[len(o.events)-1] = e
		} else {
			o.events = append(o.events, e)
		}
	}
	s.UsedBytes, s.FreeBytes, s.BudgetBytes, s.Pressure = used, free, budget, pressure
	if pressure {
		s.PressureLevel = "high"
	} else {
		s.PressureLevel = "normal"
	}
}

func (o *Observer) ObserveLedger(l Ledger) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ensureLocked()
	add := func(dst *uint64, n int64) {
		if n > 0 {
			*dst += uint64(n)
		}
	}
	add(&o.snap.Logs.OriginalBytes, l.OriginalBytes)
	add(&o.snap.Logs.RetainedBytes, l.RetainedBytes)
	add(&o.snap.Logs.DroppedBytes, l.DroppedBytes)
}

func (o *Observer) ObserveGC(scanned, removed int, freed int64, errs int) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ensureLocked()
	if scanned > 0 {
		o.snap.GC.ScannedJobs += uint64(scanned)
	}
	if removed > 0 {
		o.snap.GC.RemovedJobs += uint64(removed)
	}
	if freed > 0 {
		o.snap.GC.FreedBytes += uint64(freed)
	}
	if errs > 0 {
		o.snap.GC.Errors += uint64(errs)
	}
}

// ObserveGCRun records only a fixed result vocabulary; arbitrary error text is
// intentionally discarded at this boundary.
func (o *Observer) ObserveGCRun(result string, duration time.Duration, failureReason string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ensureLocked()
	if o.snap.GC.Runs == nil {
		o.snap.GC.Runs = map[string]uint64{"success": 0, "error": 0, "dry_run": 0}
	}
	if result != "success" && result != "error" && result != "dry_run" {
		result = "error"
	}
	o.snap.GC.Runs[result]++
	if duration > 0 {
		o.snap.GC.DurationMS += uint64(duration / time.Millisecond)
	}
	if failureReason == "io" || failureReason == "unsafe" || failureReason == "budget" {
		if o.snap.GC.FailureReasons == nil {
			o.snap.GC.FailureReasons = map[string]uint64{"io": 0, "unsafe": 0, "budget": 0}
		}
		o.snap.GC.FailureReasons[failureReason]++
	}
}

func (o *Observer) ObserveQuotaHit() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.ensureLocked()
	o.snap.QuotaHits++
	o.mu.Unlock()
}

func (o *Observer) Snapshot() MetricsSnapshot {
	if o == nil {
		return MetricsSnapshot{SchemaVersion: MetricsSchemaVersion}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ensureLocked()
	out := o.snap
	out.GC.Runs = cloneCounters(o.snap.GC.Runs)
	out.GC.FailureReasons = cloneCounters(o.snap.GC.FailureReasons)
	out.PressureEvents = append([]PressureEvent(nil), o.events...)
	return out
}

func cloneCounters(in map[string]uint64) map[string]uint64 {
	if in == nil {
		return nil
	}
	out := make(map[string]uint64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// LoadMetrics reads an optional persisted summary. Invalid or absent files
// fail closed to an empty snapshot; the caller may choose to report the error.
func LoadMetrics(path string) (MetricsSnapshot, error) {
	zero := newMetricsSnapshot()
	st, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return zero, nil
	}
	if err != nil {
		return zero, err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() {
		return zero, errors.New("storage metrics is not a regular file")
	}
	if st.Size() > MaxMetricsBytes {
		return zero, errors.New("storage metrics exceeds size limit")
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return zero, nil
	}
	if err != nil {
		return zero, err
	}
	var out MetricsSnapshot
	if err := json.Unmarshal(b, &out); err != nil {
		return zero, err
	}
	if out.SchemaVersion == 0 {
		out.SchemaVersion = MetricsSchemaVersion
	}
	if out.SchemaVersion != MetricsSchemaVersion || len(out.PressureEvents) > MaxPressureEvents {
		return zero, errors.New("invalid storage metrics schema")
	}
	if err := normalizeMetrics(&out); err != nil {
		return zero, err
	}
	return out, nil
}

func SaveMetrics(path string, snapshot MetricsSnapshot) error {
	if snapshot.SchemaVersion == 0 {
		snapshot.SchemaVersion = MetricsSchemaVersion
	}
	if snapshot.SchemaVersion != MetricsSchemaVersion || len(snapshot.PressureEvents) > MaxPressureEvents {
		return errors.New("invalid storage metrics schema")
	}
	if err := normalizeMetrics(&snapshot); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	// Create a fresh, exclusive temporary file.  A fixed `path+.tmp` would
	// follow a pre-existing symlink and could overwrite an arbitrary file; it
	// would also let concurrent writers trample one another's temporary data.
	tmpFile, err := os.CreateTemp(filepath.Dir(path), ".storage-metrics-*.tmp")
	if err != nil {
		return err
	}
	tmp := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmp)
	}()
	if err := tmpFile.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmpFile.Write(b); err != nil {
		return err
	}
	// Flush the file before the atomic rename so a successful response does
	// not acknowledge telemetry that is still only in the page cache.
	if err := tmpFile.Sync(); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

// normalizeMetrics rejects persisted values that could turn fixed telemetry
// vocabularies into attacker-controlled labels. Missing fields remain
// backward-compatible and are populated with their zero-valued fixed keys.
func normalizeMetrics(s *MetricsSnapshot) error {
	defaults := newMetricsSnapshot()
	if s.GC.Runs == nil {
		s.GC.Runs = defaults.GC.Runs
	} else {
		for key := range s.GC.Runs {
			if key != "success" && key != "error" && key != "dry_run" {
				return errors.New("invalid storage metrics result label")
			}
		}
		for key := range defaults.GC.Runs {
			if _, ok := s.GC.Runs[key]; !ok {
				s.GC.Runs[key] = 0
			}
		}
	}
	if s.GC.FailureReasons == nil {
		s.GC.FailureReasons = defaults.GC.FailureReasons
	} else {
		for key := range s.GC.FailureReasons {
			if key != "io" && key != "unsafe" && key != "budget" {
				return errors.New("invalid storage metrics failure label")
			}
		}
		for key := range defaults.GC.FailureReasons {
			if _, ok := s.GC.FailureReasons[key]; !ok {
				s.GC.FailureReasons[key] = 0
			}
		}
	}
	for _, scope := range []*ScopeMetrics{&s.Local, &s.RemoteState} {
		if scope.UsedBytes < 0 || scope.BudgetBytes < 0 || scope.FreeBytes < -1 {
			return errors.New("invalid storage metrics scope values")
		}
		if scope.PressureLevel == "" {
			if scope.Pressure {
				scope.PressureLevel = "high"
			} else {
				scope.PressureLevel = "normal"
			}
		}
		if scope.PressureLevel != "normal" && scope.PressureLevel != "high" {
			return errors.New("invalid storage metrics pressure level")
		}
	}
	for _, event := range s.PressureEvents {
		if (event.Scope != "local" && event.Scope != "remote_state") || (event.State != "entered" && event.State != "cleared") {
			return errors.New("invalid storage metrics pressure event")
		}
		if _, err := time.Parse(time.RFC3339Nano, event.At); err != nil {
			return errors.New("invalid storage metrics pressure event timestamp")
		}
	}
	return nil
}
