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

func (o *Observer) scope(name string) *ScopeMetrics {
	if name == "local" {
		return &o.snap.Local
	}
	if name == "remote_state" {
		return &o.snap.RemoteState
	}
	return nil
}

// ObserveUsage records the latest usage and emits an event only on a pressure
// state transition. Unknown scope names are ignored to preserve cardinality.
func (o *Observer) ObserveUsage(scope string, used, free, budget int64, pressure bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	s := o.scope(scope)
	if s == nil {
		return
	}
	if s.Pressure != pressure {
		state := "cleared"
		if pressure {
			state = "entered"
		}
		e := PressureEvent{Scope: scope, State: state, At: o.now().Format(time.RFC3339Nano)}
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
	o.snap.QuotaHits++
	o.mu.Unlock()
}

func (o *Observer) Snapshot() MetricsSnapshot {
	if o == nil {
		return MetricsSnapshot{SchemaVersion: MetricsSchemaVersion}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
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
	defaults := newMetricsSnapshot()
	if out.GC.Runs == nil {
		out.GC.Runs = defaults.GC.Runs
	}
	if out.GC.FailureReasons == nil {
		out.GC.FailureReasons = defaults.GC.FailureReasons
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
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
