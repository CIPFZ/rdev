// Package storage contains the persisted storage policy and bounded log
// accounting used by detached jobs. Incremental GC lives at the agent boundary
// where process identity and the shared job lock are available.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	DefaultLocalMaxBytes  int64 = 1 << 30
	DefaultRemoteMaxBytes int64 = 5 << 30
	DefaultMinFreeBytes   int64 = 2 << 30
	DefaultPerJobBytes    int64 = 128 << 20
	HardMaxBytes          int64 = 64 << 30
	HardPerJobBytes       int64 = 2 << 30
	HardKeepLastJobs            = 100000
)

// ScopePolicy is a budget for one managed state root.
type ScopePolicy struct {
	MaxBytes      int64   `json:"max_bytes"`
	RetentionSec  int64   `json:"retention_sec"`
	Retention     string  `json:"retention,omitempty"`
	KeepLastJobs  int     `json:"keep_last_jobs"`
	HighWatermark float64 `json:"high_watermark"`
	LowWatermark  float64 `json:"low_watermark"`
	MinFreeBytes  int64   `json:"min_free_bytes"`
}

// PerJobPolicy controls each stdout/stderr stream.
type PerJobPolicy struct {
	MaxStdoutBytes int64  `json:"max_stdout_bytes"`
	MaxStderrBytes int64  `json:"max_stderr_bytes"`
	OnLogLimit     string `json:"on_log_limit"`
}

// CleanupPolicy bounds one incremental garbage-collection pass.  The agent
// deliberately keeps these controls in the persisted policy so every process
// sharing a state directory applies the same work budget.
type CleanupPolicy struct {
	IntervalSec    int64 `json:"interval_sec"`
	MaxDeleteBytes int64 `json:"max_delete_bytes"`
	MaxDeleteJobs  int   `json:"max_delete_jobs"`
	MaxScanJobs    int   `json:"max_scan_jobs"`
	DryRun         bool  `json:"dry_run"`
}

type Policy struct {
	Local       ScopePolicy   `json:"local"`
	RemoteState ScopePolicy   `json:"remote_state"`
	PerJob      PerJobPolicy  `json:"per_job"`
	Cleanup     CleanupPolicy `json:"cleanup"`
}

func Default() Policy {
	return Policy{
		Local:       ScopePolicy{MaxBytes: DefaultLocalMaxBytes, RetentionSec: 7 * 24 * 3600, Retention: "168h", KeepLastJobs: 100, HighWatermark: .85, LowWatermark: .70},
		RemoteState: ScopePolicy{MaxBytes: DefaultRemoteMaxBytes, RetentionSec: 7 * 24 * 3600, Retention: "168h", KeepLastJobs: 100, HighWatermark: .85, LowWatermark: .70, MinFreeBytes: DefaultMinFreeBytes},
		PerJob:      PerJobPolicy{MaxStdoutBytes: DefaultPerJobBytes, MaxStderrBytes: DefaultPerJobBytes, OnLogLimit: "truncate_oldest"},
		Cleanup:     CleanupPolicy{IntervalSec: 300, MaxDeleteBytes: 1 << 30, MaxDeleteJobs: 100, MaxScanJobs: 1000},
	}
}

func (p Policy) Validate() error {
	if err := validateScope("local", p.Local); err != nil {
		return err
	}
	if err := validateScope("remote_state", p.RemoteState); err != nil {
		return err
	}
	if p.PerJob.MaxStdoutBytes <= 0 || p.PerJob.MaxStdoutBytes > HardPerJobBytes || p.PerJob.MaxStderrBytes <= 0 || p.PerJob.MaxStderrBytes > HardPerJobBytes {
		return errors.New("per_job byte limits exceed hard cap")
	}
	if p.Cleanup.IntervalSec < 0 || p.Cleanup.MaxDeleteBytes < 0 || p.Cleanup.MaxDeleteBytes > HardMaxBytes || p.Cleanup.MaxDeleteJobs < 0 || p.Cleanup.MaxDeleteJobs > HardKeepLastJobs || p.Cleanup.MaxScanJobs < 0 || p.Cleanup.MaxScanJobs > HardKeepLastJobs {
		return errors.New("invalid cleanup limits")
	}
	switch p.PerJob.OnLogLimit {
	case "truncate_oldest", "discard_new", "stop_job":
	default:
		return fmt.Errorf("invalid on_log_limit %q", p.PerJob.OnLogLimit)
	}
	return nil
}

func validateScope(name string, s ScopePolicy) error {
	if s.MaxBytes <= 0 || s.MaxBytes > HardMaxBytes {
		return fmt.Errorf("%s max_bytes exceeds hard cap", name)
	}
	if s.RetentionSec < 0 || s.KeepLastJobs < 0 || s.KeepLastJobs > HardKeepLastJobs {
		return fmt.Errorf("invalid %s retention/keep_last_jobs", name)
	}
	if s.Retention != "" {
		if d, err := time.ParseDuration(s.Retention); err != nil || d < 0 {
			return fmt.Errorf("invalid %s retention", name)
		}
	}
	if math.IsNaN(s.HighWatermark) || math.IsInf(s.HighWatermark, 0) || math.IsNaN(s.LowWatermark) || math.IsInf(s.LowWatermark, 0) || s.HighWatermark <= 0 || s.HighWatermark > 1 || s.LowWatermark <= 0 || s.LowWatermark > s.HighWatermark {
		return fmt.Errorf("invalid %s watermarks", name)
	}
	if s.MinFreeBytes < 0 || s.MinFreeBytes > HardMaxBytes {
		return fmt.Errorf("invalid %s min_free_bytes", name)
	}
	return nil
}

// Resolve applies an optional project/host override without allowing it to
// widen the supplied global policy. Zero fields mean "inherit".
func Resolve(global, override Policy) (Policy, error) {
	r := global
	mergeScope := func(dst *ScopePolicy, src ScopePolicy) {
		if src.MaxBytes != 0 {
			dst.MaxBytes = src.MaxBytes
		}
		if src.RetentionSec != 0 {
			dst.RetentionSec = src.RetentionSec
		}
		if src.Retention != "" {
			dst.Retention = src.Retention
			if d, err := time.ParseDuration(src.Retention); err == nil {
				dst.RetentionSec = int64(d / time.Second)
			}
		}
		if src.KeepLastJobs != 0 {
			dst.KeepLastJobs = src.KeepLastJobs
		}
		if src.HighWatermark != 0 {
			dst.HighWatermark = src.HighWatermark
		}
		if src.LowWatermark != 0 {
			dst.LowWatermark = src.LowWatermark
		}
		if src.MinFreeBytes != 0 {
			dst.MinFreeBytes = src.MinFreeBytes
		}
	}
	mergeScope(&r.Local, override.Local)
	mergeScope(&r.RemoteState, override.RemoteState)
	if override.PerJob.MaxStdoutBytes != 0 {
		r.PerJob.MaxStdoutBytes = override.PerJob.MaxStdoutBytes
	}
	if override.PerJob.MaxStderrBytes != 0 {
		r.PerJob.MaxStderrBytes = override.PerJob.MaxStderrBytes
	}
	if override.PerJob.OnLogLimit != "" {
		r.PerJob.OnLogLimit = override.PerJob.OnLogLimit
	}
	if override.Cleanup.IntervalSec != 0 {
		r.Cleanup.IntervalSec = override.Cleanup.IntervalSec
	}
	if override.Cleanup.MaxDeleteBytes != 0 {
		r.Cleanup.MaxDeleteBytes = override.Cleanup.MaxDeleteBytes
	}
	if override.Cleanup.MaxDeleteJobs != 0 {
		r.Cleanup.MaxDeleteJobs = override.Cleanup.MaxDeleteJobs
	}
	if override.Cleanup.MaxScanJobs != 0 {
		r.Cleanup.MaxScanJobs = override.Cleanup.MaxScanJobs
	}
	if override.Cleanup.DryRun {
		r.Cleanup.DryRun = true
	}
	if err := r.Validate(); err != nil {
		return Policy{}, err
	}
	if r.Local.MaxBytes > global.Local.MaxBytes || r.RemoteState.MaxBytes > global.RemoteState.MaxBytes || r.PerJob.MaxStdoutBytes > global.PerJob.MaxStdoutBytes || r.PerJob.MaxStderrBytes > global.PerJob.MaxStderrBytes {
		return Policy{}, errors.New("override cannot widen global hard limit")
	}
	if r.Cleanup.MaxDeleteBytes > global.Cleanup.MaxDeleteBytes || r.Cleanup.MaxDeleteJobs > global.Cleanup.MaxDeleteJobs || r.Cleanup.MaxScanJobs > global.Cleanup.MaxScanJobs {
		return Policy{}, errors.New("override cannot widen global cleanup limit")
	}
	return r, nil
}

func Load(path string) (Policy, error) {
	p := Default()
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return p, nil
	}
	if err != nil {
		return Policy{}, err
	}
	if err := json.Unmarshal(b, &p); err != nil {
		return Policy{}, err
	}
	// Retention is the human-readable form exposed in configuration. Keep the
	// legacy integer field populated as the canonical value so callers do not
	// need to parse it themselves.
	for name, scope := range map[string]*ScopePolicy{"local": &p.Local, "remote_state": &p.RemoteState} {
		if scope.Retention == "" {
			continue
		}
		d, parseErr := time.ParseDuration(scope.Retention)
		if parseErr != nil || d < 0 {
			return Policy{}, fmt.Errorf("invalid %s retention", name)
		}
		scope.RetentionSec = int64(d / time.Second)
	}
	if err := p.Validate(); err != nil {
		return Policy{}, err
	}
	return p, nil
}

func Save(path string, p Policy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func NormalizeStrategy(s string) string { return strings.TrimSpace(strings.ToLower(s)) }
