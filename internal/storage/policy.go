// Package storage contains the persisted storage policy and bounded log
// accounting used by detached jobs.  It deliberately does not implement GC.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
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

type Policy struct {
	Local       ScopePolicy  `json:"local"`
	RemoteState ScopePolicy  `json:"remote_state"`
	PerJob      PerJobPolicy `json:"per_job"`
}

func Default() Policy {
	return Policy{
		Local:       ScopePolicy{MaxBytes: DefaultLocalMaxBytes, RetentionSec: 7 * 24 * 3600, Retention: "168h", KeepLastJobs: 100, HighWatermark: .85, LowWatermark: .70},
		RemoteState: ScopePolicy{MaxBytes: DefaultRemoteMaxBytes, RetentionSec: 7 * 24 * 3600, Retention: "168h", KeepLastJobs: 100, HighWatermark: .85, LowWatermark: .70, MinFreeBytes: DefaultMinFreeBytes},
		PerJob:      PerJobPolicy{MaxStdoutBytes: DefaultPerJobBytes, MaxStderrBytes: DefaultPerJobBytes, OnLogLimit: "truncate_oldest"},
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
	if s.HighWatermark <= 0 || s.HighWatermark > 1 || s.LowWatermark <= 0 || s.LowWatermark > s.HighWatermark {
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
	if err := r.Validate(); err != nil {
		return Policy{}, err
	}
	if r.Local.MaxBytes > global.Local.MaxBytes || r.RemoteState.MaxBytes > global.RemoteState.MaxBytes || r.PerJob.MaxStdoutBytes > global.PerJob.MaxStdoutBytes || r.PerJob.MaxStderrBytes > global.PerJob.MaxStderrBytes {
		return Policy{}, errors.New("override cannot widen global hard limit")
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
