package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/storage"
)

func writeGCJob(t *testing.T, state, id string, started, ended time.Time, body string, running bool) string {
	t.Helper()
	root := filepath.Join(state, "jobs")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, id)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	meta := jobMeta{ID: id, PID: 0, StartedAt: started.UTC().Format(time.RFC3339Nano)}
	if running {
		meta.PID = os.Getpid()
		meta.ProcessIdentity, _ = processIdentity(meta.PID)
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), &meta); err != nil {
		t.Fatal(err)
	}
	if !running {
		if err := writeJSON(filepath.Join(dir, "status.json"), map[string]any{"exit_code": 0, "ended_at": ended.UTC().Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "stdout"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestStorageGCRetentionAndKeepLast(t *testing.T) {
	state := t.TempDir()
	now := time.Now().UTC()
	writeGCJob(t, state, "old", now.Add(-48*time.Hour), now.Add(-47*time.Hour), "old", false)
	writeGCJob(t, state, "middle", now.Add(-24*time.Hour), now.Add(-23*time.Hour), "middle", false)
	writeGCJob(t, state, "new", now.Add(-time.Hour), now.Add(-time.Minute), "new", false)
	scope := storage.Default().RemoteState
	scope.RetentionSec = 6 * 3600
	scope.KeepLastJobs = 1
	scope.MaxBytes = 1 << 30
	scope.MinFreeBytes = 0
	report, err := runStorageGC(state, scope, GCOptions{Now: now, MaxDeleteJobs: 10, MaxDeleteBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Removed) != 2 || report.Removed[0].ID != "old" || report.Removed[1].ID != "middle" {
		t.Fatalf("removed=%+v", report.Removed)
	}
	if _, err := os.Stat(filepath.Join(state, "jobs", "new")); err != nil {
		t.Fatalf("keep-last job removed: %v", err)
	}
}

func TestStorageGCProtectsRunningSymlinkAndUnknown(t *testing.T) {
	state := t.TempDir()
	now := time.Now().UTC()
	writeGCJob(t, state, "live", now.Add(-48*time.Hour), now.Add(-47*time.Hour), "live", true)
	unknown := writeGCJob(t, state, "unknown", now.Add(-48*time.Hour), now.Add(-47*time.Hour), "unknown", false)
	if err := os.WriteFile(filepath.Join(unknown, "user-data"), []byte("do not delete"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(state, "outside")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(state, "jobs", "link")); err != nil {
		t.Fatal(err)
	}
	scope := storage.Default().RemoteState
	scope.RetentionSec = int64(time.Hour / time.Second)
	scope.KeepLastJobs = 0
	scope.MinFreeBytes = 0
	report, err := runStorageGC(state, scope, GCOptions{Now: now, MaxDeleteJobs: 10, MaxDeleteBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Removed) != 0 {
		t.Fatalf("unsafe records removed: %+v", report.Removed)
	}
	for _, id := range []string{"live", "unknown", "link"} {
		if _, err := os.Lstat(filepath.Join(state, "jobs", id)); err != nil {
			t.Errorf("%s disappeared: %v", id, err)
		}
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("outside file changed: %v", err)
	}
}

func TestStorageGCDryRunQuotaAndBoundedBatch(t *testing.T) {
	state := t.TempDir()
	now := time.Now().UTC()
	for i, id := range []string{"a", "b", "c"} {
		writeGCJob(t, state, id, now.Add(time.Duration(-3+i)*time.Hour), now.Add(time.Duration(-3+i)*time.Hour), "1234567890", false)
	}
	scope := storage.Default().RemoteState
	scope.MaxBytes = 20
	scope.HighWatermark = 0.5
	scope.LowWatermark = 0.25
	scope.KeepLastJobs = 0
	scope.RetentionSec = 0
	scope.MinFreeBytes = 0
	dry, err := runStorageGC(state, scope, GCOptions{DryRun: true, Now: now, MaxScanJobs: 2, MaxDeleteJobs: 1, MaxDeleteBytes: 300})
	if err != nil {
		t.Fatal(err)
	}
	if !dry.DryRun || !dry.Pressure || !dry.ScanTruncated || len(dry.Candidates) != 1 {
		t.Fatalf("unexpected dry report: %+v", dry)
	}
	if _, err := os.Stat(filepath.Join(state, "jobs", dry.Candidates[0].ID)); err != nil {
		t.Fatalf("dry-run mutated state: %v", err)
	}
	actual, err := runStorageGC(state, scope, GCOptions{Now: now, MaxScanJobs: 10, MaxDeleteJobs: 1, MaxDeleteBytes: dry.Candidates[0].Bytes})
	if err != nil {
		t.Fatal(err)
	}
	if len(actual.Removed) != 1 || actual.FreedBytes != dry.Candidates[0].Bytes {
		t.Fatalf("bounded quota report: %+v", actual)
	}
}

func TestStorageGCScopeReportJSON(t *testing.T) {
	state := t.TempDir()
	now := time.Now().UTC()
	writeGCJob(t, state, "j", now.Add(-time.Hour), now.Add(-time.Hour), "x", false)
	scope := storage.Default().RemoteState
	scope.RetentionSec = 1
	report, err := planStorageGC(state, scope, GCOptions{DryRun: true, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(report); err != nil {
		t.Fatal(err)
	}
}
