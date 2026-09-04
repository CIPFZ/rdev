package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/storage"
)

func TestStorageOpsStatusGCAndDoctor(t *testing.T) {
	state := t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(state, "jobs"), 0o700); err != nil {
		t.Fatal(err)
	}
	p := storage.Default()
	p.RemoteState.KeepLastJobs = 0
	p.RemoteState.RetentionSec = 1
	if err := storage.Save(filepath.Join(state, "storage-policy.json"), p); err != nil {
		t.Fatal(err)
	}
	writeGCJob(t, state, "old", time.Now().Add(-8*24*time.Hour), time.Now().Add(-8*24*time.Hour), "hello", false)
	status, err := doStorage(proto.OpStorageStatus, &proto.StorageParams{Scope: "remote_state"}, state)
	if err != nil || status.Status == nil || status.Status.JobCount != 1 {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	dry, err := doStorage(proto.OpStorageGC, &proto.StorageParams{Scope: "remote_state", DryRun: true, MaxDeleteJobs: 1}, state)
	if err != nil || dry.GC == nil || !dry.GC.DryRun || len(dry.GC.Candidates) == 0 {
		t.Fatalf("gc=%+v err=%v", dry, err)
	}
	if _, err := os.Stat(filepath.Join(state, "jobs", "old")); err != nil {
		t.Fatalf("dry-run mutated state: %v", err)
	}
	doc, err := doStorage(proto.OpStorageDoctor, &proto.StorageParams{Scope: "remote_state"}, state)
	if err != nil || doc.Doctor == nil || !doc.Doctor.OK {
		t.Fatalf("doctor=%+v err=%v findings=%+v", doc, err, doc.Doctor.Findings)
	}
	if status.Status.Root != storageReportRoot || status.Status.PolicySource != storageReportPolicy {
		t.Fatalf("status leaked filesystem paths: %+v", status.Status)
	}
	if dry.GC.Root != storageReportRoot {
		t.Fatalf("gc leaked filesystem path: %+v", dry.GC)
	}
	if doc.Doctor.Root != storageReportRoot {
		t.Fatalf("doctor leaked filesystem path: %+v", doc.Doctor)
	}
}

func TestStorageDoctorIsNonMutatingAndFindsTombstone(t *testing.T) {
	state := t.TempDir()
	root := filepath.Join(state, "jobs")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	tomb := filepath.Join(root, ".rdev-gc-old-1")
	if err := os.WriteFile(tomb, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(tomb, old, old); err != nil {
		t.Fatal(err)
	}
	res, err := storageDoctor(&proto.StorageParams{}, state)
	if err != nil {
		t.Fatal(err)
	}
	if res == nil || len(res.Findings) == 0 {
		t.Fatalf("findings=%+v", res)
	}
	found := false
	for _, f := range res.Findings {
		if f.Code == "stale_gc_tombstone" {
			found = true
		}
	}
	if !found {
		t.Fatalf("stale tombstone not reported: %+v", res.Findings)
	}
	for _, f := range res.Findings {
		if filepath.IsAbs(f.Path) {
			t.Fatalf("doctor exposed absolute finding path: %+v", f)
		}
	}
	st, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o755 {
		t.Fatalf("doctor mutated permissions: %o", st.Mode().Perm())
	}
}

func TestStorageReadOnlyOperationsDoNotRepairModesOrCreateRoots(t *testing.T) {
	state := t.TempDir()
	root := filepath.Join(state, "jobs")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	dir := writeGCJob(t, state, "legacy", time.Now().Add(-time.Hour), time.Now().Add(-time.Hour), "x", false)
	meta := filepath.Join(dir, "meta.json")
	if err := os.Chmod(meta, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := storageStatus(&proto.StorageParams{}, state); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(meta)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o644 {
		t.Fatalf("status repaired metadata mode: %o", st.Mode().Perm())
	}
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := storageStatus(&proto.StorageParams{}, missing); err == nil {
		t.Fatal("status unexpectedly created a missing jobs root")
	}
}

func TestStorageDoctorDoesNotRepairExistingLockMode(t *testing.T) {
	state := t.TempDir()
	root := filepath.Join(state, "jobs")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	lockRoot := filepath.Join(state, lockDirName)
	if err := os.Mkdir(lockRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(lockRoot, "old.lock")
	if err := os.WriteFile(lock, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(lock, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := storageDoctor(&proto.StorageParams{}, state); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(lock)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o644 {
		t.Fatalf("doctor repaired lock mode: %o", st.Mode().Perm())
	}
}

func TestStorageGCHonorsConfiguredCleanupBudget(t *testing.T) {
	state := t.TempDir()
	p := storage.Default()
	p.Cleanup.MaxDeleteJobs = 1
	if err := storage.Save(filepath.Join(state, "storage-policy.json"), p); err != nil {
		t.Fatal(err)
	}
	if _, err := storageGC(&proto.StorageParams{MaxDeleteJobs: 2}, state); err == nil {
		t.Fatal("gc widened configured cleanup job budget")
	}
}

func TestStorageRejectsUnsafeScopeAndBounds(t *testing.T) {
	state := t.TempDir()
	if _, err := doStorage(proto.OpStorageStatus, &proto.StorageParams{Scope: "../../x"}, state); err == nil {
		t.Fatal("unsafe scope accepted")
	}
	if _, err := doStorage(proto.OpStorageGC, &proto.StorageParams{MaxDeleteBytes: storage.HardMaxBytes + 1}, state); err == nil {
		t.Fatal("oversized budget accepted")
	}
}

func TestStorageMetricsConcurrentUpdatesAndLedgerAccounting(t *testing.T) {
	state := t.TempDir()
	root := filepath.Join(state, "jobs")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "job")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	ledger := map[string]any{
		"stdout_ledger": map[string]any{"original_bytes": int64(10), "retained_bytes": int64(6), "dropped_bytes": int64(4)},
		"stderr_ledger": map[string]any{"original_bytes": int64(5), "retained_bytes": int64(5)},
	}
	b, _ := json.Marshal(ledger)
	if err := os.WriteFile(filepath.Join(dir, "ledger.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
	scope := storage.Default().RemoteState
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			updateStorageMetrics(filepath.Join(state, storageReportMetrics), state, "remote_state", scope, &GCReport{Scanned: 1}, time.Millisecond)
		}()
	}
	wg.Wait()
	loaded, err := storage.LoadMetrics(filepath.Join(state, storageReportMetrics))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.GC.Runs["success"] != 20 || loaded.GC.ScannedJobs != 20 {
		t.Fatalf("concurrent updates lost counters: %+v", loaded.GC)
	}
	if loaded.Logs.OriginalBytes != 15 || loaded.Logs.RetainedBytes != 11 || loaded.Logs.DroppedBytes != 4 {
		t.Fatalf("ledger accounting=%+v", loaded.Logs)
	}
}

func TestStorageMetricsLockRejectsSymlink(t *testing.T) {
	state := t.TempDir()
	victim := filepath.Join(state, "victim")
	if err := os.WriteFile(victim, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(state, ".storage-metrics.lock")); err != nil {
		t.Fatal(err)
	}
	called := false
	if tryStorageMetricsLock(state, func() { called = true }) {
		t.Fatal("acquired symlinked metrics lock")
	}
	if called {
		t.Fatal("ran callback while lock path was unsafe")
	}
	b, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "keep" {
		t.Fatalf("lock acquisition changed symlink target: %q", b)
	}
}
