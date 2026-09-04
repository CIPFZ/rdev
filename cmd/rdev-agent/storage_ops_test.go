package main

import (
	"os"
	"path/filepath"
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
	st, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o755 {
		t.Fatalf("doctor mutated permissions: %o", st.Mode().Perm())
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
