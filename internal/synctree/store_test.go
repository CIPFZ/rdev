package synctree

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSyncStoreBinaryRoundTripAndRestart(t *testing.T) {
	if os.Getenv("RDEV_SYNC_STORE_HELPER") == "1" {
		store, err := NewStore(os.Getenv("RDEV_SYNC_STORE_ROOT"))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(os.Getenv("RDEV_SYNC_STORE_PLAN"))
		if err != nil {
			t.Fatal(err)
		}
		var p Plan
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Fatal(err)
		}
		out, err := store.Execute(t.Context(), "owner", os.Getenv("RDEV_SYNC_STAGE_ID"), os.Getenv("RDEV_SYNC_STORE_DEST"), "stable-operation", p)
		if !errors.Is(err, ErrRecorded) || out.State != "completed" {
			t.Fatalf("process replay accepted: %+v %v", out, err)
		}
		return
	}
	source := t.TempDir()
	name := "binary-\xff\nname"
	data := bytes.Repeat([]byte{0, 255, 1, 13, 10}, ChunkBytes/3)
	if err := os.WriteFile(filepath.Join(source, name), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(name, filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	a, err := NewStore(privateTemp(t))
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewStore(privateTemp(t))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewID()
	stage, err := a.Capture(t.Context(), "owner", id, source, "preserve")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(stage.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	var wire Manifest
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Begin(t.Context(), "owner", id, wire); err != nil {
		t.Fatal(err)
	}
	for i, e := range wire.Entries {
		if e.Kind != "file" {
			continue
		}
		for off := int64(0); off < e.Size; {
			chunk, err := a.Read(t.Context(), "owner", id, i, off)
			if err != nil {
				t.Fatal(err)
			}
			if err := b.Put(t.Context(), "owner", id, i, off, chunk); err != nil {
				t.Fatal(err)
			}
			off += int64(len(chunk))
		}
	}
	if _, err := b.Seal(t.Context(), "owner", id); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(t.Context(), "other", id); err == nil {
		t.Fatal("cross-owner stage exposed")
	}
	dest := filepath.Join(t.TempDir(), "destination")
	snap, err := Inspect(t.Context(), dest, StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Build(wire, snap, true, "overwrite")
	if err != nil {
		t.Fatal(err)
	}
	out, err := b.Execute(t.Context(), "owner", id, dest, "stable-operation", plan)
	if err != nil || out.State != "completed" {
		t.Fatal(out, err)
	}
	got, err := os.ReadFile(filepath.Join(dest, name))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("binary content drift", err)
	}
	gotManifest, err := Scan(t.Context(), dest, "preserve", StageLimits)
	if err != nil || gotManifest.Digest != wire.Digest {
		t.Fatal("metadata/link drift", err)
	}
	if _, err := b.Outcome(t.Context(), "other", "stable-operation"); err == nil {
		t.Fatal("cross-owner outcome exposed")
	}
	// A distinct process with only the on-disk ledger must refuse to apply again.
	planPath := filepath.Join(t.TempDir(), "plan.json")
	raw, _ = json.Marshal(plan)
	if err := os.WriteFile(planPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSyncStoreBinaryRoundTripAndRestart$")
	cmd.Env = append(os.Environ(), "RDEV_SYNC_STORE_HELPER=1", "RDEV_SYNC_STORE_ROOT="+b.Root, "RDEV_SYNC_STORE_PLAN="+planPath, "RDEV_SYNC_STAGE_ID="+id, "RDEV_SYNC_STORE_DEST="+dest)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("restart: %v %s", err, output)
	}
	if err := b.Remove(t.Context(), "owner", id); err != nil {
		t.Fatal(err)
	}
	if out, err := b.Outcome(t.Context(), "owner", "stable-operation"); err != nil || out.State != "completed" {
		t.Fatal("cleanup removed durable outcome", err)
	}
}

func TestSyncStoreRejectsCorruptionAndCancelsLock(t *testing.T) {
	source := t.TempDir()
	write(t, filepath.Join(source, "file"), "approved")
	store, err := NewStore(privateTemp(t))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewID()
	stage, err := store.Capture(t.Context(), "owner", id, source, "preserve")
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "new")
	snap, err := Inspect(t.Context(), dest, StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Build(stage.Manifest, snap, false, "overwrite")
	if err != nil {
		t.Fatal(err)
	}
	dir, err := store.Directory("owner", id)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "file"), "tampered")
	out, err := store.Execute(t.Context(), "owner", id, dest, "corrupted-operation", plan)
	if !errors.Is(err, ErrChanged) || out.State != "failed" {
		t.Fatal(out, err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt stage wrote destination")
	}
	unlock, err := lockStore(t.Context(), filepath.Join(store.Root, ".lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if _, err := store.Get(ctx, "owner", id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("lock ignored cancellation", err)
	}
}

func TestSyncPlanSkipBlockedDirectory(t *testing.T) {
	source, dest := t.TempDir(), t.TempDir()
	if err := os.Mkdir(filepath.Join(source, "blocked"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(source, "blocked", "child"), "new")
	write(t, filepath.Join(dest, "blocked"), "keep")
	stage, m, err := Capture(t.Context(), source, privateTemp(t), "preserve", StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := Inspect(t.Context(), dest, StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Build(m, snap, true, "skip")
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(t.Context(), stage, dest, plan, StageLimits); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "blocked"))
	if err != nil || string(data) != "keep" {
		t.Fatal("skip modified blocked subtree", err)
	}
}

func TestSyncStageReadOnlyCleanupAndOrphanIsolation(t *testing.T) {
	source := t.TempDir()
	nested := filepath.Join(source, "readonly")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(nested, "file"), "readable")
	if err := os.Chmod(nested, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(nested, 0700) })
	store, err := NewStore(privateTemp(t))
	if err != nil {
		t.Fatal(err)
	}
	orphanID, _ := NewID()
	orphan, err := store.stagePath("crashed", orphanID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(orphan, 0700); err != nil {
		t.Fatal(err)
	}
	id, _ := NewID()
	if _, err := store.Capture(t.Context(), "healthy", id, source, "preserve"); err != nil {
		t.Fatal("another owner's unpublished stage blocked capture", err)
	}
	if err := store.Remove(t.Context(), "healthy", id); err != nil {
		t.Fatal("read-only retained source prevents cleanup", err)
	}
}
