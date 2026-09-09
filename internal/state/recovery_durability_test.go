package state

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryDirectoryBindingSyncFailureStopsPreparation(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "backup", "run", "jobs")
	failed := errors.New("injected directory sync failure")
	observed := map[string]bool{}
	err := ensurePrivateDirSynced(dir, 0700, func(path string) error {
		observed[path] = true
		if path == filepath.Join(root, "backup") {
			return failed
		}
		return nil
	})
	if !errors.Is(err, failed) {
		t.Fatalf("ignored ancestor sync failure: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("prepared descendants after failed durable parent binding")
	}
	if !observed[filepath.Join(root, "backup")] {
		t.Fatal("parent not synchronized")
	}
}
func TestQuarantineSyncsBothDirectoriesAndRetainsMovedEvidenceOnFailure(t *testing.T) {
	for _, failDest := range []bool{false, true} {
		root := t.TempDir()
		srcdir := filepath.Join(root, "job")
		dstdir := filepath.Join(root, "quarantine")
		for _, dir := range []string{srcdir, dstdir} {
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
		}
		src, dst := filepath.Join(srcdir, "meta.json"), filepath.Join(dstdir, "meta.json")
		if err := os.WriteFile(src, []byte("corrupt-preserve"), 0600); err != nil {
			t.Fatal(err)
		}
		var synced []string
		failed := errors.New("injected sync failure")
		err := renameRecovery(src, dst, func(path string) error {
			synced = append(synced, path)
			if failDest {
				return failed
			}
			return nil
		})
		if failDest && !errors.Is(err, failed) {
			t.Fatal("sync failure hidden")
		}
		if !failDest && (err != nil || len(synced) != 2 || synced[0] != dstdir || synced[1] != srcdir) {
			t.Fatalf("rename directories not synced: %v %v", synced, err)
		}
		if b, err := os.ReadFile(dst); err != nil || string(b) != "corrupt-preserve" {
			t.Fatal("evidence discarded after move")
		}
	}
}
