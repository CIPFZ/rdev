package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/fileedit"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestEditBackupAndRollback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.txt")
	if err := os.WriteFile(path, []byte("before\n"), 0644); err != nil {
		t.Fatal(err)
	}
	digest := fileedit.Digest([]byte("before\n"))
	content := "after\n"
	res, err := doEdit(context.Background(), &proto.EditParams{Path: path, Kind: "replace", BaseDigest: digest, Content: &content, Backup: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.BackupID == "" || !res.Committed {
		t.Fatalf("missing backup result: %+v", res)
	}
	got, _ := os.ReadFile(path)
	if string(got) != content {
		t.Fatalf("edited content = %q", got)
	}
	rolled, err := doEditRollback(context.Background(), &proto.EditRollbackParams{Path: path, BackupID: res.BackupID, ExpectedDigest: res.NewDigest})
	if err != nil {
		t.Fatal(err)
	}
	if !rolled.Committed || rolled.NewDigest != digest {
		t.Fatalf("rollback result: %+v", rolled)
	}
	got, _ = os.ReadFile(path)
	if string(got) != "before\n" {
		t.Fatalf("restored content = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ".rdev-backup-"+res.BackupID)); !os.IsNotExist(err) {
		t.Fatalf("backup remains: %v", err)
	}
}
