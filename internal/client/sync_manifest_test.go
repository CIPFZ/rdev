package client

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSyncManifestDetectsLargeContentChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 5<<20)), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := buildSyncManifest(dir, "preserve")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("y"), 4<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := verifySyncManifest(dir, "preserve", before); err == nil {
		t.Fatal("large source content changed without invalidating its manifest")
	}
}

func TestDanglingFollowLinkDoesNotDiscardManifest(t *testing.T) {
	c := syncTestClient(t)
	dir := t.TempDir()
	if err := os.Symlink("missing", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	called := false
	c.rsync = func(context.Context, []string, io.Writer, io.Writer) error { called = true; return nil }
	if _, err := c.Sync(t.Context(), SyncOptions{Host: "dev", Direction: "push", Local: dir, Remote: "dst", SymlinkPolicy: "follow"}); err == nil || called {
		t.Fatal("dangling source link discarded the guard and invoked rsync")
	}
}
