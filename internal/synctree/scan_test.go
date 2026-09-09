package synctree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func scan(t *testing.T, path, policy string) Manifest {
	t.Helper()
	m, err := Scan(t.Context(), path, policy, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestLargeContentChangeWithPreservedMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large")
	content := strings.Repeat("x", 5<<20)
	write(t, path, content)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	before := scan(t, dir, "preserve")
	want := sha256.Sum256([]byte(content))
	if before.ContentBytes != int64(len(content)) || before.Entries[1].Digest != hex.EncodeToString(want[:]) {
		t.Fatal("large-file content was not fully hashed")
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
	if err := os.Chtimes(path, st.ModTime(), st.ModTime()); err != nil {
		t.Fatal(err)
	}
	after := scan(t, dir, "preserve")
	if before.Digest == after.Digest {
		t.Fatal("same-size/same-mtime content mutation escaped the manifest")
	}
}

func TestConfinedSymlinksAndFollowedContent(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "sub", "file"), "content")
	if err := os.Symlink("sub", filepath.Join(dir, "alias")); err != nil {
		t.Fatal(err)
	}
	preserve := scan(t, dir, "preserve")
	follow := scan(t, dir, "follow")
	skip := scan(t, dir, "skip")
	if len(preserve.Entries) != 4 || len(follow.Entries) != 5 || len(skip.Entries) != 3 || follow.ContentBytes != 14 {
		t.Fatal("link policy did not describe the actual transferred paths")
	}
	if follow.Entries[2].Path != "alias/file" || follow.Entries[2].Digest == "" {
		t.Fatal("follow did not hash content under linked directory")
	}
	outside := t.TempDir()
	write(t, filepath.Join(outside, "secret"), "outside")
	if err := os.Symlink(outside, filepath.Join(dir, "outside")); err != nil {
		t.Fatal(err)
	}
	if _, err := Scan(t.Context(), dir, "follow", Limits{}); err == nil {
		t.Fatal("follow escaped root")
	}
	if err := os.Remove(filepath.Join(dir, "outside")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..", filepath.Join(dir, "sub", "loop")); err != nil {
		t.Fatal(err)
	}
	if _, err := Scan(t.Context(), dir, "follow", Limits{}); err == nil {
		t.Fatal("directory link cycle accepted")
	}
}

func TestScanBudgetsAndCancellation(t *testing.T) {
	dir := t.TempDir()
	for i := range 300 {
		write(t, filepath.Join(dir, strings.Repeat("x", i%100)+string(rune(0x100+i))), "12345")
	}
	for _, limits := range []Limits{{Entries: 128}, {MetadataBytes: 1024}, {ContentBytes: 64}} {
		if _, err := Scan(t.Context(), dir, "preserve", limits); !errors.Is(err, ErrLimit) {
			t.Fatalf("budget was not enforced: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := Scan(ctx, dir, "preserve", Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled scan proceeded")
	}
	// A custom context cancels while enumeration is underway, without relying
	// on wall-clock timing or requiring a giant filesystem fixture.
	progress := &countedContext{Context: t.Context(), remaining: 20}
	if _, err := Scan(progress, dir, "preserve", Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatal("directory enumeration ignored cancellation")
	}
	if m := scan(t, dir, "preserve"); len(m.Entries) != 301 {
		t.Fatal("bounded enumeration omitted a batch")
	}
}

type countedContext struct {
	context.Context
	remaining int
}

func (c *countedContext) Err() error {
	c.remaining--
	if c.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func TestRawNamesAndStableDigest(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a\nb", "a\xffb", "a\xfeb", "back\\slash"} {
		write(t, filepath.Join(dir, name), name)
	}
	m := scan(t, dir, "preserve")
	if len(m.Entries) != 5 || m.Digest != scan(t, dir, "preserve").Digest {
		t.Fatal("manifest unstable or lost byte-exact names")
	}
	if err := os.Rename(filepath.Join(dir, "a\xffb"), filepath.Join(dir, "a\xfdb")); err != nil {
		t.Fatal(err)
	}
	if scan(t, dir, "preserve").Digest == m.Digest {
		t.Fatal("invalid UTF-8 filename substitution did not alter digest")
	}
}

func TestSingleFileRootAndEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	if m := scan(t, dir, "preserve"); len(m.Entries) != 1 || m.Entries[0].Path != "" || m.Entries[0].Kind != "directory" {
		t.Fatal("empty root identity missing")
	}
	path := filepath.Join(dir, "single")
	write(t, path, "one")
	if m := scan(t, path, "preserve"); len(m.Entries) != 1 || m.Entries[0].Path != "single" || m.ContentBytes != 3 {
		t.Fatal("single-file source changed semantics")
	}
}

func TestSameInfoDetectsReplacementWithPreservedMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file")
	write(t, path, "first")
	first, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, "next"), "first")
	if err := os.Chtimes(filepath.Join(dir, "next"), time.Now(), first.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "next"), path); err != nil {
		t.Fatal(err)
	}
	next, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if sameInfo(first, next) {
		t.Fatal("inode substitution accepted")
	}
}
