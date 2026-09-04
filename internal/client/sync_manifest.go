package client

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const maxSyncManifestEntries = 100000

// Keep manifest construction bounded even when a tree contains millions of
// metadata rows.  A manifest is an audit/snapshot guard, not an unbounded
// inventory database.
const maxSyncManifestBytes = 16 << 20
const maxSyncManifestDigestBytes = 4 << 20

// syncManifest is an immutable snapshot of a local source tree. It is hashed
// before rsync starts and surfaced in SyncResult, allowing callers to audit
// exactly which source state was used for a --delete operation.
type syncManifest struct {
	Digest   string
	Entries  int
	Complete bool
}

func buildSyncManifest(root string, symlinkPolicy string) (syncManifest, error) {
	var rows []string
	var rowBytes int
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return syncManifest{}, err
	}
	info, err := os.Lstat(rootAbs)
	if err != nil {
		return syncManifest{}, err
	}
	add := func(path, rel string, i fs.FileInfo) error {
		if len(rows) >= maxSyncManifestEntries {
			return fmt.Errorf("sync manifest exceeds %d entries", maxSyncManifestEntries)
		}
		kind := "f"
		extra := ""
		digest := "-"
		if i.IsDir() {
			kind = "d"
		} else if i.Mode()&os.ModeSymlink != 0 {
			kind = "l"
			if symlinkPolicy == "skip" {
				return nil
			}
			target, e := os.Readlink(path)
			if e != nil {
				return e
			}
			extra = target
		} else if i.Mode().IsRegular() && i.Size() <= maxSyncManifestDigestBytes {
			f, e := os.Open(path)
			if e != nil {
				return e
			}
			h := sha256.New()
			_, copyErr := io.Copy(h, f)
			closeErr := f.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			digest = hex.EncodeToString(h.Sum(nil))
		}
		// follow must not silently escape the source root. rsync --copy-links
		// would otherwise copy arbitrary files pointed to by an untrusted link.
		if i.Mode()&os.ModeSymlink != 0 && symlinkPolicy == "follow" {
			resolved, e := filepath.EvalSymlinks(path)
			if e != nil {
				return e
			}
			relResolved, e := filepath.Rel(rootAbs, resolved)
			if e != nil || relResolved == ".." || strings.HasPrefix(relResolved, ".."+string(filepath.Separator)) {
				return fmt.Errorf("symlink escapes sync root")
			}
		}
		row := strings.Join([]string{rel, kind, strconv.FormatInt(i.Size(), 10), strconv.FormatUint(uint64(i.Mode()), 10), strconv.FormatInt(i.ModTime().UnixNano(), 10), digest, extra}, "\x00")
		if rowBytes+len(row)+1 > maxSyncManifestBytes {
			return fmt.Errorf("sync manifest exceeds %d bytes", maxSyncManifestBytes)
		}
		rowBytes += len(row) + 1
		rows = append(rows, row)
		return nil
	}
	if !info.IsDir() {
		if err := add(rootAbs, filepath.Base(rootAbs), info); err != nil {
			return syncManifest{}, err
		}
	} else {
		err = filepath.WalkDir(rootAbs, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			rel, e := filepath.Rel(rootAbs, path)
			if e != nil {
				return e
			}
			if rel == "." {
				rel = ""
			}
			i, e := d.Info()
			if e != nil {
				return e
			}
			return add(path, filepath.ToSlash(rel), i)
		})
		if err != nil {
			return syncManifest{}, err
		}
	}
	sort.Strings(rows)
	h := sha256.New()
	for _, row := range rows {
		_, _ = h.Write([]byte(row))
		_, _ = h.Write([]byte{'\n'})
	}
	return syncManifest{Digest: hex.EncodeToString(h.Sum(nil)), Entries: len(rows), Complete: true}, nil
}

// verifySyncManifest re-reads the source after rsync. A successful transfer
// whose source changed while in flight is reported as a conflict instead of
// pretending the advertised snapshot was actually transferred.
func verifySyncManifest(root, policy string, expected syncManifest) error {
	if expected.Digest == "" {
		return nil
	}
	got, err := buildSyncManifest(root, policy)
	if err != nil {
		return err
	}
	if !got.Complete || got.Digest != expected.Digest || got.Entries != expected.Entries {
		return fmt.Errorf("sync source changed during transfer")
	}
	return nil
}
