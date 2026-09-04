package client

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const maxSyncManifestEntries = 100000

// syncManifest is an immutable snapshot of a local source tree. It is hashed
// before rsync starts and surfaced in SyncResult, allowing callers to audit
// exactly which source state was used for a --delete operation.
type syncManifest struct {
	Digest  string
	Entries int
}

func buildSyncManifest(root string, symlinkPolicy string) (syncManifest, error) {
	var rows []string
	info, err := os.Lstat(root)
	if err != nil {
		return syncManifest{}, err
	}
	add := func(path, rel string, i fs.FileInfo) error {
		if len(rows) >= maxSyncManifestEntries {
			return fmt.Errorf("sync manifest exceeds %d entries", maxSyncManifestEntries)
		}
		kind := "f"
		extra := ""
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
		}
		rows = append(rows, strings.Join([]string{rel, kind, strconv.FormatInt(i.Size(), 10), strconv.FormatUint(uint64(i.Mode()), 10), strconv.FormatInt(i.ModTime().UnixNano(), 10), extra}, "\x00"))
		return nil
	}
	if !info.IsDir() {
		if err := add(root, filepath.Base(root), info); err != nil {
			return syncManifest{}, err
		}
	} else {
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			rel, e := filepath.Rel(root, path)
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
	return syncManifest{Digest: hex.EncodeToString(h.Sum(nil)), Entries: len(rows)}, nil
}
