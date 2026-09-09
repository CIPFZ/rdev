package state

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Recovery evidence is never automatically retired. Exhaustion rejects a new
// migration/repair before mutation and requires deliberate external archival.
const RecoveryMaxRuns = 64
const RecoveryMaxFiles = 16384
const RecoveryMaxBytes int64 = 64 << 20

func reserveRecovery(root, kind string, sources []string) error {
	if kind != "backup" && kind != "quarantine" {
		return errors.New("invalid recovery kind")
	}
	base := filepath.Join(root, kind)
	runs, files := 0, 0
	var total int64
	if st, err := os.Lstat(base); err == nil {
		if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return errors.New("unsafe recovery directory")
		}
		entries, err := os.ReadDir(base)
		if err != nil {
			return err
		}
		runs = len(entries)
		err = filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if path == base {
				return nil
			}
			st, err := d.Info()
			if err != nil {
				return err
			}
			if st.Mode()&os.ModeSymlink != 0 || (!st.IsDir() && !st.Mode().IsRegular()) {
				return errors.New("unknown recovery object preserved")
			}
			files++
			if !st.IsDir() {
				if st.Size() > RecoveryMaxBytes-total {
					return errors.New("state recovery evidence budget exhausted")
				}
				total += st.Size()
			}
			if files > RecoveryMaxFiles || total > RecoveryMaxBytes {
				return errors.New("state recovery evidence budget exhausted")
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Migrate also copies an existing manifest when only records change. The
	// caller's changed-path list does not necessarily include that manifest.
	paths := make(map[string]bool, len(sources)+1)
	for _, rel := range sources {
		if !filepath.IsLocal(rel) {
			return errors.New("unsafe recovery source path")
		}
		paths[filepath.Clean(rel)] = true
	}
	if kind == "backup" {
		paths[manifestName] = true
	}
	for rel := range paths {
		st, err := os.Lstat(filepath.Join(root, rel))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if !st.Mode().IsRegular() {
			return errors.New("unsafe recovery source")
		}
		bytes := st.Size()
		if kind == "backup" {
			if bytes > MaxMetadataBytes {
				return errors.New("state recovery metadata exceeds budget")
			}
			data, err := os.ReadFile(filepath.Join(root, rel))
			if err != nil {
				return err
			}
			// This exactly matches writeAtomic's RawMessage serialization.
			// Compact JSON can grow substantially when indented; source stat
			// bytes therefore cannot stand in for the actual recovery bytes.
			encoded, err := json.MarshalIndent(json.RawMessage(data), "", "  ")
			if err != nil {
				return err
			}
			bytes = int64(len(encoded)) + 1
		}
		if bytes > RecoveryMaxBytes-total {
			return errors.New("state recovery evidence budget exhausted; preserve and archive before retry")
		}
		// Reserve both copied file and intermediate directories conservatively.
		files += 3
		total += bytes
	}
	if runs >= RecoveryMaxRuns || files+1 > RecoveryMaxFiles || total > RecoveryMaxBytes {
		return errors.New("state recovery evidence budget exhausted; preserve and archive before retry")
	}
	return nil
}
