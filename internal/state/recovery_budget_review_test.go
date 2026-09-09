package state

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The reservation must cover what Migrate actually writes, including compact
// JSON expansion and an unchanged manifest that is still copied for recovery.
func TestMigrationRecoveryBudgetMatchesActualWrittenBytes(t *testing.T) {
	for _, manifest := range []bool{false, true} {
		name := "compact_record"
		if manifest {
			name = "unchanged_manifest"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(filepath.Join(root, "jobs", "legacy"), 0700); err != nil {
				t.Fatal(err)
			}
			raw := []byte(`{"values":[` + strings.Repeat("0,", 128) + `0]}`)
			record := filepath.Join(root, "jobs", "legacy", "meta.json")
			if err := os.WriteFile(record, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if manifest {
				data := []byte(`{"schema_version":1,"writer_version":"` + strings.Repeat("v", 4096) + `"}`)
				if err := os.WriteFile(filepath.Join(root, "manifest.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			prior := filepath.Join(root, "backup", "prior")
			if err := os.MkdirAll(prior, 0700); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(filepath.Join(prior, "evidence"), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(RecoveryMaxBytes - int64(len(raw)) - 1); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			_, migrateErr := Migrate(root, false)
			var total int64
			if err := filepath.WalkDir(filepath.Join(root, "backup"), func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !d.IsDir() {
					st, err := d.Info()
					if err != nil {
						return err
					}
					total += st.Size()
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if total > RecoveryMaxBytes {
				t.Fatalf("migration exceeded reserved recovery bytes: got=%d max=%d err=%v", total, RecoveryMaxBytes, migrateErr)
			}
			if migrateErr != nil {
				after, err := os.ReadFile(record)
				if err != nil || !bytes.Equal(raw, after) {
					t.Fatal("rejected migration mutated record before budget check")
				}
			}
		})
	}
}

func TestRecoveryReservationCountsDistinctExactObjects(t *testing.T) {
	for _, kind := range []string{"backup", "quarantine"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			data := []byte(`{"value":true}`)
			if err := os.WriteFile(filepath.Join(root, "meta.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			sources := []string{"meta.json", "meta.json"}
			need := int64(len(data))
			if kind == "backup" {
				encoded, err := json.MarshalIndent(json.RawMessage(data), "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				need = int64(len(encoded) + 1)
				manifest := []byte(`{"schema_version":1}`)
				if err := os.WriteFile(filepath.Join(root, manifestName), manifest, 0600); err != nil {
					t.Fatal(err)
				}
				encoded, err = json.MarshalIndent(json.RawMessage(manifest), "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				need += int64(len(encoded) + 1)
				sources = append(sources, manifestName, manifestName)
			}
			prior := filepath.Join(root, kind, "prior")
			if err := os.MkdirAll(prior, 0700); err != nil {
				t.Fatal(err)
			}
			f, err := os.OpenFile(filepath.Join(prior, "evidence"), os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(RecoveryMaxBytes - need); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if err := reserveRecovery(root, kind, sources); err != nil {
				t.Fatal("exact byte boundary double-counted a recovery object", err)
			}
		})
	}
}
