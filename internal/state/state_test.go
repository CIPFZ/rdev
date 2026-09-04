package state

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestInspectAndMigrateVersionedRecords(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "jobs"), 0700); err != nil {
		t.Fatal(err)
	}
	d := filepath.Join(root, "jobs", "j1")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "meta.json"), []byte(`{"id":"j1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := Inspect(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Findings) != 1 || r.Findings[0].Kind != "manifest_missing" {
		t.Fatalf("findings=%+v", r.Findings)
	}
	dry, err := Migrate(root, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(dry.Changed) != 2 {
		t.Fatalf("dry changed=%v", dry.Changed)
	}
	if _, err := os.Stat(filepath.Join(root, "manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dry-run wrote manifest: %v", err)
	}
	if _, err := Migrate(root, false); err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	b, _ := os.ReadFile(filepath.Join(d, "meta.json"))
	if err := json.Unmarshal(b, &obj); err != nil {
		t.Fatal(err)
	}
	if obj["schema_version"] != float64(CurrentSchemaVersion) {
		t.Fatalf("record=%v", obj)
	}
	if _, err := os.Stat(filepath.Join(root, "backup")); err != nil {
		t.Fatal(err)
	}
}

func TestFutureSchemaFailsClosed(t *testing.T) {
	root := t.TempDir()
	b, _ := json.Marshal(Manifest{SchemaVersion: CurrentSchemaVersion + 1})
	if err := os.WriteFile(filepath.Join(root, manifestName), b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(root, false); !errors.Is(err, ErrFutureSchema) {
		t.Fatalf("err=%v", err)
	}
	got, _ := os.ReadFile(filepath.Join(root, manifestName))
	if string(got) != string(b) {
		t.Fatal("future manifest was modified")
	}
}

func TestMigrationLockAndRepairQuarantine(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "jobs"), 0700); err != nil {
		t.Fatal(err)
	}
	d := filepath.Join(root, "jobs", "bad")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "meta.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, lockName), []byte("held"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Repair(root, false); !errors.Is(err, ErrMigrationLocked) {
		t.Fatalf("lock err=%v", err)
	}
	os.Remove(filepath.Join(root, lockName))
	dry, err := Repair(root, true)
	if err != nil || len(dry.Quarantined) != 1 {
		t.Fatalf("dry=%+v err=%v", dry, err)
	}
	if _, err := Repair(root, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "quarantine")); err != nil {
		t.Fatal(err)
	}
}
