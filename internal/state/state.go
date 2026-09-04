// Package state owns the versioned metadata that surrounds rdev's job state.
// It deliberately treats unknown data as opaque: inspection reports it, while
// migration and repair never delete or overwrite records they cannot prove are
// safe to understand.
package state

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const CurrentSchemaVersion = 1

var ErrFutureSchema = errors.New("state schema is newer than this agent")
var ErrMigrationLocked = errors.New("state migration is already locked")

type Manifest struct {
	SchemaVersion int    `json:"schema_version"`
	WriterVersion string `json:"writer_version,omitempty"`
	AgentIdentity string `json:"agent_identity,omitempty"`
	Namespace     string `json:"namespace,omitempty"`
	LastMigration string `json:"last_migration,omitempty"`
}

type Finding struct {
	Path    string `json:"path"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
	Action  string `json:"action,omitempty"`
}

type Record struct {
	Path          string `json:"path"`
	SchemaVersion int    `json:"schema_version"`
	Valid         bool   `json:"valid"`
	Bytes         int64  `json:"bytes"`
}

type Report struct {
	Root          string    `json:"root"`
	DryRun        bool      `json:"dry_run"`
	SchemaVersion int       `json:"schema_version"`
	Manifest      *Manifest `json:"manifest,omitempty"`
	Records       []Record  `json:"records,omitempty"`
	Findings      []Finding `json:"findings,omitempty"`
	Changed       []string  `json:"changed,omitempty"`
	Quarantined   []string  `json:"quarantined,omitempty"`
}

const manifestName = "manifest.json"
const lockName = ".migration.lock"

func privateRegular(path string) (os.FileInfo, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", filepath.Base(path))
	}
	if st.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s is not private", filepath.Base(path))
	}
	return st, nil
}

func loadManifest(root string) (*Manifest, error) {
	p := filepath.Join(root, manifestName)
	if _, err := privateRegular(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Manifest{SchemaVersion: CurrentSchemaVersion}, nil
		}
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if m.SchemaVersion <= 0 {
		return nil, errors.New("manifest schema_version is required")
	}
	if m.SchemaVersion > CurrentSchemaVersion {
		return nil, ErrFutureSchema
	}
	return &m, nil
}

func writeAtomic(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".rdev-state-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func acquire(root string) (func(), error) {
	p := filepath.Join(root, lockName)
	// O_EXCL makes acquisition atomic. Lstat rejects a pre-existing symlink;
	// the exclusive create then closes the check/create race without following it.
	if st, statErr := os.Lstat(p); statErr == nil && st.Mode()&os.ModeSymlink != 0 {
		return nil, ErrMigrationLocked
	}
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, ErrMigrationLocked
		}
		return nil, err
	}
	var token [16]byte
	if _, err = rand.Read(token[:]); err != nil {
		f.Close()
		os.Remove(p)
		return nil, err
	}
	_, _ = f.WriteString(fmt.Sprintf("pid=%d token=%s\n", os.Getpid(), hex.EncodeToString(token[:])))
	_ = f.Sync()
	_ = f.Close()
	return func() { _ = os.Remove(p) }, nil
}

func Inspect(root string) (Report, error) {
	r := Report{Root: root, SchemaVersion: CurrentSchemaVersion}
	manifestPath := filepath.Join(root, manifestName)
	_, manifestStatErr := os.Lstat(manifestPath)
	m, err := loadManifest(root)
	if err != nil {
		r.Findings = append(r.Findings, Finding{Path: manifestName, Kind: "manifest_invalid", Message: err.Error(), Action: "repair or restore manifest"})
		return r, nil
	}
	if errors.Is(manifestStatErr, os.ErrNotExist) {
		r.Findings = append(r.Findings, Finding{Path: manifestName, Kind: "manifest_missing", Message: "state manifest is missing", Action: "run state migrate"})
	}
	r.Manifest = m
	r.SchemaVersion = m.SchemaVersion
	jobs := filepath.Join(root, "jobs")
	entries, err := os.ReadDir(jobs)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return r, nil
		}
		return r, err
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		recPath := filepath.Join("jobs", e.Name(), "meta.json")
		full := filepath.Join(root, recPath)
		st, statErr := privateRegular(full)
		rec := Record{Path: recPath}
		if statErr != nil {
			rec.Valid = false
			rec.SchemaVersion = 0
			r.Findings = append(r.Findings, Finding{Path: recPath, Kind: "record_corrupt", Message: statErr.Error(), Action: "inspect and repair manually"})
		} else {
			rec.Bytes = st.Size()
			b, readErr := os.ReadFile(full)
			var raw struct {
				SchemaVersion int `json:"schema_version"`
			}
			unmarshalErr := json.Unmarshal(b, &raw)
			if readErr != nil || unmarshalErr != nil || raw.SchemaVersion > CurrentSchemaVersion {
				rec.Valid = false
				rec.SchemaVersion = raw.SchemaVersion
				r.Findings = append(r.Findings, Finding{Path: recPath, Kind: "record_corrupt", Message: "invalid or future schema", Action: "quarantine after review"})
			} else {
				rec.Valid = true
				rec.SchemaVersion = raw.SchemaVersion
			}
		}
		r.Records = append(r.Records, rec)
	}
	return r, nil
}

func Migrate(root string, dryRun bool) (Report, error) {
	r, err := Inspect(root)
	if err != nil {
		return r, err
	}
	r.DryRun = dryRun
	for _, f := range r.Findings {
		if f.Kind == "manifest_invalid" {
			if strings.Contains(f.Message, ErrFutureSchema.Error()) {
				return r, ErrFutureSchema
			}
			return r, errors.New("state manifest is invalid; refusing migration")
		}
	}
	if r.Manifest == nil {
		r.Manifest = &Manifest{SchemaVersion: CurrentSchemaVersion}
		r.Changed = append(r.Changed, manifestName)
	}
	for _, f := range r.Findings {
		if f.Kind == "manifest_missing" {
			r.Changed = append(r.Changed, manifestName)
			break
		}
	}
	if r.Manifest.SchemaVersion > CurrentSchemaVersion {
		return r, ErrFutureSchema
	}
	for _, rec := range r.Records {
		if rec.Valid && rec.SchemaVersion < CurrentSchemaVersion {
			r.Changed = append(r.Changed, rec.Path)
		}
	}
	if dryRun || len(r.Changed) == 0 {
		return r, nil
	}
	release, err := acquire(root)
	if err != nil {
		return r, err
	}
	defer release()
	backupDir := filepath.Join(root, "backup", time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := os.MkdirAll(backupDir, 0700); err != nil {
		return r, err
	}
	if old, e := os.ReadFile(filepath.Join(root, manifestName)); e == nil {
		if err := writeAtomic(filepath.Join(backupDir, manifestName), json.RawMessage(old)); err != nil {
			return r, err
		}
	}
	for _, rec := range r.Records {
		if !rec.Valid || rec.SchemaVersion >= CurrentSchemaVersion {
			continue
		}
		path := filepath.Join(root, rec.Path)
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return r, readErr
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(b, &obj); err != nil {
			return r, err
		}
		// Keep a complete, root-relative backup before changing each record. A
		// later failure therefore leaves an operator a reversible recovery path.
		backupPath := filepath.Join(backupDir, filepath.FromSlash(rec.Path))
		if err := os.MkdirAll(filepath.Dir(backupPath), 0700); err != nil {
			return r, err
		}
		if err := writeAtomic(backupPath, json.RawMessage(b)); err != nil {
			return r, err
		}
		obj["schema_version"] = json.RawMessage(fmt.Sprintf("%d", CurrentSchemaVersion))
		if err := writeAtomic(path, obj); err != nil {
			return r, err
		}
	}
	m := *r.Manifest
	m.SchemaVersion = CurrentSchemaVersion
	m.LastMigration = time.Now().UTC().Format(time.RFC3339Nano)
	if err := writeAtomic(filepath.Join(root, manifestName), &m); err != nil {
		return r, err
	}
	r.Manifest = &m
	return r, nil
}

func Repair(root string, dryRun bool) (Report, error) {
	r, err := Inspect(root)
	if err != nil {
		return r, err
	}
	r.DryRun = dryRun
	for _, f := range r.Findings {
		if f.Kind == "record_corrupt" {
			r.Quarantined = append(r.Quarantined, f.Path)
		}
	}
	// Repair is intentionally conservative: it only quarantines files whose
	// metadata is unreadable, and never removes unknown data. Require dry-run
	// for callers to preview; execution is still lock-protected and reversible.
	if dryRun || len(r.Quarantined) == 0 {
		return r, nil
	}
	release, err := acquire(root)
	if err != nil {
		return r, err
	}
	defer release()
	qroot := filepath.Join(root, "quarantine", time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := os.MkdirAll(qroot, 0700); err != nil {
		return r, err
	}
	for _, rel := range r.Quarantined {
		src := filepath.Join(root, rel)
		dst := filepath.Join(qroot, filepath.Base(filepath.Dir(src))+"-meta.json")
		if err := os.Rename(src, dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return r, err
		}
	}
	return r, nil
}
