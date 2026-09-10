// Package state owns the versioned metadata that surrounds rdev's job state.
// It deliberately treats unknown data as opaque: inspection reports it, while
// migration and repair never delete or overwrite records they cannot prove are
// safe to understand.
package state

import (
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

func validateJobsDir(root string) error {
	jobs := filepath.Join(root, "jobs")
	st, err := os.Lstat(jobs)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
		return errors.New("state jobs is not a directory")
	}
	return nil
}

// ensurePrivateDir validates an existing child directory without following a
// symlink. It is used for backup/quarantine roots because MkdirAll alone would
// happily traverse an attacker-supplied symlink.
func ensurePrivateDir(path string, mode os.FileMode) error {
	return ensurePrivateDirSynced(path, mode, syncDirectory)
}
func ensurePrivateDirSynced(path string, mode os.FileMode, sync func(string) error) error {
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err = ensurePrivateDirSynced(filepath.Dir(path), mode, sync); err != nil {
			return err
		}
		if err = os.Mkdir(path, mode); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		st, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
		return fmt.Errorf("%s is not a directory", filepath.Base(path))
	}
	// Persist the child contents and its parent's name binding before a caller
	// may overwrite active metadata based on a supposedly durable backup.
	if err = sync(path); err != nil {
		return err
	}
	return sync(filepath.Dir(path))
}

func loadManifest(root string) (*Manifest, error) {
	p := filepath.Join(root, manifestName)
	st, err := privateRegular(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Manifest{SchemaVersion: CurrentSchemaVersion}, nil
		}
		return nil, err
	}
	if st.Size() > MaxMetadataBytes {
		return nil, errors.New("state manifest exceeds budget")
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var m Manifest
	var fields map[string]json.RawMessage
	if err := decodeMetadata(b, &fields, false); err != nil {
		return nil, err
	}
	for key := range fields {
		switch key {
		case "schema_version", "writer_version", "agent_identity", "namespace", "last_migration":
		default:
			return nil, errors.New("unknown state manifest field")
		}
	}
	if err := decodeMetadata(b, &m, true); err != nil {
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

func acquire(root string) (func(), error) {
	l, err := acquireLease(root, true)
	if err != nil {
		return nil, err
	}
	return func() { _ = l.Close() }, nil
}

func Inspect(root string) (Report, error) {
	r := Report{Root: root, SchemaVersion: CurrentSchemaVersion}
	if err := validateRoot(root); err != nil {
		return r, err
	}
	if err := validateJobsDir(root); err != nil {
		return r, err
	}
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
			var b []byte
			var readErr error
			if st.Size() > MaxMetadataBytes {
				readErr = errors.New("state record exceeds budget")
			} else {
				b, readErr = os.ReadFile(full)
			}
			version, schemaErr := ValidateRecordSchema(b)
			if readErr != nil || schemaErr != nil {
				rec.Valid = false
				rec.SchemaVersion = version
				kind, action := "record_corrupt", "quarantine after review"
				if errors.Is(schemaErr, ErrFutureSchema) {
					kind, action = "record_future", "use a compatible reader; do not quarantine or downgrade"
				}
				r.Findings = append(r.Findings, Finding{Path: recPath, Kind: kind, Message: "invalid or future schema", Action: action})
			} else {
				rec.Valid = true
				rec.SchemaVersion = version
			}
		}
		r.Records = append(r.Records, rec)
	}
	return r, nil
}

func Migrate(root string, dryRun bool) (Report, error) {
	if !dryRun {
		release, err := acquire(root)
		if err != nil {
			return Report{}, err
		}
		defer release()
	}
	r, err := Inspect(root)
	if err != nil {
		return r, err
	}
	r.DryRun = dryRun
	for _, f := range r.Findings {
		if f.Kind == "record_future" {
			return r, ErrFutureSchema
		}
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
	if err := reserveRecovery(root, "backup", r.Changed); err != nil {
		return r, err
	}
	backupDir := filepath.Join(root, "backup", time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := ensurePrivateDir(filepath.Dir(backupDir), 0700); err != nil {
		return r, err
	}
	if err := ensurePrivateDir(backupDir, 0700); err != nil {
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
		version, schemaErr := ValidateRecordSchema(b)
		if schemaErr != nil {
			return r, schemaErr
		}
		if version != rec.SchemaVersion {
			return r, errors.New("state record changed during migration")
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(b, &obj); err != nil {
			return r, err
		}
		// Keep a complete, root-relative backup before changing each record. A
		// later failure therefore leaves an operator a reversible recovery path.
		backupPath := filepath.Join(backupDir, filepath.FromSlash(rec.Path))
		if err := ensurePrivateDir(filepath.Dir(backupPath), 0700); err != nil {
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
	if !dryRun {
		release, err := acquire(root)
		if err != nil {
			return Report{}, err
		}
		defer release()
	}
	r, err := Inspect(root)
	if err != nil {
		return r, err
	}
	r.DryRun = dryRun
	for _, f := range r.Findings {
		if f.Kind == "manifest_invalid" {
			return r, errors.New("state manifest is invalid; refusing repair")
		}
	}
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
	if err := reserveRecovery(root, "quarantine", r.Quarantined); err != nil {
		return r, err
	}
	qroot := filepath.Join(root, "quarantine", time.Now().UTC().Format("20060102T150405.000000000Z"))
	if err := ensurePrivateDir(filepath.Dir(qroot), 0700); err != nil {
		return r, err
	}
	if err := ensurePrivateDir(qroot, 0700); err != nil {
		return r, err
	}
	for _, rel := range r.Quarantined {
		src := filepath.Join(root, rel)
		dst := filepath.Join(qroot, filepath.Base(filepath.Dir(src))+"-meta.json")
		if err := renameRecovery(src, dst, syncDirectory); err != nil {
			return r, err
		}
	}
	return r, nil
}

func renameRecovery(src, dst string, sync func(string) error) error {
	if err := os.Rename(src, dst); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := sync(filepath.Dir(dst)); err != nil {
		return err
	}
	return sync(filepath.Dir(src))
}
