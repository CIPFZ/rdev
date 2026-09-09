package synctree

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const StageContentBytes int64 = 256 << 20
const StageTTL = 10 * time.Minute
const ChunkBytes = 128 << 10
const maxStages = 16
const maxOwnerStages = 4
const maxOutcomes = 8192
const maxOwnerOutcomes = 1024

var StageLimits = Limits{Entries: 8192, MetadataBytes: 2 << 20, ContentBytes: StageContentBytes}
var ErrRecorded = errors.New("sync operation already recorded; query its outcome")
var ErrStage = errors.New("sync stage unavailable, expired or invalid")

type Stage struct {
	SourceDirectory bool      `json:"source_directory"`
	SourceName      string    `json:"-"`
	Owner           string    `json:"owner"`
	ID              string    `json:"id"`
	Expires         time.Time `json:"expires"`
	Manifest        Manifest  `json:"manifest"`
	Ready           bool      `json:"ready"`
}
type Outcome struct {
	Owner       string `json:"owner"`
	OperationID string `json:"operation_id"`
	Digest      string `json:"digest"`
	State       string `json:"state"`
}
type Store struct{ Root string }

func NewStore(path string) (*Store, error) {
	if err := PrivateDirectory(path); err != nil {
		return nil, err
	}
	return &Store{Root: path}, nil
}
func ownerHash(owner string) string {
	h := sha256.Sum256([]byte(owner))
	return hex.EncodeToString(h[:])
}
func validID(id string) bool {
	if len(id) != 64 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}
func (s *Store) stagePath(owner, id string) (string, error) {
	if owner == "" || len(owner) > 512 || !validID(id) {
		return "", ErrStage
	}
	return filepath.Join(s.Root, ownerHash(owner)+"-"+id), nil
}
func (s *Store) WithLock(ctx context.Context, fn func() error) error {
	if err := PrivateDirectory(s.Root); err != nil {
		return err
	}
	unlock, err := lockStore(ctx, filepath.Join(s.Root, ".lock"))
	if err != nil {
		return err
	}
	defer unlock()
	return fn()
}
func readPrivateJSON(path string, out any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || !ownedByCurrentUser(info) || info.Size() > 4<<20 {
		return ErrStage
	}
	parent, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer parent.Close()
	f, err := openEntry(parent, filepath.Base(path), false, false)
	if err != nil {
		return err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !sameInfo(info, opened) {
		return ErrStage
	}
	data, err := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	if err != nil || len(data) > 4<<20 {
		return ErrStage
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return ErrStage
	}
	if dec.Decode(new(any)) != io.EOF {
		return ErrStage
	}
	return nil
}
func writePrivateJSON(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(data) > 4<<20 {
		return ErrLimit
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	tmp, err := temporaryName()
	if err != nil {
		return err
	}
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer root.Remove(tmp)
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := root.Rename(tmp, filepath.Base(path)); err != nil {
		return err
	}
	return syncRoot(root)
}
func (s *Store) load(owner, id string) (Stage, string, error) {
	dir, err := s.stagePath(owner, id)
	if err != nil {
		return Stage{}, "", err
	}
	if err := privateExistingDirectory(dir); err != nil {
		return Stage{}, "", ErrStage
	}
	var stage Stage
	if err := readPrivateJSON(filepath.Join(dir, "meta.json"), &stage); err != nil {
		return Stage{}, "", ErrStage
	}
	if stage.ID != id || stage.Owner != ownerHash(owner) || time.Now().After(stage.Expires) || ValidateManifest(stage.Manifest) != nil {
		return Stage{}, "", ErrStage
	}
	return stage, dir, nil
}
func privateExistingDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 || !ownedByCurrentUser(info) {
		return ErrStage
	}
	return nil
}
func (s *Store) reserve(owner, id string, manifest Manifest) (Stage, string, error) {
	if err := ValidateManifest(manifest); err != nil {
		return Stage{}, "", err
	}
	if manifest.ContentBytes > StageContentBytes {
		return Stage{}, "", ErrLimit
	}
	dir, err := s.stagePath(owner, id)
	if err != nil {
		return Stage{}, "", err
	}
	f, err := os.Open(s.Root)
	if err != nil {
		return Stage{}, "", err
	}
	names, err := f.Readdirnames(maxStages + 3)
	f.Close()
	if err != nil && err != io.EOF {
		return Stage{}, "", err
	}
	count, owned := 0, 0
	for _, name := range names {
		if strings.HasPrefix(name, ".") {
			continue
		}
		// Only generated stage names are eligible for expiry cleanup.
		if len(name) != 129 || name[64] != '-' || !validID(name[:64]) || !validID(name[65:]) {
			return Stage{}, "", ErrStage
		}
		var old Stage
		path := filepath.Join(s.Root, name)
		if privateExistingDirectory(path) != nil {
			return Stage{}, "", ErrStage
		}
		if err := readPrivateJSON(filepath.Join(path, "meta.json"), &old); err != nil {
			// A crash between mkdir and metadata publication leaves an orphan.
			// Charge its slot until TTL rather than blocking every other owner.
			info, statErr := os.Lstat(path)
			if statErr != nil {
				return Stage{}, "", statErr
			}
			old.Expires = info.ModTime().Add(StageTTL)
		}
		if time.Now().After(old.Expires) {
			if err := RemoveCaptured(path); err != nil {
				return Stage{}, "", err
			}
			continue
		}
		count++
		if strings.HasPrefix(name, ownerHash(owner)+"-") {
			owned++
		}
	}
	if count >= maxStages || owned >= maxOwnerStages {
		return Stage{}, "", ErrLimit
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return Stage{}, "", ErrStage
	}
	stage := Stage{Owner: ownerHash(owner), ID: id, Expires: time.Now().Add(StageTTL), Manifest: manifest}
	if err := writePrivateJSON(filepath.Join(dir, "meta.json"), stage); err != nil {
		RemoveCaptured(dir)
		return Stage{}, "", err
	}
	return stage, dir, nil
}
func (s *Store) Capture(ctx context.Context, owner, id, source, policy string) (Stage, error) {
	var out Stage
	err := s.WithLock(ctx, func() error {
		if old, _, err := s.load(owner, id); err == nil && old.Ready {
			out = old
			return nil
		}
		manifest, err := Scan(ctx, source, policy, StageLimits)
		if err != nil {
			return err
		}
		// A single file gains a directory wrapper in the retained representation.
		stage, dir, err := s.reserve(owner, id, manifest)
		if err != nil {
			return err
		}
		done := false
		defer func() {
			if !done {
				RemoveCaptured(dir)
			}
		}()
		captured, m, err := Capture(ctx, source, dir, policy, StageLimits)
		if err != nil {
			return err
		}
		if err := os.Rename(captured, filepath.Join(dir, "data")); err != nil {
			return err
		}
		stage.Manifest = m
		info, e := os.Lstat(source)
		if e != nil {
			return e
		}
		stage.SourceDirectory = info.IsDir()
		stage.SourceName = filepath.Base(filepath.Clean(source))
		if !filepath.IsLocal(stage.SourceName) || stage.SourceName == "." || strings.ContainsRune(stage.SourceName, 0) {
			return ErrStage
		}
		stage.Ready = true
		if err := writePrivateJSON(filepath.Join(dir, "meta.json"), stage); err != nil {
			return err
		}
		out = stage
		done = true
		return nil
	})
	return out, err
}
func (s *Store) Begin(ctx context.Context, owner, id string, m Manifest) (Stage, error) {
	var out Stage
	err := s.WithLock(ctx, func() error {
		if old, _, err := s.load(owner, id); err == nil {
			if old.Manifest.Digest != m.Digest {
				return ErrStage
			}
			out = old
			return nil
		}
		stage, dir, err := s.reserve(owner, id, m)
		if err != nil {
			return err
		}
		done := false
		defer func() {
			if !done {
				RemoveCaptured(dir)
			}
		}()
		if err := os.Mkdir(filepath.Join(dir, "data"), 0700); err != nil {
			return err
		}
		root, err := os.OpenRoot(filepath.Join(dir, "data"))
		if err != nil {
			return err
		}
		defer root.Close()
		for _, entry := range m.Entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.Path == "" {
				continue
			}
			switch entry.Kind {
			case "directory":
				err = root.Mkdir(entry.Path, 0700)
			case "symlink":
				err = root.Symlink(entry.Link, entry.Path)
			case "file":
				var f *os.File
				f, err = root.OpenFile(entry.Path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if err == nil {
					err = f.Truncate(entry.Size)
					closeErr := f.Close()
					if err == nil {
						err = closeErr
					}
				}
			}
			if err != nil {
				return err
			}
		}
		done = true
		out = stage
		return nil
	})
	return out, err
}
func (s *Store) Put(ctx context.Context, owner, id string, index int, offset int64, data []byte) error {
	if len(data) > ChunkBytes || len(data) == 0 {
		return ErrLimit
	}
	return s.WithLock(ctx, func() error {
		stage, dir, err := s.load(owner, id)
		if err != nil {
			return err
		}
		if stage.Ready || index < 0 || index >= len(stage.Manifest.Entries) {
			return ErrStage
		}
		entry := stage.Manifest.Entries[index]
		if entry.Kind != "file" || offset < 0 || offset > entry.Size-int64(len(data)) {
			return ErrStage
		}
		root, err := os.OpenRoot(filepath.Join(dir, "data"))
		if err != nil {
			return err
		}
		defer root.Close()
		// The private stage never exposes a caller-supplied destination path.
		f, err := root.OpenFile(entry.Path, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() != entry.Size {
			return ErrStage
		}
		n, err := f.WriteAt(data, offset)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		return err
	})
}
func (s *Store) Seal(ctx context.Context, owner, id string) (Stage, error) {
	var out Stage
	err := s.WithLock(ctx, func() error {
		stage, dir, err := s.load(owner, id)
		if err != nil {
			return err
		}
		if stage.Ready {
			out = stage
			return nil
		}
		root, err := os.OpenRoot(filepath.Join(dir, "data"))
		if err != nil {
			return err
		}
		defer root.Close()
		for i := len(stage.Manifest.Entries) - 1; i >= 0; i-- {
			if err := ctx.Err(); err != nil {
				return err
			}
			e := stage.Manifest.Entries[i]
			name := e.Path
			if name == "" {
				name = "."
			}
			if e.Kind == "symlink" {
				if err := setLinkTime(root, name, e.ModifiedNS); err != nil {
					return err
				}
				continue
			}
			if err := root.Chmod(name, os.FileMode(e.Mode)); err != nil {
				return err
			}
			stamp := time.Unix(0, e.ModifiedNS)
			if err := root.Chtimes(name, stamp, stamp); err != nil {
				return err
			}
			if e.Kind == "file" {
				f, err := root.Open(name)
				if err != nil {
					return err
				}
				err = f.Sync()
				f.Close()
				if err != nil {
					return err
				}
			}
		}
		m, err := Scan(ctx, filepath.Join(dir, "data"), "preserve", StageLimits)
		if err != nil {
			return err
		}
		if m.Digest != stage.Manifest.Digest {
			return ErrChanged
		}
		stage.Ready = true
		if err := writePrivateJSON(filepath.Join(dir, "meta.json"), stage); err != nil {
			return err
		}
		out = stage
		return nil
	})
	return out, err
}
func (s *Store) Read(ctx context.Context, owner, id string, index int, offset int64) ([]byte, error) {
	var data []byte
	err := s.WithLock(ctx, func() error {
		stage, dir, err := s.load(owner, id)
		if err != nil {
			return err
		}
		if !stage.Ready || index < 0 || index >= len(stage.Manifest.Entries) {
			return ErrStage
		}
		entry := stage.Manifest.Entries[index]
		if entry.Kind != "file" || offset < 0 || offset > entry.Size {
			return ErrStage
		}
		root, err := os.OpenRoot(filepath.Join(dir, "data"))
		if err != nil {
			return err
		}
		defer root.Close()
		f, err := openEntry(root, entry.Path, false, false)
		if err != nil {
			return err
		}
		defer f.Close()
		size := min(int64(ChunkBytes), entry.Size-offset)
		data = make([]byte, size)
		_, err = io.ReadFull(io.NewSectionReader(f, offset, size), data)
		return err
	})
	return data, err
}
func (s *Store) Remove(ctx context.Context, owner, id string) error {
	return s.WithLock(ctx, func() error {
		_, dir, err := s.load(owner, id)
		if err != nil {
			return err
		}
		return RemoveCaptured(dir)
	})
}
func (s *Store) Directory(owner, id string) (string, error) {
	stage, dir, err := s.load(owner, id)
	if err != nil || !stage.Ready {
		return "", ErrStage
	}
	return filepath.Join(dir, "data"), nil
}
func (s *Store) Get(ctx context.Context, owner, id string) (Stage, error) {
	var out Stage
	err := s.WithLock(ctx, func() error { stage, _, err := s.load(owner, id); out = stage; return err })
	return out, err
}
func (s *Store) outcomePath(owner, operationID string) (string, error) {
	if owner == "" || operationID == "" || len(operationID) > 128 || strings.ContainsAny(operationID, "/\\\x00") {
		return "", ErrStage
	}
	dir := filepath.Join(s.Root, ".outcomes")
	if err := PrivateDirectory(dir); err != nil {
		return "", err
	}
	return filepath.Join(dir, ownerHash(owner)+"-"+ownerHash(operationID)+".json"), nil
}
func (s *Store) Outcome(ctx context.Context, owner, operationID string) (Outcome, error) {
	var out Outcome
	err := s.WithLock(ctx, func() error {
		path, err := s.outcomePath(owner, operationID)
		if err != nil {
			return err
		}
		if err := readPrivateJSON(path, &out); err != nil {
			return err
		}
		if out.Owner != ownerHash(owner) || out.OperationID != operationID {
			return ErrStage
		}
		return nil
	})
	return out, err
}
func (s *Store) Execute(ctx context.Context, owner, id, destination, operationID string, plan Plan) (Outcome, error) {
	var out Outcome
	err := s.WithLock(ctx, func() error {
		if err := ValidatePlan(plan); err != nil {
			return err
		}
		path, err := s.outcomePath(owner, operationID)
		if err != nil {
			return err
		}
		if err := readPrivateJSON(path, &out); err == nil {
			return ErrRecorded
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		f, err := os.Open(filepath.Dir(path))
		if err != nil {
			return err
		}
		names, err := f.Readdirnames(maxOutcomes + 1)
		f.Close()
		if err != nil && err != io.EOF {
			return err
		}
		owned := 0
		for _, name := range names {
			if strings.HasPrefix(name, ownerHash(owner)+"-") {
				owned++
			}
		}
		if len(names) >= maxOutcomes || owned >= maxOwnerOutcomes {
			return ErrLimit
		}
		stage, dir, err := s.load(owner, id)
		if err != nil {
			return err
		}
		if !stage.Ready || stage.Manifest.Digest != plan.Source.Digest {
			return ErrChanged
		}
		out = Outcome{Owner: ownerHash(owner), OperationID: operationID, Digest: plan.Digest, State: "ambiguous"}
		if err := writePrivateJSON(path, out); err != nil {
			return err
		}
		applyErr := Apply(ctx, filepath.Join(dir, "data"), destination, plan, StageLimits)
		if applyErr == nil {
			out.State = "completed"
		} else {
			out.State = "failed"
		}
		if err := writePrivateJSON(path, out); err != nil {
			return err
		}
		return applyErr
	})
	return out, err
}

// NewID creates a plan identifier independent of all source paths and values.
func NewID() (string, error) {
	name, err := temporaryName()
	if err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(name + strconv.FormatInt(time.Now().UnixNano(), 10)))
	return hex.EncodeToString(h[:]), nil
}

// Rewrite prepares filters/path layout inside an already private retained stage.
// It is used before publishing a plan, never after approval.
func (s *Store) Rewrite(ctx context.Context, owner, id string, fn func(string) error) (Stage, error) {
	var out Stage
	err := s.WithLock(ctx, func() error {
		stage, dir, err := s.load(owner, id)
		if err != nil {
			return err
		}
		if !stage.Ready {
			return ErrStage
		}
		if err := fn(dir); err != nil {
			return err
		}
		m, err := Scan(ctx, filepath.Join(dir, "data"), "preserve", StageLimits)
		if err != nil {
			return err
		}
		stage.Manifest = m
		if err := writePrivateJSON(filepath.Join(dir, "meta.json"), stage); err != nil {
			return err
		}
		out = stage
		return nil
	})
	return out, err
}
