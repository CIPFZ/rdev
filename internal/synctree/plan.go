package synctree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Snapshot binds semantic contents and filesystem identities. Identity is kept
// separate from Manifest.Digest because captured files have different inodes.
type Snapshot struct {
	Exists   bool     `json:"exists"`
	ParentID string   `json:"parent_id"`
	RootID   string   `json:"root_id"`
	Identity string   `json:"identity"`
	Manifest Manifest `json:"manifest"`
}

type Change struct {
	Path   string `json:"path"`
	Before *Entry `json:"before,omitempty"`
	After  *Entry `json:"after,omitempty"`
}

type Plan struct {
	Source      Manifest `json:"source"`
	Destination Snapshot `json:"destination"`
	Changes     []Change `json:"changes"`
	Digest      string   `json:"digest"`
}

func Inspect(ctx context.Context, path string, limits Limits) (Snapshot, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return Snapshot{}, err
	}
	parent, err := os.Stat(filepath.Dir(abs))
	if err != nil {
		return Snapshot{}, err
	}
	snap := Snapshot{ParentID: fileIdentity(parent)}
	if snap.ParentID == "" {
		return Snapshot{}, errors.New("sync filesystem identity unsupported")
	}
	info, err := os.Lstat(abs)
	if errors.Is(err, os.ErrNotExist) {
		return snap, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	// A destination root may not redirect writes through a link.
	if info.Mode()&os.ModeSymlink != 0 {
		return Snapshot{}, ErrType
	}
	snap.RootID = fileIdentity(info)
	snap.Exists = true
	snap.Manifest, err = Scan(ctx, abs, "preserve", limits)
	if err != nil {
		return Snapshot{}, err
	}
	h := sha256.New()
	writeField(h, snap.RootID)
	base := abs
	if !info.IsDir() {
		base = filepath.Dir(abs)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return Snapshot{}, err
	}
	defer root.Close()
	for _, entry := range snap.Manifest.Entries {
		if err := ctx.Err(); err != nil {
			return Snapshot{}, err
		}
		name := filepath.FromSlash(entry.Path)
		if name == "" {
			name = "."
		}
		if !info.IsDir() {
			name = filepath.Base(abs)
		}
		current, err := root.Lstat(name)
		if err != nil {
			return Snapshot{}, err
		}
		writeField(h, entry.Path)
		writeField(h, fileIdentity(current))
	}
	end, err := os.Lstat(abs)
	if err != nil || !sameInfo(info, end) {
		return Snapshot{}, ErrChanged
	}
	snap.Identity = hex.EncodeToString(h.Sum(nil))
	return snap, nil
}

// Build fixes all creates, replacements and removals before approval. It never
// recomputes a deletion set during execution. Source is an already retained tree.
func Build(source Manifest, dest Snapshot, deleteExtra bool, conflict string) (Plan, error) {
	var deletions map[string]bool
	if deleteExtra {
		deletions = map[string]bool{}
		for _, e := range dest.Manifest.Entries {
			deletions[e.Path] = true
		}
	}
	return BuildScoped(source, dest, deletions, conflict)
}

// BuildScoped accepts only the deletion paths selected by the retained-source
// rsync filter/layout pass. Excluded destination entries and unrelated siblings
// are never inferred to be deletions just because they are absent from source.
func BuildScoped(source Manifest, dest Snapshot, deletions map[string]bool, conflict string) (Plan, error) {
	if err := ValidateManifest(source); err != nil {
		return Plan{}, err
	}
	if dest.Exists {
		if err := ValidateManifest(dest.Manifest); err != nil {
			return Plan{}, err
		}
	}
	if conflict == "" {
		conflict = "overwrite"
	}
	if conflict != "overwrite" && conflict != "skip" && conflict != "fail" {
		return Plan{}, ErrType
	}
	if len(source.Entries) == 0 || source.Entries[0].Path != "" || source.Entries[0].Kind != "directory" {
		return Plan{}, errors.New("sync source must be a retained directory")
	}
	if dest.Exists && (len(dest.Manifest.Entries) == 0 || dest.Manifest.Entries[0].Path != "" || dest.Manifest.Entries[0].Kind != "directory") {
		return Plan{}, errors.New("sync destination must be a directory")
	}
	before := make(map[string]Entry, len(dest.Manifest.Entries))
	after := make(map[string]Entry, len(source.Entries))
	for _, e := range dest.Manifest.Entries {
		before[e.Path] = e
	}
	for _, e := range source.Entries {
		after[e.Path] = e
	}
	p := Plan{Source: source, Destination: dest}
	skipped := map[string]bool{}
	for _, e := range source.Entries {
		blocked := false
		for parent := filepath.ToSlash(filepath.Dir(e.Path)); parent != "."; parent = filepath.ToSlash(filepath.Dir(parent)) {
			if skipped[parent] {
				blocked = true
				break
			}
		}
		if blocked {
			skipped[e.Path] = true
			continue
		}
		old, exists := before[e.Path]
		if exists && sameEntry(old, e) {
			continue
		}
		if exists && old.Kind != "directory" && conflict == "skip" {
			skipped[e.Path] = true
			continue
		}
		if exists && old.Kind != "directory" && conflict == "fail" {
			return Plan{}, errors.New("sync destination conflict")
		}
		if exists && old.Kind == "directory" && e.Kind != "directory" {
			// Replacing a directory requires authorization for every descendant even
			// without --delete. Conflicting skipped descendants cannot be discarded.
			if conflict == "skip" {
				skipped[e.Path] = true
				continue
			}
			if conflict != "overwrite" {
				return Plan{}, errors.New("sync directory conflict")
			}
			for _, child := range dest.Manifest.Entries {
				if strings.HasPrefix(child.Path, e.Path+"/") {
					copy := child
					p.Changes = append(p.Changes, Change{Path: child.Path, Before: &copy})
				}
			}
		}
		next := e
		change := Change{Path: e.Path, After: &next}
		if exists {
			copy := old
			change.Before = &copy
		}
		p.Changes = append(p.Changes, change)
	}
	if len(deletions) > 0 {
		for _, e := range dest.Manifest.Entries {
			if !deletions[e.Path] {
				continue
			}
			if _, ok := after[e.Path]; ok {
				continue
			}
			// A directory replacement above already includes its descendants.
			covered := false
			for parent := filepath.ToSlash(filepath.Dir(e.Path)); parent != "."; parent = filepath.ToSlash(filepath.Dir(parent)) {
				if sourceEntry, ok := after[parent]; ok && sourceEntry.Kind != "directory" {
					covered = true
					break
				}
			}
			if !covered {
				copy := e
				p.Changes = append(p.Changes, Change{Path: e.Path, Before: &copy})
			}
		}
	}
	// Child writes change directory timestamps. Include their source ancestors
	// explicitly so restoration is part of the reviewed plan as well.
	changed := map[string]bool{}
	for _, c := range p.Changes {
		changed[c.Path] = true
	}
	for _, c := range append([]Change(nil), p.Changes...) {
		if c.Path == "" {
			continue
		}
		for parent := filepath.ToSlash(filepath.Dir(c.Path)); ; parent = filepath.ToSlash(filepath.Dir(parent)) {
			if parent == "." {
				parent = ""
			}
			if next, ok := after[parent]; ok && next.Kind == "directory" && !changed[parent] && !skipped[parent] {
				copy := next
				change := Change{Path: parent, After: &copy}
				if old, ok := before[parent]; ok {
					copy := old
					change.Before = &copy
				}
				p.Changes = append(p.Changes, change)
				changed[parent] = true
			}
			if parent == "" {
				break
			}
		}
	}
	sort.Slice(p.Changes, func(i, j int) bool { return p.Changes[i].Path < p.Changes[j].Path })
	digest, err := planDigest(p)
	p.Digest = digest
	return p, err
}

func planDigest(p Plan) (string, error) {
	p.Digest = ""
	data, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:]), nil
}
func sameEntry(a, b Entry) bool {
	return a.Path == b.Path && a.Kind == b.Kind && a.Size == b.Size && a.Mode == b.Mode && a.ModifiedNS == b.ModifiedNS && a.Digest == b.Digest && a.Link == b.Link
}
func validRelative(path string) bool {
	return path == "" || filepath.IsLocal(path) && filepath.ToSlash(filepath.Clean(path)) == path && !strings.ContainsRune(path, 0)
}
func ValidateManifest(m Manifest) error {
	if len(m.Entries) == 0 || len(m.Entries) > 8192 || m.ContentBytes < 0 || m.ContentBytes > MaxContentBytes {
		return ErrLimit
	}
	bytes, metadata := int64(0), 0
	previous := ""
	kinds := map[string]string{}
	h := sha256.New()
	writeField(h, "rdev.sync.manifest.v2")
	writeField(h, m.Policy)
	if m.Policy != "preserve" && m.Policy != "follow" && m.Policy != "skip" {
		return ErrType
	}
	for i, e := range m.Entries {
		if !validRelative(e.Path) || i > 0 && e.Path <= previous {
			return ErrType
		}
		previous = e.Path
		if e.Path != "" {
			parent := filepath.ToSlash(filepath.Dir(e.Path))
			if parent == "." {
				parent = ""
			}
			// A single scanned file has no wrapper; all tree descendants must
			// have an explicitly declared directory parent.
			if kind, ok := kinds[parent]; (!ok && !(i == 0 && parent == "" && len(m.Entries) == 1)) || ok && kind != "directory" {
				return ErrType
			}
		}
		kinds[e.Path] = e.Kind
		digestEntry(h, e)
		metadata += len(e.Path) + len(e.Link) + 160
		if metadata > 2<<20 {
			return ErrLimit
		}
		switch e.Kind {
		case "file":
			if e.Size < 0 || e.Size > MaxContentBytes-bytes || len(e.Digest) != 64 || !os.FileMode(e.Mode).IsRegular() || e.Link != "" {
				return ErrType
			}
			if _, err := hex.DecodeString(e.Digest); err != nil || strings.ToLower(e.Digest) != e.Digest {
				return ErrType
			}
			bytes += e.Size
		case "directory":
			if e.Size != 0 || os.FileMode(e.Mode).Type() != os.ModeDir || e.Digest != "" || e.Link != "" {
				return ErrType
			}
		case "symlink":
			if e.Link == "" || strings.ContainsRune(e.Link, 0) || e.Size != int64(len(e.Link)) || os.FileMode(e.Mode).Type() != os.ModeSymlink || e.Digest != "" {
				return ErrType
			}
		default:
			return ErrType
		}
	}
	if hex.EncodeToString(h.Sum(nil)) != m.Digest {
		return ErrChanged
	}
	if bytes != m.ContentBytes || bytes > MaxContentBytes {
		return ErrLimit
	}
	return nil
}
