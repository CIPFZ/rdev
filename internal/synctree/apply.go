package synctree

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Apply executes only the paths in a prepared plan. The caller serializes
// destination operations and durably records the possibly-executed boundary
// before calling it. A later failure can leave a partially applied plan and
// must never cause an automatic replay. Files are published by atomic rename;
// directories are removed only when empty, never by a recursive delete.
func Apply(ctx context.Context, source, destination string, plan Plan, limits Limits) error {
	// Inspect binds the cleaned absolute path. Use the same spelling when
	// pinning its parent, including for directory operands ending in '/'.
	var err error
	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}
	if err := ValidatePlan(plan); err != nil {
		return err
	}
	staged, err := Scan(ctx, source, "preserve", limits)
	if err != nil {
		return err
	}
	if staged.Digest != plan.Source.Digest {
		return ErrChanged
	}
	current, err := Inspect(ctx, destination, limits)
	if err != nil {
		return err
	}
	if !sameSnapshot(current, plan.Destination) {
		return ErrChanged
	}
	sourceRoot, err := os.OpenRoot(source)
	if err != nil {
		return err
	}
	defer sourceRoot.Close()
	parent, err := os.OpenRoot(filepath.Dir(destination))
	if err != nil {
		return err
	}
	defer parent.Close()
	parentInfo, err := parent.Stat(".")
	if err != nil || fileIdentity(parentInfo) != plan.Destination.ParentID {
		return ErrChanged
	}
	if !current.Exists {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := parent.Mkdir(filepath.Base(destination), 0700); err != nil {
			return err
		}
	}
	dest, err := parent.OpenRoot(filepath.Base(destination))
	if err != nil {
		return err
	}
	defer dest.Close()
	if current.Exists {
		info, err := dest.Stat(".")
		if err != nil || fileIdentity(info) != current.RootID {
			return ErrChanged
		}
	}
	// Capture the identity of each parent before our own writes alter its mtime.
	identities := map[string]string{}
	for _, e := range current.Manifest.Entries {
		if e.Kind == "directory" {
			name := e.Path
			if name == "" {
				name = "."
			}
			info, err := dest.Lstat(name)
			if err != nil {
				return err
			}
			identities[e.Path] = fileIdentity(info)
		}
	}
	pinnedParent := func(path string) (*os.Root, string, error) {
		parentName := filepath.ToSlash(filepath.Dir(path))
		if parentName == "." {
			parentName = ""
		}
		for name := parentName; name != ""; name = filepath.ToSlash(filepath.Dir(name)) {
			if name == "." {
				break
			}
			info, err := dest.Lstat(name)
			if err != nil || !info.IsDir() || identities[name] != "" && fileIdentity(info) != identities[name] {
				return nil, "", ErrChanged
			}
		}
		openName := parentName
		if openName == "" {
			openName = "."
		}
		p, err := dest.OpenRoot(openName)
		if err != nil {
			return nil, "", err
		}
		info, err := p.Stat(".")
		if err != nil || identities[parentName] != "" && fileIdentity(info) != identities[parentName] {
			p.Close()
			return nil, "", ErrChanged
		}
		return p, filepath.Base(path), nil
	}
	changes := append([]Change(nil), plan.Changes...)
	// Remove only approved entries, deepest first. Directory replacement includes
	// all old descendants in the plan, and newly added files make Remove fail.
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path > changes[j].Path })
	for _, change := range changes {
		if change.Path == "" || change.Before == nil || change.After != nil && change.Before.Kind == change.After.Kind {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		p, name, err := pinnedParent(change.Path)
		if err != nil {
			return err
		}
		err = verifyEntry(ctx, p, name, *change.Before, change.Before.Kind == "directory")
		if err == nil && change.Before.Kind == "directory" {
			info, statErr := p.Lstat(name)
			if statErr != nil || fileIdentity(info) != identities[change.Path] {
				err = ErrChanged
			}
		}
		if err == nil {
			err = p.Remove(name)
		}
		if err == nil {
			err = syncRoot(p)
		}
		p.Close()
		if err != nil {
			return err
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	for _, change := range changes {
		if change.After == nil || change.Path == "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		p, name, err := pinnedParent(change.Path)
		if err != nil {
			return err
		}
		after := *change.After
		before := change.Before
		if before != nil && before.Kind != after.Kind {
			before = nil
		}
		if before != nil {
			err = verifyEntry(ctx, p, name, *before, before.Kind == "directory")
		} else if _, e := p.Lstat(name); !errors.Is(e, os.ErrNotExist) {
			err = ErrChanged
		}
		if err == nil {
			switch after.Kind {
			case "directory":
				if before == nil {
					err = p.Mkdir(name, 0700)
				}
				if err == nil {
					info, e := p.Lstat(name)
					err = e
					if e == nil {
						identities[change.Path] = fileIdentity(info)
					}
				}
			case "file":
				err = publishFile(ctx, sourceRoot, p, name, after)
			case "symlink":
				tmp, e := temporaryName()
				err = e
				if err == nil {
					err = p.Symlink(after.Link, tmp)
				}
				if err == nil {
					err = setLinkTime(p, tmp, after.ModifiedNS)
				}
				if err == nil {
					err = p.Rename(tmp, name)
				}
				if tmp != "" {
					_ = p.Remove(tmp)
				}
			default:
				err = ErrType
			}
		}
		if err == nil {
			err = syncRoot(p)
		}
		p.Close()
		if err != nil {
			return err
		}
	}
	// Restore only directory metadata explicitly included in the approved plan.
	for i := len(changes) - 1; i >= 0; i-- {
		if changes[i].After == nil || changes[i].After.Kind != "directory" {
			continue
		}
		entry := *changes[i].After
		if err := ctx.Err(); err != nil {
			return err
		}
		name := entry.Path
		if name == "" {
			name = "."
		}
		info, err := dest.Lstat(name)
		if err != nil || !info.IsDir() {
			return ErrChanged
		}
		if identities[entry.Path] != "" && fileIdentity(info) != identities[entry.Path] {
			return ErrChanged
		}
		if err := dest.Chmod(name, os.FileMode(entry.Mode)); err != nil {
			return err
		}
		stamp := time.Unix(0, entry.ModifiedNS)
		if err := dest.Chtimes(name, stamp, stamp); err != nil {
			return err
		}
		directory, err := dest.OpenRoot(name)
		if err != nil {
			return err
		}
		err = syncRoot(directory)
		directory.Close()
		if err != nil {
			return err
		}
	}
	if err := syncRoot(dest); err != nil {
		return err
	}
	return syncRoot(parent)
}

func sameSnapshot(a, b Snapshot) bool {
	return a.Exists == b.Exists && a.ParentID == b.ParentID && a.RootID == b.RootID && a.Identity == b.Identity && a.Manifest.Digest == b.Manifest.Digest
}
func syncRoot(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func temporaryName() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return ".rdev-sync-" + hex.EncodeToString(b[:]), nil
}
func verifyEntry(ctx context.Context, root *os.Root, name string, expected Entry, directory bool) error {
	info, err := root.Lstat(name)
	if err != nil {
		return ErrChanged
	}
	if directory {
		if !info.IsDir() {
			return ErrChanged
		}
		return nil
	}
	if uint32(info.Mode()) != expected.Mode || info.Size() != expected.Size || info.ModTime().UnixNano() != expected.ModifiedNS {
		return ErrChanged
	}
	if expected.Kind == "symlink" {
		link, err := root.Readlink(name)
		if err != nil || link != expected.Link {
			return ErrChanged
		}
		return nil
	}
	f, err := openEntry(root, name, false, false)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, &contextReader{ctx: ctx, reader: io.LimitReader(f, expected.Size+1)})
	if err != nil {
		return err
	}
	if n != expected.Size || hex.EncodeToString(h.Sum(nil)) != expected.Digest {
		return ErrChanged
	}
	return nil
}
func publishFile(ctx context.Context, source, dest *os.Root, name string, entry Entry) error {
	f, err := openEntry(source, entry.Path, false, false)
	if err != nil {
		return err
	}
	defer f.Close()
	tmp, err := temporaryName()
	if err != nil {
		return err
	}
	out, err := dest.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer dest.Remove(tmp)
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), &contextReader{ctx: ctx, reader: io.LimitReader(f, entry.Size)})
	if err == nil && (n != entry.Size || hex.EncodeToString(h.Sum(nil)) != entry.Digest) {
		err = ErrChanged
	}
	if err == nil {
		err = out.Chmod(os.FileMode(entry.Mode))
	}
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	stamp := time.Unix(0, entry.ModifiedNS)
	if err := dest.Chtimes(tmp, stamp, stamp); err != nil {
		return err
	}
	fresh, err := dest.Open(tmp)
	if err != nil {
		return err
	}
	err = fresh.Sync()
	fresh.Close()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return dest.Rename(tmp, name)
}

func ValidatePlan(plan Plan) error {
	digest, err := planDigest(plan)
	if err != nil || digest != plan.Digest {
		return errors.New("sync plan digest mismatch")
	}
	if err := ValidateManifest(plan.Source); err != nil {
		return err
	}
	before := map[string]Entry{}
	after := map[string]Entry{}
	if plan.Destination.Exists {
		if err := ValidateManifest(plan.Destination.Manifest); err != nil {
			return err
		}
	}
	for _, e := range plan.Destination.Manifest.Entries {
		before[e.Path] = e
	}
	for _, e := range plan.Source.Entries {
		after[e.Path] = e
	}
	seen := map[string]bool{}
	for _, change := range plan.Changes {
		if !validRelative(change.Path) || seen[change.Path] || change.Before == nil && change.After == nil {
			return ErrType
		}
		seen[change.Path] = true
		if change.Before != nil {
			old, ok := before[change.Path]
			if !ok || !sameEntry(old, *change.Before) {
				return fmt.Errorf("sync plan target mismatch")
			}
		}
		if change.After != nil {
			next, ok := after[change.Path]
			if !ok || !sameEntry(next, *change.After) {
				return fmt.Errorf("sync plan source mismatch")
			}
		}
		if change.Path == "" && change.After == nil {
			return errors.New("sync cannot delete its root")
		}
		if strings.Contains(change.Path, "\\") && os.PathSeparator == '\\' {
			return ErrType
		}
	}
	return nil
}
