package synctree

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Capture copies an observed source into a new private directory. Files are
// opened beneath the pinned source root and copied with an exact byte bound;
// the advertised content digest must match before any snapshot is returned.
// The caller owns the returned directory and must retain it until execution or
// expiry. Hard links are deliberately not used: later source writes must not
// alter the approved content.
func Capture(ctx context.Context, source, privateParent, policy string, limits Limits) (string, Manifest, error) {
	observed, err := Scan(ctx, source, policy, limits)
	if err != nil {
		return "", Manifest{}, err
	}
	abs, err := filepath.Abs(source)
	if err != nil {
		return "", Manifest{}, err
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return "", Manifest{}, err
	}
	base := abs
	if !info.IsDir() {
		base = filepath.Dir(abs)
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return "", Manifest{}, err
	}
	defer root.Close()
	if err := PrivateDirectory(privateParent); err != nil {
		return "", Manifest{}, err
	}
	dir, err := os.MkdirTemp(privateParent, "capture-")
	if err != nil {
		return "", Manifest{}, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = RemoveCaptured(dir)
		}
	}()
	dest, err := os.OpenRoot(dir)
	if err != nil {
		return "", Manifest{}, err
	}
	defer dest.Close()
	buffer := make([]byte, 64<<10)
	for _, entry := range observed.Entries {
		if err := ctx.Err(); err != nil {
			return "", Manifest{}, err
		}
		name := entry.Path
		if name == "" {
			continue
		}
		if entry.Kind == "directory" {
			if err := dest.Mkdir(name, 0700); err != nil {
				return "", Manifest{}, err
			}
			continue
		}
		if entry.Kind == "symlink" {
			if err := dest.Symlink(entry.Link, name); err != nil {
				return "", Manifest{}, err
			}
			continue
		}
		if entry.Kind != "file" {
			return "", Manifest{}, ErrType
		}
		f, err := openEntry(root, name, false, policy == "follow")
		if err != nil {
			return "", Manifest{}, err
		}
		opened, err := f.Stat()
		if err != nil || opened.Size() != entry.Size || uint32(opened.Mode()) != entry.Mode || opened.ModTime().UnixNano() != entry.ModifiedNS {
			f.Close()
			return "", Manifest{}, ErrChanged
		}
		out, err := dest.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			f.Close()
			return "", Manifest{}, err
		}
		h := sha256.New()
		// LimitReader prevents concurrent growth from consuming an unbounded stage.
		n, copyErr := io.CopyBuffer(io.MultiWriter(out, h), &contextReader{ctx: ctx, reader: io.LimitReader(f, entry.Size)}, buffer)
		var extra [1]byte
		k, endErr := f.Read(extra[:])
		after, statErr := f.Stat()
		f.Close()
		if copyErr == nil && (n != entry.Size || k != 0 || endErr != io.EOF || statErr != nil || !sameInfo(opened, after) || hex.EncodeToString(h.Sum(nil)) != entry.Digest) {
			copyErr = ErrChanged
		}
		if copyErr == nil {
			copyErr = out.Sync()
		}
		closeErr := out.Close()
		if copyErr != nil {
			return "", Manifest{}, copyErr
		}
		if closeErr != nil {
			return "", Manifest{}, closeErr
		}
	}
	// Apply directory metadata last, so read-only directories can still be filled
	// and their final times are not changed by our own child creation.
	for i := len(observed.Entries) - 1; i >= 0; i-- {
		entry := observed.Entries[i]
		if entry.Kind == "symlink" {
			if err := setLinkTime(dest, entry.Path, entry.ModifiedNS); err != nil {
				return "", Manifest{}, err
			}
			continue
		}
		name := entry.Path
		if name == "" {
			name = "."
		}
		if err := dest.Chmod(name, os.FileMode(entry.Mode)); err != nil {
			return "", Manifest{}, err
		}
		stamp := time.Unix(0, entry.ModifiedNS)
		if err := dest.Chtimes(name, stamp, stamp); err != nil {
			return "", Manifest{}, err
		}
	}
	after, err := Scan(ctx, source, policy, limits)
	if err != nil {
		return "", Manifest{}, err
	}
	if observed.Digest != after.Digest {
		return "", Manifest{}, ErrChanged
	}
	captured, err := Scan(ctx, dir, "preserve", limits)
	if err != nil {
		return "", Manifest{}, err
	}
	complete = true
	return dir, captured, nil
}

// PrivateDirectory refuses symlink, public and foreign-owned managed roots.
// It creates only the final component; callers select an already private parent.
func PrivateDirectory(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 || !ownedByCurrentUser(info) {
		return errors.New("sync state requires a private owned directory")
	}
	return nil
}

// RemoveCaptured reopens only directories under a managed private stage and
// restores owner write access before cleanup. Retained source permissions may
// intentionally be read-only; links never authorize traversal outside the stage.
func RemoveCaptured(path string) error {
	root, err := os.OpenRoot(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	visited := 0
	var writable func(*os.Root, int) error
	writable = func(dir *os.Root, depth int) error {
		visited++
		if visited > 65536 || depth > 256 {
			return ErrLimit
		}
		if err := dir.Chmod(".", 0700); err != nil {
			return err
		}
		f, err := dir.Open(".")
		if err != nil {
			return err
		}
		defer f.Close()
		for {
			names, readErr := f.Readdirnames(32)
			for _, name := range names {
				info, err := dir.Lstat(name)
				if err != nil {
					return err
				}
				if !info.IsDir() {
					continue
				}
				child, err := dir.OpenRoot(name)
				if err != nil {
					return err
				}
				err = writable(child, depth+1)
				child.Close()
				if err != nil {
					return err
				}
			}
			if readErr == io.EOF {
				return nil
			}
			if readErr != nil {
				return readErr
			}
		}
	}
	err = writable(root, 0)
	root.Close()
	if err != nil {
		return err
	}
	return os.RemoveAll(path)
}
