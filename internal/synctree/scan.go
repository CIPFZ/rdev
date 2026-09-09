// Package synctree builds bounded, content-complete tree observations. An
// observation can detect changes but is not an immutable copy of the source;
// execution must retain staged content and bind the destination plan separately.
package synctree

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const (
	MaxEntries             = 100000
	MaxMetadataBytes       = 16 << 20
	MaxContentBytes  int64 = 8 << 30
	maxDepth               = 256
)

var (
	ErrChanged = errors.New("sync tree changed while being scanned")
	ErrLimit   = errors.New("sync tree exceeds scan budget")
	ErrType    = errors.New("sync tree contains an unsupported file type")
)

type Entry struct {
	Path       string
	Kind       string
	Size       int64
	Mode       uint32
	ModifiedNS int64
	Digest     string
	Link       string
}

type Manifest struct {
	Entries      []Entry
	Digest       string
	ContentBytes int64
}

// Limits may lower, but cannot raise, the system caps. Zero uses the default.
type Limits struct {
	Entries       int
	MetadataBytes int
	ContentBytes  int64
}

type scanner struct {
	ctx      context.Context
	root     *os.Root
	policy   string
	limits   Limits
	manifest Manifest
	visited  int
	metadata int
	buffer   []byte
}

// Scan hashes every regular file, including large files, and checks open-file
// identities and metadata before and after reading. Directory enumeration uses
// bounded batches, so a huge directory cannot allocate an unbounded name list.
// Paths and link targets are digested as length-prefixed raw bytes; even invalid
// UTF-8 and newline-containing names remain distinct.
func Scan(ctx context.Context, path, policy string, limits Limits) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if policy == "" {
		policy = "preserve"
	}
	if policy != "preserve" && policy != "skip" && policy != "follow" {
		return Manifest{}, errors.New("invalid sync symlink policy")
	}
	if limits.Entries == 0 {
		limits.Entries = MaxEntries
	}
	if limits.MetadataBytes == 0 {
		limits.MetadataBytes = MaxMetadataBytes
	}
	if limits.ContentBytes == 0 {
		limits.ContentBytes = MaxContentBytes
	}
	if limits.Entries < 1 || limits.Entries > MaxEntries || limits.MetadataBytes < 1 || limits.MetadataBytes > MaxMetadataBytes || limits.ContentBytes < 1 || limits.ContentBytes > MaxContentBytes {
		return Manifest{}, ErrLimit
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return Manifest{}, err
	}
	before, err := os.Lstat(abs)
	if err != nil {
		return Manifest{}, err
	}
	parent, name, relative := filepath.Dir(abs), filepath.Base(abs), filepath.Base(abs)
	if before.IsDir() {
		parent, name, relative = abs, ".", ""
	}
	if before.Mode()&os.ModeSymlink != 0 && policy == "follow" {
		return Manifest{}, errors.New("sync root symlink cannot be followed")
	}
	root, err := os.OpenRoot(parent)
	if err != nil {
		return Manifest{}, err
	}
	defer root.Close()
	after, err := root.Lstat(name)
	if err != nil || !sameInfo(before, after) {
		return Manifest{}, ErrChanged
	}
	s := scanner{ctx: ctx, root: root, policy: policy, limits: limits, buffer: make([]byte, 64<<10)}
	if err := s.walk(name, relative, nil); err != nil {
		return Manifest{}, err
	}
	after, err = os.Lstat(abs)
	if err != nil || !sameInfo(before, after) {
		return Manifest{}, ErrChanged
	}
	sort.Slice(s.manifest.Entries, func(i, j int) bool { return s.manifest.Entries[i].Path < s.manifest.Entries[j].Path })
	h := sha256.New()
	writeField(h, "rdev.sync.manifest.v2")
	writeField(h, policy)
	for _, entry := range s.manifest.Entries {
		if err := ctx.Err(); err != nil {
			return Manifest{}, err
		}
		digestEntry(h, entry)
	}
	s.manifest.Digest = hex.EncodeToString(h.Sum(nil))
	return s.manifest, nil
}

func (s *scanner) walk(name, relative string, ancestors []os.FileInfo) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}
	s.visited++
	if s.visited > s.limits.Entries || len(ancestors) > maxDepth {
		return ErrLimit
	}
	before, err := s.root.Lstat(name)
	if err != nil {
		return err
	}
	info := before
	link := ""
	if before.Mode()&os.ModeSymlink != 0 {
		if s.policy == "skip" {
			return nil
		}
		link, err = s.root.Readlink(name)
		if err != nil {
			return err
		}
		if s.policy == "follow" {
			// Root resolves links against the pinned directory and rejects every
			// escape, including a changed intermediate path component.
			info, err = s.root.Stat(name)
			if err != nil {
				return err
			}
		}
	}
	entry := Entry{Path: relative, Size: info.Size(), Mode: uint32(info.Mode()), ModifiedNS: info.ModTime().UnixNano()}
	s.metadata += len(relative) + len(link) + 160
	if s.metadata > s.limits.MetadataBytes {
		return ErrLimit
	}
	switch {
	case info.IsDir():
		entry.Kind, entry.Size = "directory", 0
		for _, ancestor := range ancestors {
			if os.SameFile(ancestor, info) {
				return errors.New("sync tree contains a symlink cycle")
			}
		}
		f, err := openEntry(s.root, name, true, s.policy == "follow")
		if err != nil {
			return err
		}
		defer f.Close()
		opened, err := f.Stat()
		if err != nil || !sameInfo(info, opened) {
			return ErrChanged
		}
		ancestors = append(ancestors, info)
		for {
			if err := s.ctx.Err(); err != nil {
				return err
			}
			names, readErr := f.Readdirnames(16)
			for _, child := range names {
				if err := s.walk(filepath.Join(name, child), filepath.ToSlash(filepath.Join(relative, child)), ancestors); err != nil {
					return err
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
		end, err := f.Stat()
		if err != nil || !sameInfo(info, end) {
			return ErrChanged
		}
	case info.Mode().IsRegular():
		entry.Kind = "file"
		if info.Size() < 0 || info.Size() > s.limits.ContentBytes-s.manifest.ContentBytes {
			return ErrLimit
		}
		f, err := openEntry(s.root, name, false, s.policy == "follow")
		if err != nil {
			return err
		}
		opened, err := f.Stat()
		if err != nil || !sameInfo(info, opened) {
			_ = f.Close()
			return ErrChanged
		}
		h := sha256.New()
		// One excess byte detects growth without permitting an active writer
		// to keep hashing beyond the declared content budget indefinitely.
		n, readErr := io.CopyBuffer(h, &contextReader{ctx: s.ctx, reader: io.LimitReader(f, info.Size()+1)}, s.buffer)
		end, statErr := f.Stat()
		closeErr := f.Close()
		if readErr != nil {
			return readErr
		}
		if statErr != nil || n != info.Size() || !sameInfo(info, end) {
			return ErrChanged
		}
		if closeErr != nil {
			return closeErr
		}
		entry.Digest = hex.EncodeToString(h.Sum(nil))
		s.manifest.ContentBytes += n
	case info.Mode()&os.ModeSymlink != 0:
		entry.Kind, entry.Link = "symlink", link
	default:
		return ErrType
	}
	after, err := s.root.Lstat(name)
	if err != nil || !sameInfo(before, after) {
		return ErrChanged
	}
	if s.policy == "follow" && before.Mode()&os.ModeSymlink != 0 {
		end, err := s.root.Stat(name)
		if err != nil || !sameInfo(info, end) {
			return ErrChanged
		}
	}
	s.manifest.Entries = append(s.manifest.Entries, entry)
	return nil
}

func sameInfo(a, b os.FileInfo) bool {
	return a != nil && b != nil && os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

func writeField(h hash.Hash, value string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(value)))
	_, _ = h.Write(n[:])
	_, _ = h.Write([]byte(value))
}

func digestEntry(h hash.Hash, e Entry) {
	writeField(h, e.Path)
	writeField(h, e.Kind)
	var n [24]byte
	binary.BigEndian.PutUint64(n[0:8], uint64(e.Size))
	binary.BigEndian.PutUint64(n[8:16], uint64(e.Mode))
	binary.BigEndian.PutUint64(n[16:24], uint64(e.ModifiedNS))
	_, _ = h.Write(n[:])
	writeField(h, e.Digest)
	writeField(h, e.Link)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
