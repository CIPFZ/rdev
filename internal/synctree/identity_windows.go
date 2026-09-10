package synctree

import (
	"errors"
	"github.com/CIPFZ/rdev/internal/winutil"
	"os"
	"path/filepath"
	"strings"
)

type windowsInfo struct {
	os.FileInfo
	identity string
	private  bool
}

func nativeStatFile(f *os.File) (os.FileInfo, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	id, err := winutil.Identity(f)
	if err != nil {
		return nil, err
	}
	return windowsInfo{FileInfo: info, identity: id, private: winutil.CheckHandle(f, true) == nil}, nil
}
func nativeLstatPath(p string) (os.FileInfo, error) {
	f, err := winutil.Open(p, os.O_RDONLY, false)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return nativeStatFile(f)
}
func nativeStatPath(p string) (os.FileInfo, error) { return nativeLstatPath(p) }
func nativeRootStat(r *os.Root, p string, follow bool) (os.FileInfo, error) {
	if err := winutil.ValidatePath(filepath.Join(r.Name(), p)); err != nil {
		return nil, err
	}
	f, err := r.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err = winutil.CheckHandle(f, false); err != nil {
		return nil, err
	}
	return nativeStatFile(f)
}
func fileIdentity(info os.FileInfo) string {
	if i, ok := info.(windowsInfo); ok {
		return i.identity
	}
	return ""
}
func sameObject(a, b os.FileInfo) bool {
	return fileIdentity(a) != "" && fileIdentity(a) == fileIdentity(b)
}
func ownedByCurrentUser(info os.FileInfo) bool         { i, ok := info.(windowsInfo); return ok && i.private }
func privateInfo(info os.FileInfo, _ os.FileMode) bool { return ownedByCurrentUser(info) }
func PrivateDirectory(path string) error               { return winutil.EnsurePrivateDir(path) }
func syncRoot(root *os.Root) error                     { return winutil.ValidatePath(root.Name()) }

// Data was flushed through its writable handle. Windows read handles cannot
// FlushFileBuffers; namespace publication uses a write-through move below.
func syncReadableFile(f *os.File) error { return winutil.CheckHandle(f, false) }
func syncWrittenChunk(f *os.File) error { return f.Sync() }
func renameInRoot(r *os.Root, from, to string) error {
	if !filepath.IsLocal(from) || !filepath.IsLocal(to) {
		return ErrType
	}
	return winutil.Replace(filepath.Join(r.Name(), from), filepath.Join(r.Name(), to))
}

// NTFS represents time in 100 ns units; Windows exposes the read-only bit,
// not POSIX owner/group/execute bits. Content and path identity remain exact.
func platformManifestEqual(actual, want Manifest) bool {
	if actual.Policy != want.Policy || actual.ContentBytes != want.ContentBytes || len(actual.Entries) != len(want.Entries) {
		return false
	}
	for i, a := range actual.Entries {
		b := want.Entries[i]
		if a.Path != b.Path || a.Kind != b.Kind || a.Size != b.Size || a.Digest != b.Digest || a.Link != b.Link || a.ModifiedNS/100 != b.ModifiedNS/100 {
			return false
		}
		if a.Kind == "file" && (a.Mode&0200 != 0) != (b.Mode&0200 != 0) {
			return false
		}
	}
	return true
}
func validatePlatformManifest(m Manifest) error {
	seen := map[string]bool{}
	for _, e := range m.Entries {
		if e.Kind == "symlink" {
			return errors.New("Windows sync supports regular files and directories; symlink preservation is unsupported")
		}
		if strings.Contains(e.Path, `\`) {
			return errors.New("Windows sync entry uses a backslash")
		}
		if err := winutil.ValidatePath(`C:\rdev-validation\` + filepath.FromSlash(e.Path)); err != nil {
			return err
		}
		key := strings.ToUpper(e.Path)
		if seen[key] {
			return errors.New("Windows sync contains colliding case-insensitive names")
		}
		seen[key] = true
	}
	return nil
}
