//go:build !windows

package synctree

import "os"

func nativeStatFile(f *os.File) (os.FileInfo, error) { return f.Stat() }
func nativeLstatPath(p string) (os.FileInfo, error)  { return os.Lstat(p) }
func nativeStatPath(p string) (os.FileInfo, error)   { return os.Stat(p) }
func nativeRootStat(r *os.Root, p string, follow bool) (os.FileInfo, error) {
	if follow {
		return r.Stat(p)
	}
	return r.Lstat(p)
}
func sameObject(a, b os.FileInfo) bool { return os.SameFile(a, b) }
func privateInfo(info os.FileInfo, mode os.FileMode) bool {
	return info.Mode().Perm() == mode && ownedByCurrentUser(info)
}
func platformManifestEqual(a, b Manifest) bool       { return a.Digest == b.Digest }
func validatePlatformManifest(Manifest) error        { return nil }
func syncReadableFile(f *os.File) error              { return f.Sync() }
func syncWrittenChunk(*os.File) error                { return nil }
func renameInRoot(r *os.Root, from, to string) error { return r.Rename(from, to) }
