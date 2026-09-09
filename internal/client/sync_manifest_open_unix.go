//go:build !windows

package client

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// openManifestFile opens a candidate regular file without following the final
// path component.  The caller still validates the entry with Lstat; O_NOFOLLOW
// closes the replacement race between that validation and this open.
func openManifestFile(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("open manifest file: %w", err)
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("manifest source is not a regular file")
	}
	return f, nil
}
