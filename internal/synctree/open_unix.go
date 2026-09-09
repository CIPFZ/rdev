//go:build !windows

package synctree

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func openEntry(root *os.Root, name string, directory, follow bool) (*os.File, error) {
	flags := os.O_RDONLY | unix.O_NONBLOCK | unix.O_CLOEXEC
	if directory {
		flags |= unix.O_DIRECTORY
	}
	var f *os.File
	var err error
	if follow {
		f, err = root.OpenFile(name, flags, 0)
	} else {
		// Root resolves symlinks itself, even when OpenFile receives
		// O_NOFOLLOW. Resolve and pin the confined parent first, then let
		// openat enforce no-follow on the final component in the kernel.
		parent, parentErr := root.OpenFile(filepath.Dir(name), os.O_RDONLY|unix.O_DIRECTORY, 0)
		if parentErr != nil {
			return nil, parentErr
		}
		fd, openErr := unix.Openat(int(parent.Fd()), filepath.Base(name), flags|unix.O_NOFOLLOW, 0)
		_ = parent.Close()
		err = openErr
		if err == nil {
			f = os.NewFile(uintptr(fd), name)
		}
	}
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || directory && !info.IsDir() || !directory && !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, ErrType
	}
	return f, nil
}
