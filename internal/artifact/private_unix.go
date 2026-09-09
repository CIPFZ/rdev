//go:build linux || darwin

package artifact

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"syscall"
)

func checkPrivatePath(path string) error {
	for p := path; ; p = filepath.Dir(p) {
		fd, e := unix.Open(p, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if e != nil {
			return e
		}
		f := os.NewFile(uintptr(fd), p)
		st, e := f.Stat()
		if e != nil {
			f.Close()
			return e
		}
		if e = rejectPolicyACL(fd, p); e != nil {
			f.Close()
			return e
		}
		f.Close()
		if st.Mode()&os.ModeSymlink != 0 {
			return errors.New("release policy path contains symlink")
		}
		stat, ok := st.Sys().(*syscall.Stat_t)
		if !ok {
			return errors.New("cannot determine policy owner")
		}
		if int(stat.Uid) != os.Getuid() && stat.Uid != 0 {
			return errors.New("release policy path has untrusted owner")
		}
		if p == path {
			if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
				return errors.New("release policy must be a private regular file")
			}
		} else {
			if !st.IsDir() {
				return errors.New("policy parent is not directory")
			}
			if st.Mode().Perm()&0022 != 0 && !(st.Mode()&os.ModeSticky != 0 && stat.Uid == 0) {
				return errors.New("release policy parent is writable by others")
			}
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	return nil
}

func readPolicyFile(path string) ([]byte, error) {
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var st unix.Stat_t
	if e = unix.Fstat(fd, &st); e != nil {
		return nil, e
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 || (st.Uid != uint32(os.Getuid()) && st.Uid != 0) {
		return nil, errors.New("release policy descriptor is not private and owned")
	}
	if e = rejectPolicyACL(fd, path); e != nil {
		return nil, e
	}
	return readBounded(f, MaxDocumentBytes)
}

func openArtifact(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("artifact is not a regular file")
	}
	return f, nil
}
