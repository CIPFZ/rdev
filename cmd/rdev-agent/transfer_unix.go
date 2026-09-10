//go:build !windows

package main

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func acquireTransferLock(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, errors.New("failed to create transfer lock")
	}
	st, statErr := f.Stat()
	if statErr != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		_ = f.Close()
		if statErr != nil {
			return nil, statErr
		}
		return nil, errors.New("transfer lock is not a private regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func validateTransferArtifact(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return errors.New("transfer artifact is not private regular file")
	}
	return nil
}

func openTransferArtifact(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, errors.New("failed to open transfer artifact")
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		_ = f.Close()
		return nil, errors.New("transfer artifact is not a private regular file")
	}
	return f, nil
}
