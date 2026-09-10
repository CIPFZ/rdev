//go:build !windows

package main

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func tryStorageMetricsLock(state string, fn func()) bool {
	if err := secureDir(state, 0o700); err != nil {
		return false
	}
	path := filepath.Join(state, ".storage-metrics.lock")
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return false
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return false
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || !pathOwnedByCurrentUser(st) {
		return false
	}
	if st.Mode().Perm() != 0o600 && f.Chmod(0o600) != nil {
		return false
	}
	// Metrics are best effort and must never delay GC behind another agent.
	// Skipping one contested sample is preferable to blocking a user operation.
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return false
		}
		return false
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	fn()
	return true
}
