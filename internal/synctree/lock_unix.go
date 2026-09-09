//go:build !windows

package synctree

import (
	"context"
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"time"
)

func lockStore(ctx context.Context, path string) (func(), error) {
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	parent, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	fd, err := unix.Openat(int(parent.Fd()), filepath.Base(path), unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || !ownedByCurrentUser(info) {
		f.Close()
		return nil, ErrStage
	}
	for {
		if err := ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() { _ = unix.Flock(fd, unix.LOCK_UN); _ = f.Close() }, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func setLinkTime(root *os.Root, name string, ns int64) error {
	parent, err := root.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer parent.Close()
	stamp := unix.NsecToTimespec(ns)
	return unix.UtimesNanoAt(int(parent.Fd()), filepath.Base(name), []unix.Timespec{stamp, stamp}, unix.AT_SYMLINK_NOFOLLOW)
}
