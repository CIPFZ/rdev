//go:build !windows

// Package broker contains the local rdevd IPC boundary.
package broker

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

func ValidateSocket(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.Mode().Perm() != 0o600 {
		return fmt.Errorf("broker socket mode must be 0600, got %o", st.Mode().Perm())
	}
	if info, ok := st.Sys().(*syscall.Stat_t); ok && uint32(info.Uid) != uint32(os.Getuid()) {
		return errors.New("broker socket owner mismatch")
	}
	return nil
}

// Listener owns a private Unix socket and a process lock.
type Listener struct {
	net.Listener
	lock      *os.File
	path      string
	stopOnce  sync.Once
	closeOnce sync.Once
	stopErr   error
	closeErr  error
}

// Listen creates a 0600 socket and refuses a second live broker.
func Listen(path string) (*Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	st, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode().Perm() != 0o700 {
		return nil, errors.New("broker socket parent must be a private 0700 directory")
	}
	if info, ok := st.Sys().(*syscall.Stat_t); !ok || uint32(info.Uid) != uint32(os.Getuid()) {
		return nil, errors.New("broker socket parent owner mismatch")
	}
	fd, err := unix.Open(path+".lock", unix.O_CREAT|unix.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return nil, err
	}
	lock := os.NewFile(uintptr(fd), path+".lock")
	var lockStat unix.Stat_t
	if err := unix.Fstat(fd, &lockStat); err != nil {
		lock.Close()
		return nil, err
	}
	if lockStat.Mode&unix.S_IFMT != unix.S_IFREG || lockStat.Mode&0o777 != 0o600 || lockStat.Uid != uint32(os.Getuid()) {
		lock.Close()
		return nil, errors.New("invalid broker lock ownership, type or mode")
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("broker already running: %w", err)
	}
	if st, err := os.Lstat(path); err == nil {
		if st.Mode()&os.ModeSocket == 0 {
			lock.Close()
			return nil, errors.New("refusing to remove non-socket broker path")
		}
		if err := os.Remove(path); err != nil {
			lock.Close()
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		lock.Close()
		return nil, err
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		lock.Close()
		return nil, err
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if err = os.Chmod(path, 0o600); err != nil {
		ln.Close()
		lock.Close()
		return nil, err
	}
	if err = ValidateSocket(path); err != nil {
		ln.Close()
		lock.Close()
		return nil, err
	}
	return &Listener{Listener: ln, lock: lock, path: path}, nil
}

func (l *Listener) StopAccepting() error {
	l.stopOnce.Do(func() { l.stopErr = l.Listener.Close() })
	return l.stopErr
}

func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		l.closeErr = l.StopAccepting()
		_ = os.Remove(l.path)
		_ = unix.Flock(int(l.lock.Fd()), unix.LOCK_UN)
		if err := l.lock.Close(); l.closeErr == nil {
			l.closeErr = err
		}
	})
	return l.closeErr
}
