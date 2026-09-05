//go:build !windows

// Package broker contains the local rdevd IPC boundary.
package broker

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Listener owns a private Unix socket and a process lock.
type Listener struct {
	net.Listener
	lock *os.File
	path string
}

// Listen creates a 0600 socket and refuses a second live broker.
func Listen(path string) (*Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("broker already running: %w", err)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		lock.Close()
		return nil, err
	}
	if err = os.Chmod(path, 0o600); err != nil {
		ln.Close()
		lock.Close()
		return nil, err
	}
	return &Listener{Listener: ln, lock: lock, path: path}, nil
}

func (l *Listener) Close() error {
	err := l.Listener.Close()
	_ = os.Remove(l.path)
	_ = unix.Flock(int(l.lock.Fd()), unix.LOCK_UN)
	if closeErr := l.lock.Close(); err == nil {
		err = closeErr
	}
	return err
}
