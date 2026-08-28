//go:build darwin

package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

// observeProcessExit uses EVFILT_PROC instead of Wait, so an exited leader stays
// unreaped (and its PID/PGID stays reserved) until escalation has finished.
func observeProcessExit(pid int) (<-chan struct{}, func(), error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, nil, err
	}
	var change unix.Kevent_t
	unix.SetKevent(&change, pid, unix.EVFILT_PROC, unix.EV_ADD|unix.EV_ONESHOT)
	change.Fflags = unix.NOTE_EXIT
	if _, err := unix.Kevent(kq, []unix.Kevent_t{change}, nil, nil); err != nil {
		_ = unix.Close(kq)
		return nil, nil, err
	}
	done := make(chan struct{})
	go func() {
		events := make([]unix.Kevent_t, 1)
		for {
			_, waitErr := unix.Kevent(kq, nil, events, nil)
			if !errors.Is(waitErr, unix.EINTR) {
				break
			}
		}
		close(done)
	}()
	return done, func() { _ = unix.Close(kq) }, nil
}
