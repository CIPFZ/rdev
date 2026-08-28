//go:build linux

package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

// observeProcessExit reports child exit without reaping it. WNOWAIT deliberately
// keeps the leader PID/PGID reserved until group escalation is complete, so the
// numeric group identity cannot be reused underneath a delayed SIGKILL.
func observeProcessExit(pid int) (<-chan struct{}, func(), error) {
	done := make(chan struct{})
	go func() {
		for {
			var info unix.Siginfo
			err := unix.Waitid(unix.P_PID, pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
			if !errors.Is(err, unix.EINTR) {
				break
			}
		}
		close(done)
	}()
	return done, func() {}, nil
}
