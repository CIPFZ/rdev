//go:build darwin

package main

import (
	"fmt"
	"strconv"

	"golang.org/x/sys/unix"
)

// Darwin exposes a process start timeval through kern.proc.pid. The pair is
// serialized as seconds.microseconds and is stable for the lifetime of a PID.
func processIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid")
	}
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(kp.Proc.P_starttime.Sec, 10) + "." + strconv.FormatInt(int64(kp.Proc.P_starttime.Usec), 10), nil
}
