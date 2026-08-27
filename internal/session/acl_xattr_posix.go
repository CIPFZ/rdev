//go:build !windows && !darwin

package session

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func rejectConfigACL(fd int, path string) error {
	n, err := unix.Flistxattr(fd, nil)
	if err != nil || n == 0 {
		return err
	}
	buf := make([]byte, n)
	n, err = unix.Flistxattr(fd, buf)
	if err != nil {
		return fmt.Errorf("inspect ACLs on %s: %w", path, err)
	}
	return validateConfigACLNames(path, strings.Split(string(buf[:n]), "\x00"))
}
