//go:build linux

package artifact

import (
	"errors"
	"golang.org/x/sys/unix"
)

func rejectPolicyACL(fd int, _ string) error {
	for _, name := range []string{"system.posix_acl_access", "system.posix_acl_default"} {
		n, e := unix.Fgetxattr(fd, name, nil)
		if errors.Is(e, unix.ENODATA) || errors.Is(e, unix.ENOTSUP) {
			continue
		}
		if e != nil {
			return e
		}
		if n > 0 {
			return errors.New("release trust path has an unsupported extended ACL")
		}
	}
	return nil
}
