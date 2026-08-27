//go:build darwin

package session

import (
	"fmt"
	"os/exec"
	"strings"

	"golang.org/x/sys/unix"
)

// Darwin exposes native NFSv4-style ACLs separately from xattrs. The standard
// library has no fd ACL API, so bind the system ACL query to the already-open
// object by checking device/inode before and after it.
func rejectConfigACL(fd int, path string) error {
	var opened, before, after unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		return fmt.Errorf("fstat ACL target %s: %w", path, err)
	}
	if err := unix.Lstat(path, &before); err != nil {
		return fmt.Errorf("inspect ACL target %s: %w", path, err)
	}
	if opened.Dev != before.Dev || opened.Ino != before.Ino || before.Mode&unix.S_IFMT == unix.S_IFLNK {
		return fmt.Errorf("ACL target %s changed after no-follow open", path)
	}
	out, err := exec.Command("/bin/ls", "-lde", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("inspect ACL on %s: %w", path, err)
	}
	if err := unix.Lstat(path, &after); err != nil || opened.Dev != after.Dev || opened.Ino != after.Ino {
		return fmt.Errorf("ACL target %s changed during inspection", path)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines[1:] {
		entry := strings.TrimSpace(line)
		if index, _, ok := strings.Cut(entry, ":"); ok && index != "" {
			numeric := true
			for _, r := range index {
				if r < '0' || r > '9' {
					numeric = false
					break
				}
			}
			if numeric {
				return fmt.Errorf("security-sensitive path %s has an unsupported extended ACL", path)
			}
		}
	}
	return nil
}
