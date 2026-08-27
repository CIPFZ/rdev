//go:build darwin && cgo

package session

/*
#include <errno.h>
#include <sys/acl.h>
#include <sys/types.h>

static int rdev_acl_fd_entry_count(int fd, int *count) {
	acl_t acl = acl_get_fd_np(fd, ACL_TYPE_EXTENDED);
	if (acl == NULL) {
		if (errno == ENOENT) {
			*count = 0;
			return 0;
		}
		return errno == 0 ? EIO : errno;
	}
	acl_entry_t entry;
	int entry_id = ACL_FIRST_ENTRY;
	int rc;
	*count = 0;
	while ((rc = acl_get_entry(acl, entry_id, &entry)) == 0) {
		(*count)++;
		entry_id = ACL_NEXT_ENTRY;
	}
	// Darwin reports EINVAL when ACL_NEXT_ENTRY reaches the end.
	int saved = (rc < 0 && errno != EINVAL) ? (errno == 0 ? EIO : errno) : 0;
	acl_free(acl);
	return saved;
}
*/
import "C"

import (
	"fmt"
	"syscall"
)

// Darwin's native NFSv4-style ACL is queried on the already-open descriptor.
// No pathname lookup occurs after the no-follow open, so replacement and
// replacement-back races cannot change the authorization object inspected.
func rejectConfigACL(fd int, path string) error {
	var count C.int
	errno := C.rdev_acl_fd_entry_count(C.int(fd), &count)
	if errno != 0 {
		return fmt.Errorf("inspect fd-native ACL on %s: %w", path, syscall.Errno(errno))
	}
	if count != 0 {
		return fmt.Errorf("security-sensitive path %s has an unsupported extended ACL", path)
	}
	return nil
}
