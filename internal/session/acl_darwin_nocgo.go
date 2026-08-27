//go:build darwin && !cgo

package session

import "fmt"

// Darwin builds without cgo cannot call acl_get_fd_np. Fail closed instead of
// falling back to a pathname query that is not bound to the no-follow fd.
func rejectConfigACL(_ int, path string) error {
	return fmt.Errorf("fd-native ACL inspection unavailable for %s in a darwin build without cgo", path)
}
