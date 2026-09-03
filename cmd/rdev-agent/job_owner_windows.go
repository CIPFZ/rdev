//go:build windows

package main

import "os"

// Windows ACLs do not expose a portable numeric uid through os.FileInfo. The
// agent's supported targets are Unix; retain a conservative compatibility hook
// for cross-compilation.
func pathOwnedByCurrentUser(os.FileInfo) bool { return true }
