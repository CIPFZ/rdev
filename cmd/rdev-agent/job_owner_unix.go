//go:build !windows

package main

import (
	"os"
	"syscall"
)

func pathOwnedByCurrentUser(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && uint64(st.Uid) == uint64(os.Geteuid())
}
