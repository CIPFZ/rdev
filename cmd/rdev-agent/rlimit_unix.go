//go:build !windows

package main

import (
	"syscall"
)

func rlimitAvailable() bool {
	var limit syscall.Rlimit
	return syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit) == nil
}
