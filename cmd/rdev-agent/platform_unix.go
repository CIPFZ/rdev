//go:build !windows

package main

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"

	statepkg "github.com/CIPFZ/rdev/internal/state"
)

func configureSupervisor(cmd *exec.Cmd, lease *statepkg.Lease) error {
	cmd.ExtraFiles = []*os.File{lease.File()}
	cmd.Env = setEnvValue(cmd.Env, supervisorLeaseEnv, "3")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return nil
}
func validateFDLimit(n int) error {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil || uint64(n) > lim.Max {
		return fmt.Errorf("requested fd limit exceeds enforceable hard limit")
	}
	return nil
}
func releaseTransferLock(f *os.File)                              { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
func awaitSupervisor(*exec.Cmd, string) error                     { return nil }
func ownedPath(_ string, info os.FileInfo) bool                   { return pathOwnedByCurrentUser(info) }
func platformPrivateMode(info os.FileInfo, mode os.FileMode) bool { return info.Mode().Perm() == mode }
func validateBusinessPath(string) error                           { return nil }
func replaceFile(from, to string) error                           { return os.Rename(from, to) }
func syncDataDirectory(f *os.File) error                          { return f.Sync() }
func readManagedFile(path string) ([]byte, error)                 { return os.ReadFile(path) }
func openJobLog(path string) (*os.File, error)                    { return os.Open(path) }
