package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	statepkg "github.com/CIPFZ/rdev/internal/state"
	"github.com/CIPFZ/rdev/internal/winutil"
	"golang.org/x/sys/windows"
)

func secureDir(path string, _ os.FileMode) error { return winutil.EnsurePrivateDir(path) }
func validateBusinessPath(path string) error     { return winutil.ValidatePath(path) }
func replaceFile(from, to string) error          { return winutil.Replace(from, to) }
func syncDataDirectory(f *os.File) error         { return winutil.ValidatePath(f.Name()) }
func secureJobRoot(state string) (string, error) {
	p, err := filepath.Abs(filepath.Join(state, "jobs"))
	if err != nil {
		return "", err
	}
	return p, secureDir(p, 0700)
}
func secureJobDir(path string) error { return secureDir(path, 0700) }
func secureRecordFile(path string) error {
	f, err := winutil.Open(path, os.O_RDONLY, true)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err == nil && !st.Mode().IsRegular() {
		err = errors.New("job record is not a regular file")
	}
	return err
}

func readManagedFile(path string) ([]byte, error) {
	f, err := winutil.Open(path, os.O_RDONLY, true)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

func openJobLog(path string) (*os.File, error)  { return winutil.Open(path, os.O_RDONLY, true) }
func ownedPath(path string, _ os.FileInfo) bool { return winutil.CheckPrivate(path) == nil }

// Windows permission admission is the ACL check in ownedPath/secureRecordFile.
func platformPrivateMode(os.FileInfo, os.FileMode) bool { return true }
func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return winutil.AtomicWrite(path, b)
}
func syncJobDirectory(path string) error      { return winutil.CheckPrivate(path) }
func processIdentity(pid int) (string, error) { return winutil.ProcessIdentity(pid) }
func processAlive(pid int) bool               { _, err := processIdentity(pid); return err == nil }
func rlimitAvailable() bool                   { return false }
func validateFDLimit(int) error               { return errors.New("POSIX fd limits are unsupported on Windows") }
func filesystemFreeBytes(path string) int64 {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return -1
	}
	var free, total, all uint64
	if windows.GetDiskFreeSpaceEx(p, &free, &total, &all) != nil || free > uint64(^uint64(0)>>1) {
		return -1
	}
	return int64(free)
}
func configureSupervisor(cmd *exec.Cmd, _ *statepkg.Lease) error {
	identity, err := processIdentity(os.Getpid())
	if err != nil {
		return err
	}
	cmd.Env = setEnvValue(cmd.Env, "RDEV_SUPERVISOR_PARENT_IDENTITY", identity)
	cmd.Env = withoutEnvValue(cmd.Env, supervisorLeaseEnv)
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_BREAKAWAY_FROM_JOB | windows.CREATE_NEW_PROCESS_GROUP | 0x00000008}
	return nil
}
func awaitSupervisor(cmd *exec.Cmd, dir string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var ready struct {
			PID      int    `json:"pid"`
			Identity string `json:"identity"`
		}
		if err := readJSON(filepath.Join(dir, "supervisor-ready.json"), &ready); err == nil && ready.PID == cmd.Process.Pid && processMatches(ready.PID, ready.Identity) {
			return nil
		}
		if !processAlive(cmd.Process.Pid) {
			return errors.New("detached Windows supervisor exited before acquiring its state lease")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("detached Windows supervisor readiness timed out")
}
func rollbackStartedJob(cmd *exec.Cmd, dir string) error {
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	return os.RemoveAll(dir)
}
func releaseTransferLock(f *os.File) { _ = winutil.Unlock(f) }
func openTransferArtifact(path string, flags int) (*os.File, error) {
	return winutil.Open(path, flags, true)
}
func validateTransferArtifact(path string) error { return secureRecordFile(path) }
func acquireTransferLock(path string) (*os.File, error) {
	f, err := openTransferArtifact(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}
	if err = winutil.Lock(f, true, false); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func tryStorageMetricsLock(state string, fn func()) bool {
	f, err := winutil.Open(filepath.Join(state, ".storage-metrics.lock"), os.O_RDWR|os.O_CREATE, true)
	if err != nil {
		return false
	}
	defer f.Close()
	if winutil.Lock(f, true, true) != nil {
		return false
	}
	defer winutil.Unlock(f)
	fn()
	return true
}

// The name binds the private state namespace and immutable process creation
// identity. It is shared by independent agent sessions without using PID alone.
func windowsJobName(dir, identity string) string {
	return fmt.Sprintf(`Local\rdev-job-%s-%s`, filepath.Base(dir), strings.ReplaceAll(identity, ":", "-"))
}
