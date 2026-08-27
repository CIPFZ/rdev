//go:build !windows

package session

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const maxConfigBytes = 1 << 20

func validateConfigFD(fd int, path string, wantDir bool) error {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return fmt.Errorf("fstat security-sensitive path %s: %w", path, err)
	}
	if err := validateConfigMetadata(st.Uid, uint32(os.Geteuid()), uint32(st.Mode), wantDir); err != nil {
		return fmt.Errorf("security-sensitive path %s: %w", path, err)
	}
	if err := rejectConfigACL(fd, path); err != nil {
		return err
	}
	return nil
}

func validateConfigMetadata(uid, effectiveUID, mode uint32, wantDir bool) error {
	wantType := uint32(unix.S_IFREG)
	if wantDir {
		wantType = uint32(unix.S_IFDIR)
	}
	if mode&uint32(unix.S_IFMT) != wantType {
		return errors.New("unexpected file type")
	}
	if uid != effectiveUID {
		return fmt.Errorf("owned by uid %d, want effective uid %d", uid, effectiveUID)
	}
	// Preserve compatibility with legacy 0755/0644 state while requiring that
	// only the effective user can modify authorization inputs.
	if mode&0o022 != 0 {
		return fmt.Errorf("writable by group or other (mode %04o)", mode&0o7777)
	}
	return nil
}

func validateConfigACLNames(path string, names []string) error {
	for _, name := range names {
		if name == "system.posix_acl_access" || name == "system.posix_acl_default" || name == "com.apple.acl.text" {
			return fmt.Errorf("security-sensitive path %s has an unsupported extended ACL", path)
		}
	}
	return nil
}

func openConfigDir(dir string, create bool) (int, error) {
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) && create {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return -1, err
		}
		info, err = os.Lstat(dir)
	}
	if err != nil {
		return -1, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return -1, fmt.Errorf("refusing symlink config directory %s", dir)
	}
	if !info.IsDir() {
		return -1, fmt.Errorf("config parent %s is not a directory", dir)
	}
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return -1, fmt.Errorf("open config directory %s without following links: %w", dir, err)
	}
	if create {
		var st unix.Stat_t
		if err := unix.Fstat(fd, &st); err != nil || uint32(st.Mode)&uint32(unix.S_IFMT) != uint32(unix.S_IFDIR) || st.Uid != uint32(os.Geteuid()) {
			unix.Close(fd)
			if err != nil {
				return -1, fmt.Errorf("inspect config directory %s: %w", dir, err)
			}
			return -1, fmt.Errorf("refusing to repair config directory %s not owned by effective uid", dir)
		}
		if err := unix.Fchmod(fd, 0o700); err != nil {
			unix.Close(fd)
			return -1, fmt.Errorf("secure config directory %s: %w", dir, err)
		}
	}
	if err := validateConfigFD(fd, dir, true); err != nil {
		unix.Close(fd)
		return -1, err
	}
	return fd, nil
}

func readConfigFile(path string) ([]byte, error) {
	dirFD, err := openConfigDir(filepath.Dir(path), false)
	if err != nil {
		return nil, err
	}
	defer unix.Close(dirFD)
	if err := unix.Flock(dirFD, unix.LOCK_SH); err != nil {
		return nil, fmt.Errorf("lock config directory %s: %w", filepath.Dir(path), err)
	}
	defer unix.Flock(dirFD, unix.LOCK_UN)

	fd, err := unix.Openat(dirFD, filepath.Base(path), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open no-follow", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	if err := validateConfigFD(fd, path, false); err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxConfigBytes {
		return nil, fmt.Errorf("config file %s exceeds %d bytes", path, maxConfigBytes)
	}
	return b, nil
}

func atomicWriteConfigFile(path string, data []byte) error {
	return atomicWriteConfigFileWithHook(path, data, nil)
}

func atomicWriteConfigFileWithHook(path string, data []byte, hook func(string) error) (retErr error) {
	fail := func(stage string) error {
		if hook == nil {
			return nil
		}
		return hook(stage)
	}
	if len(data) > maxConfigBytes {
		return fmt.Errorf("config file %s exceeds %d bytes", path, maxConfigBytes)
	}
	dirFD, err := openConfigDir(filepath.Dir(path), true)
	if err != nil {
		return err
	}
	defer unix.Close(dirFD)
	if err := unix.Flock(dirFD, unix.LOCK_EX); err != nil {
		return fmt.Errorf("lock config directory %s: %w", filepath.Dir(path), err)
	}
	defer unix.Flock(dirFD, unix.LOCK_UN)

	base := filepath.Base(path)
	var existing unix.Stat_t
	existed := false
	if err := unix.Fstatat(dirFD, base, &existing, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		existed = true
		switch existing.Mode & unix.S_IFMT {
		case unix.S_IFLNK:
			return fmt.Errorf("refusing symlink config file %s", path)
		case unix.S_IFREG:
			if existing.Uid != uint32(os.Geteuid()) {
				return fmt.Errorf("refusing config target %s not owned by effective uid", path)
			}
		default:
			return fmt.Errorf("config target %s is not a regular file", path)
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("inspect config target %s: %w", path, err)
	}

	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("name config temp file: %w", err)
	}
	tmp := "." + base + ".tmp-" + hex.EncodeToString(random[:])
	backup := tmp + ".old"
	backupExists := false
	preserveBackup := false
	if existed {
		if err := unix.Linkat(dirFD, base, dirFD, backup, 0); err != nil {
			return fmt.Errorf("preserve prior config %s: %w", path, err)
		}
		backupExists = true
		defer func() {
			if backupExists && !preserveBackup {
				_ = unix.Unlinkat(dirFD, backup, 0)
			}
		}()
		if err := unix.Fsync(dirFD); err != nil {
			return fmt.Errorf("durably preserve prior config %s: %w", path, err)
		}
	}
	tmpFD, err := unix.Openat(dirFD, tmp,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("create config temp file: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = unix.Unlinkat(dirFD, tmp, 0)
		}
	}()

	f := os.NewFile(uintptr(tmpFD), filepath.Join(filepath.Dir(path), tmp))
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	if err := fail("write"); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write config temp file: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod config temp file: %w", err)
	}
	if err := fail("file-fsync"); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync config temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close config temp file: %w", err)
	}
	closed = true
	if err := fail("rename"); err != nil {
		return err
	}
	if err := unix.Renameat(dirFD, tmp, dirFD, base); err != nil {
		return fmt.Errorf("atomically replace config %s: %w", path, err)
	}
	cleanup = false
	rollback := func(commitErr error) error {
		if backupExists {
			if err := fail("rollback-rename"); err != nil {
				preserveBackup = true
				return &ConfigWriteAmbiguousError{Cause: commitErr, Rollback: err}
			}
			if err := unix.Renameat(dirFD, backup, dirFD, base); err != nil {
				preserveBackup = true
				return &ConfigWriteAmbiguousError{Cause: commitErr, Rollback: fmt.Errorf("restore prior config: %w", err)}
			}
			backupExists = false
		} else {
			if err := fail("rollback-unlink"); err != nil {
				return &ConfigWriteAmbiguousError{Cause: commitErr, Rollback: err}
			}
			if err := unix.Unlinkat(dirFD, base, 0); err != nil {
				return &ConfigWriteAmbiguousError{Cause: commitErr, Rollback: fmt.Errorf("remove uncommitted config: %w", err)}
			}
		}
		if err := fail("rollback-fsync"); err != nil {
			return &ConfigWriteAmbiguousError{Cause: commitErr, Rollback: err}
		}
		if err := unix.Fsync(dirFD); err != nil {
			return &ConfigWriteAmbiguousError{Cause: commitErr, Rollback: fmt.Errorf("fsync rollback: %w", err)}
		}
		return commitErr
	}
	if err := fail("dir-fsync"); err != nil {
		return rollback(err)
	}
	if err := unix.Fsync(dirFD); err != nil {
		return rollback(fmt.Errorf("fsync config directory %s: %w", filepath.Dir(path), err))
	}
	// Commit point: the new file and its directory entry are now durable.
	if backupExists {
		if err := fail("backup-unlink"); err != nil {
			preserveBackup = true
			return &ConfigWriteCommittedError{Cause: err}
		}
		if err := unix.Unlinkat(dirFD, backup, 0); err != nil {
			preserveBackup = true
			return &ConfigWriteCommittedError{Cause: fmt.Errorf("remove prior config backup: %w", err)}
		}
		backupExists = false
		if err := fail("cleanup-fsync"); err != nil {
			return &ConfigWriteCommittedError{Cause: err}
		}
		if err := unix.Fsync(dirFD); err != nil {
			return &ConfigWriteCommittedError{Cause: fmt.Errorf("fsync backup cleanup: %w", err)}
		}
	}
	return nil
}
