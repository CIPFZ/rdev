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
		if err := unix.Fchmod(fd, 0o700); err != nil {
			unix.Close(fd)
			return -1, fmt.Errorf("secure config directory %s: %w", dir, err)
		}
	}
	return fd, nil
}

func readConfigFile(path string) ([]byte, error) {
	dirFD, err := openConfigDir(filepath.Dir(path), false)
	if err != nil {
		return nil, err
	}
	defer unix.Close(dirFD)

	fd, err := unix.Openat(dirFD, filepath.Base(path), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open no-follow", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("config file %s is not a regular file", path)
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

func atomicWriteConfigFile(path string, data []byte) (retErr error) {
	if len(data) > maxConfigBytes {
		return fmt.Errorf("config file %s exceeds %d bytes", path, maxConfigBytes)
	}
	dirFD, err := openConfigDir(filepath.Dir(path), true)
	if err != nil {
		return err
	}
	defer unix.Close(dirFD)

	base := filepath.Base(path)
	var existing unix.Stat_t
	if err := unix.Fstatat(dirFD, base, &existing, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		switch existing.Mode & unix.S_IFMT {
		case unix.S_IFLNK:
			return fmt.Errorf("refusing symlink config file %s", path)
		case unix.S_IFREG:
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
	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("write config temp file: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod config temp file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync config temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close config temp file: %w", err)
	}
	closed = true
	if err := unix.Renameat(dirFD, tmp, dirFD, base); err != nil {
		return fmt.Errorf("atomically replace config %s: %w", path, err)
	}
	cleanup = false
	if err := unix.Fsync(dirFD); err != nil {
		return fmt.Errorf("fsync config directory %s: %w", filepath.Dir(path), err)
	}
	return nil
}
