//go:build windows

package session

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const maxConfigBytes = 1 << 20

// Windows is not a supported local runtime. These implementations preserve build
// compatibility and fail closed on visible links; Tier 1 no-follow guarantees are
// provided by the POSIX openat implementation.
func rejectVisibleLink(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing symlink %s", path)
	}
	return nil
}

func readConfigFile(path string) ([]byte, error) {
	if err := rejectVisibleLink(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if err := rejectVisibleLink(path); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if len(b) > maxConfigBytes {
		return nil, fmt.Errorf("config file %s exceeds %d bytes", path, maxConfigBytes)
	}
	return b, err
}

func atomicWriteConfigFile(path string, data []byte) error {
	return atomicWriteConfigFileWithHook(path, data, nil)
}

func atomicWriteConfigFileWithHook(path string, data []byte, hook func(string) error) error {
	if len(data) > maxConfigBytes {
		return fmt.Errorf("config file %s exceeds %d bytes", path, maxConfigBytes)
	}
	if err := rejectVisibleLink(filepath.Dir(path)); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := rejectVisibleLink(path); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".hosts.json.tmp-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return err
	}
	if hook != nil {
		if err := hook("write"); err != nil {
			f.Close()
			return err
		}
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if hook != nil {
		if err := hook("file-fsync"); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if hook != nil {
		if err := hook("rename"); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if hook != nil {
		return hook("dir-fsync")
	}
	return nil
}
