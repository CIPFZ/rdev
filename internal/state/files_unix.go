//go:build !windows

package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// validateRoot is deliberately read-only. The agent normally creates this
// directory before calling the state package, but exported state operations
// must not follow a caller-controlled symlink into an arbitrary tree.

func validateRoot(root string) error {
	if root == "" {
		return errors.New("state root is required")
	}
	st, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
		return errors.New("state root is not a directory")
	}
	var native unix.Stat_t
	if err := unix.Lstat(root, &native); err != nil {
		return err
	}
	if st.Mode().Perm() != 0700 || int(native.Uid) != os.Geteuid() {
		return errors.New("state root is not private and owned")
	}
	return nil
}

func privateRegular(path string) (os.FileInfo, error) {
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", filepath.Base(path))
	}
	if st.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s is not private", filepath.Base(path))
	}
	return st, nil
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeAtomic(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".rdev-state-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
