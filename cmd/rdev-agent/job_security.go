package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// secureDir creates or repairs a private directory and rejects ownership that
// would let another account substitute job records.
func secureDir(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	if !pathOwnedByCurrentUser(st) {
		return fmt.Errorf("%s is not owned by the current user", path)
	}
	if st.Mode().Perm() != mode.Perm() {
		if err := os.Chmod(path, mode.Perm()); err != nil {
			return err
		}
	}
	return nil
}

func secureJobRoot(state string) (string, error) {
	root, err := filepath.Abs(filepath.Join(state, "jobs"))
	if err != nil {
		return "", err
	}
	if err := secureDir(root, 0o700); err != nil {
		return "", err
	}
	return root, nil
}

func secureJobDir(dir string) error { return secureDir(dir, 0o700) }

// secureRecordFile rejects symlinks/foreign owners and repairs broad modes.
func secureRecordFile(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	if !pathOwnedByCurrentUser(st) {
		return fmt.Errorf("%s is not owned by the current user", path)
	}
	if st.Mode().Perm() != 0o600 {
		if err := os.Chmod(path, 0o600); err != nil {
			return err
		}
	}
	return nil
}
