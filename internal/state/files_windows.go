package state

import (
	"encoding/json"
	"errors"
	"os"

	"github.com/CIPFZ/rdev/internal/winutil"
)

// validateRoot is deliberately read-only. The agent normally creates this
// directory before calling the state package, but exported state operations
// must not follow a caller-controlled symlink into an arbitrary tree.

func validateRoot(root string) error {
	if root == "" {
		return errors.New("state root is required")
	}
	f, err := winutil.Open(root, os.O_RDONLY, true)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if !st.IsDir() {
		return errors.New("state root is not a directory")
	}
	return nil
}
func privateRegular(path string) (os.FileInfo, error) {
	f, err := winutil.Open(path, os.O_RDONLY, true)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("state record is not a regular file")
	}
	return st, nil
}

// Windows has no unprivileged POSIX directory fsync. Record publication uses
// MOVEFILE_WRITE_THROUGH and flushed file data; this check never fabricates a
// successful FlushFileBuffers call on a directory handle.
func syncDirectory(path string) error { return winutil.ValidatePath(path) }
func writeAtomic(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return winutil.AtomicWrite(path, append(b, '\n'))
}
