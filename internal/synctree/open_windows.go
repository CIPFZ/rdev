package synctree

import (
	"github.com/CIPFZ/rdev/internal/winutil"
	"os"
	"path/filepath"
)

func openEntry(root *os.Root, name string, directory, follow bool) (*os.File, error) {
	if err := winutil.ValidatePath(filepath.Join(root.Name(), name)); err != nil {
		return nil, err
	}
	if !follow {
		info, err := root.Lstat(name)
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrType
		}
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	if err := winutil.CheckHandle(f, false); err != nil {
		f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || directory && !info.IsDir() || !directory && !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, ErrType
	}
	return f, nil
}
