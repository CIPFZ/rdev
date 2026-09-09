package synctree

import "os"

func openEntry(root *os.Root, name string, directory, follow bool) (*os.File, error) {
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
	info, err := f.Stat()
	if err != nil || directory && !info.IsDir() || !directory && !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, ErrType
	}
	return f, nil
}
