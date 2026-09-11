//go:build windows

package broker

import (
	"fmt"
	"io"
	"os"
)

// Windows does not expose the Unix no-follow/openat primitives used by the
// Unix implementation. Keep the same regular-file and private-mode contract
// for the Windows client build; native broker deployment remains unsupported.
func ReadPrivateFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("private file requires a regular file with private permissions")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(data)) > limit {
		return nil, fmt.Errorf("private file exceeds size limit")
	}
	return data, err
}

func openPrivateAppend(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		f.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("audit requires a private regular file")
	}
	return f, nil
}
