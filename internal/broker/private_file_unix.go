//go:build !windows

package broker

import (
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
)

// ReadPrivateFile refuses symlinks, non-regular objects, foreign owners and
// permissions broader than 0600. O_NONBLOCK also makes FIFO attacks fail fast.
func ReadPrivateFile(path string, limit int64) ([]byte, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0o777 != 0o600 || st.Uid != uint32(os.Getuid()) {
		return nil, fmt.Errorf("private file requires a regular file owned by the current user with mode 0600")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err == nil && int64(len(data)) > limit {
		return nil, fmt.Errorf("private file exceeds size limit")
	}
	return data, err
}

// openPrivateAppend validates the opened inode, including when a path changes
// between the initial check and open. It never follows a final symlink or blocks
// opening a FIFO.
func openPrivateAppend(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_APPEND|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		f.Close()
		return nil, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 || st.Uid != uint32(os.Getuid()) {
		f.Close()
		return nil, fmt.Errorf("audit requires a private regular file owned by the current user")
	}
	return f, nil
}
