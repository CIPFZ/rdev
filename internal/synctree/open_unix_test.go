//go:build !windows

package synctree

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestOpenRejectsFIFOAndSymlinkSubstitution(t *testing.T) {
	dir := t.TempDir()
	if err := unix.Mkfifo(filepath.Join(dir, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	done := make(chan error, 1)
	go func() {
		f, err := openEntry(root, "fifo", false, false)
		if f != nil {
			f.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted")
		}
	case <-time.After(time.Second):
		// Unblock an accidentally blocking implementation before failing.
		fd, _ := unix.Open(filepath.Join(dir, "fifo"), unix.O_RDWR|unix.O_NONBLOCK, 0)
		if fd >= 0 {
			defer unix.Close(fd)
		}
		t.Fatal("FIFO open blocked")
	}
	outside := t.TempDir()
	write(t, filepath.Join(outside, "file"), "outside")
	if err := os.Symlink(outside, filepath.Join(dir, "parent")); err != nil {
		t.Fatal(err)
	}
	for _, follow := range []bool{false, true} {
		if f, err := openEntry(root, "parent/file", false, follow); err == nil {
			f.Close()
			t.Fatal("ancestor symlink escaped root")
		}
	}
	write(t, filepath.Join(dir, "inside"), "inside")
	if err := os.Symlink("inside", filepath.Join(dir, "leaf")); err != nil {
		t.Fatal(err)
	}
	if f, err := openEntry(root, "leaf", false, false); err == nil {
		f.Close()
		t.Fatal("leaf symlink was followed without permission")
	}
	if f, err := openEntry(root, "leaf", false, true); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}
}
