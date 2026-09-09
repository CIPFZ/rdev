//go:build linux || darwin

package artifact

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestArtifactOpenRejectsFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifo")
	if err := unix.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		file, err := openArtifact(path)
		if file != nil {
			file.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO accepted as an artifact")
		}
	case <-time.After(5 * time.Second):
		// Free a regressed blocking opener before reporting the failure.
		fd, err := unix.Open(path, unix.O_RDWR|unix.O_NONBLOCK, 0)
		if err == nil {
			defer unix.Close(fd)
		}
		t.Fatal("artifact open waited for a FIFO writer")
	}
}

func TestArtifactOpenDoesNotFollowSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "regular")
	if err := os.WriteFile(target, []byte("regular artifact bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	file, err := openArtifact(link)
	if file != nil {
		file.Close()
	}
	if err == nil {
		t.Fatal("artifact opener followed a symlink")
	}
}
