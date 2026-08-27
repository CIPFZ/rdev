//go:build darwin

package session

import (
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinNativeACLIsRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hosts.json")
	if err := os.WriteFile(path, []byte(`{"hosts":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("chmod", "+a", current.Username+" allow write", path).CombinedOutput(); err != nil {
		t.Fatalf("create ACL: %v: %s", err, out)
	}
	_, err = readConfigFile(path)
	if err == nil || !strings.Contains(err.Error(), "unsupported extended ACL") {
		t.Fatalf("ACL read error=%v", err)
	}
}

func TestDarwinACLInspectionIsBoundToOpenedFD(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hosts.json")
	moved := filepath.Join(dir, "opened-with-acl")
	if err := os.WriteFile(path, []byte(`{"hosts":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("chmod", "+a", current.Username+" allow write", path).CombinedOutput(); err != nil {
		t.Fatalf("create ACL: %v: %s", err, out)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"hosts":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := rejectConfigACL(fd, path); err == nil || !strings.Contains(err.Error(), "unsupported extended ACL") {
		t.Fatalf("fd-bound ACL query followed replacement pathname: %v", err)
	}
}
