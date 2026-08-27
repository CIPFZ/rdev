//go:build darwin

package session

import (
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
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
