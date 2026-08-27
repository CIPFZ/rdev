//go:build !windows

package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestConfigMetadataRejectsWrongOwnerAndWritableModes(t *testing.T) {
	for _, tc := range []struct {
		name            string
		uid, euid, mode uint32
		dir             bool
	}{
		{"wrong owner file", 2, 1, unix.S_IFREG | 0o600, false},
		{"wrong owner directory", 2, 1, unix.S_IFDIR | 0o700, true},
		{"group writable file", 1, 1, unix.S_IFREG | 0o620, false},
		{"world writable file", 1, 1, unix.S_IFREG | 0o602, false},
		{"group writable directory", 1, 1, unix.S_IFDIR | 0o720, true},
		{"world writable directory", 1, 1, unix.S_IFDIR | 0o702, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateConfigMetadata(tc.uid, tc.euid, tc.mode, tc.dir); err == nil {
				t.Fatal("insecure metadata accepted")
			}
		})
	}
	for _, tc := range []struct {
		mode uint32
		dir  bool
	}{{unix.S_IFREG | 0o644, false}, {unix.S_IFDIR | 0o755, true}} {
		if err := validateConfigMetadata(1, 1, tc.mode, tc.dir); err != nil {
			t.Fatalf("legacy read-only mode rejected: %v", err)
		}
	}
}

func TestReadConfigRejectsWritableDirectoryAndFile(t *testing.T) {
	for _, target := range []string{"directory", "file"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "hosts.json")
			if err := os.WriteFile(path, []byte(`{"hosts":[]}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if target == "directory" {
				if err := os.Chmod(dir, 0o720); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Chmod(path, 0o620); err != nil {
					t.Fatal(err)
				}
			}
			_, err := readConfigFile(path)
			if err == nil || !strings.Contains(err.Error(), "writable by group or other") {
				t.Fatalf("read error=%v", err)
			}
		})
	}
}

func TestExtendedACLNamesAreFailClosed(t *testing.T) {
	for _, name := range []string{"system.posix_acl_access", "system.posix_acl_default", "com.apple.acl.text"} {
		if err := validateConfigACLNames("config", []string{name}); err == nil {
			t.Fatalf("ACL %q accepted", name)
		}
	}
	// Production obtains these names with Flistxattr on the already-open fd.
	// Platform ACL creation is privilege/filesystem dependent, so the policy is
	// also asserted structurally here and exercised when such xattrs are present.
	dir := t.TempDir()
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := rejectConfigACL(fd, dir); err != nil {
		t.Fatalf("ordinary directory ACL inspection failed: %v", err)
	}
}

func TestTrustStoreRejectsWritableFileBeforeParsing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir := filepath.Join(home, ".rdev")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "trusted-projects.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"projects":[]}`), 0o620); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o620); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistry().loadTrustFile(); err == nil || !strings.Contains(err.Error(), "writable by group or other") {
		t.Fatalf("trust read error=%v", err)
	}
}
