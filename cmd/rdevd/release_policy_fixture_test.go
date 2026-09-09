package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
)

// The isolated SSH harness checks its own trust fixture before dispatching any
// runtime operation. Diagnostics are enums and permission bits, never paths,
// policy contents, peer output, or the original error text.
func TestIsolatedReleasePolicyReadiness(t *testing.T) {
	if os.Getenv("RDEV_TEST_RELEASE_POLICY_PREFLIGHT") != "1" {
		t.Skip("isolated SSH trust-fixture preflight only")
	}
	path, err := artifact.DefaultPolicyPath()
	if err == nil {
		_, err = artifact.LoadPolicy(path, time.Now())
	}
	reason := releasePolicyFixtureReason(err)
	fmt.Printf("RDEV_SAFE_POLICY reason=%s explicit=%t\n", reason, os.Getenv("RDEV_RELEASE_POLICY") != "")
	if err == nil {
		return
	}
	for depth, p := 0, path; depth < 32 && p != ""; depth, p = depth+1, filepath.Dir(p) {
		st, statErr := os.Lstat(p)
		if statErr != nil {
			fmt.Printf("RDEV_SAFE_POLICY_COMPONENT depth=%d status=%s\n", depth, releasePolicyFixtureReason(statErr))
		} else {
			owner := "unknown"
			if native, ok := st.Sys().(*syscall.Stat_t); ok {
				switch native.Uid {
				case uint32(os.Getuid()):
					owner = "self"
				case 0:
					owner = "root"
				default:
					owner = "other"
				}
			}
			fmt.Printf("RDEV_SAFE_POLICY_COMPONENT depth=%d status=present owner=%s mode=%04o symlink=%t directory=%t\n", depth, owner, st.Mode().Perm(), st.Mode()&os.ModeSymlink != 0, st.IsDir())
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	t.Fatal("isolated release policy readiness rejected")
}

func releasePolicyFixtureReason(err error) string {
	if err == nil {
		return "accepted"
	}
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "missing"
	case errors.Is(err, os.ErrPermission):
		return "permission_denied"
	}
	for _, item := range []struct{ text, code string }{
		{"release policy parent is writable by others", "writable_parent"},
		{"release policy path has untrusted owner", "untrusted_owner"},
		{"unsupported extended ACL", "extended_acl"},
		{"release policy path contains symlink", "symlink"},
		{"release policy must be a private regular file", "private_file_required"},
		{"release policy descriptor is not private and owned", "private_descriptor_required"},
		{"release policy is invalid or expired", "invalid_or_expired"},
	} {
		if strings.Contains(err.Error(), item.text) {
			return item.code
		}
	}
	return "rejected"
}

func TestReleasePolicyFixtureDiagnosticsExcludeRawValues(t *testing.T) {
	for _, err := range []error{errors.New("private-fixture-value"), &os.PathError{Op: "open", Path: "private-fixture-value", Err: syscall.EACCES}} {
		if strings.Contains(releasePolicyFixtureReason(err), "private-fixture-value") {
			t.Fatal("fixture diagnostic exposed raw values")
		}
	}
}

func TestReleasePolicyFixtureChecksInheritedPathAndWritableAncestors(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "rdev-policy-readiness-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(dir, "parent")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "policy.json")
	data := fmt.Sprintf(`{"schema_version":1,"valid_until":%q,"channels":["dev"],"allow_unsigned_dev":true,"allow_test_roots":false,"roots":[]}`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	for _, writable := range []bool{false, true} {
		if writable {
			if err := os.Chmod(parent, 0777); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestIsolatedReleasePolicyReadiness$", "-test.v")
		cmd.Env = append(os.Environ(), "RDEV_TEST_RELEASE_POLICY_PREFLIGHT=1", "RDEV_RELEASE_POLICY="+path)
		out, err := cmd.CombinedOutput()
		want := "RDEV_SAFE_POLICY reason=accepted explicit=true"
		if writable {
			want = "RDEV_SAFE_POLICY reason=writable_parent explicit=true"
		}
		if (err != nil) != writable || !strings.Contains(string(out), want) || strings.Contains(string(out), path) || strings.Contains(string(out), data) {
			t.Fatalf("policy readiness inheritance or rejection contract failed: writable=%t exit=%v", writable, err)
		}
	}
}
