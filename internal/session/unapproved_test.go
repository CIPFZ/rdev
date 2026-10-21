package session

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnapprovedProjectHostsExplainResolution(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(project)
	for _, dir := range []string{filepath.Join(home, ".rdev"), filepath.Join(project, ".rdev")} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(home, ".rdev", "hosts.json"), []byte(`{"hosts":[{"name":"dev","addr":"u@global"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	projectPath := filepath.Join(project, ".rdev", "hosts.json")
	if err := os.WriteFile(projectPath, []byte(`{"hosts":[{"name":"dev","addr":"u@project"},{"name":"box-250","addr":"root@192.0.2.250"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewRegistry()
	var notices []string
	r.SetNotice(func(m string) { notices = append(notices, m) })
	var untrusted *UntrustedProjectError
	if err := r.Load(); !errors.As(err, &untrusted) {
		t.Fatalf("Load error = %v, want UntrustedProjectError", err)
	}
	if got := strings.Join(r.UnapprovedProjectHosts(), ","); got != "box-250,dev" {
		t.Fatalf("UnapprovedProjectHosts = %q", got)
	}

	// A name only the unapproved file defines must say so instead of "unknown".
	_, err := r.Resolve("box-250")
	var hidden *UnapprovedProjectHostError
	if !errors.As(err, &hidden) || hidden.Name != "box-250" {
		t.Fatalf("Resolve(box-250) error = %v, want UnapprovedProjectHostError", err)
	}
	for _, want := range []string{"unapproved project config", untrusted.Trust.Path, "rdev hosts approve-project " + untrusted.Trust.Digest} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err, want)
		}
	}

	// A global alias the project also defines still resolves globally, with a
	// single notice rather than one per call.
	for i := 0; i < 3; i++ {
		h, err := r.Host("dev")
		if err != nil || h.Addr != "u@global" {
			t.Fatalf("Host(dev) = %+v, %v", h, err)
		}
	}
	if len(notices) != 1 || !strings.Contains(notices[0], `"dev"`) || !strings.Contains(notices[0], "global") {
		t.Fatalf("notices = %q, want one global-shadow notice", notices)
	}

	// Unrelated unknown names keep the existing error.
	if _, err := r.Resolve("nope"); err == nil || errors.As(err, &hidden) || !strings.Contains(err.Error(), "unknown host") {
		t.Fatalf("Resolve(nope) error = %v", err)
	}

	if _, err := r.ApproveProject(untrusted.Trust.Digest); err != nil {
		t.Fatal(err)
	}
	if len(r.UnapprovedProjectHosts()) != 0 {
		t.Fatal("approval left hidden names behind")
	}
	if h, err := r.Host("box-250"); err != nil || h.Addr != "root@192.0.2.250" {
		t.Fatalf("approved Host(box-250) = %+v, %v", h, err)
	}
}
