package keys

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOpenStableAndGenerateDoesNotAdoptExistingKey(t *testing.T) {
	root, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a, err := Open(root, "alice", "host", 22, "state")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(root, "alice", "host", 22, "state")
	if err != nil {
		t.Fatal(err)
	}
	if a.DeviceID != b.DeviceID || a.HostID != b.HostID {
		t.Fatalf("identity changed: %+v %+v", a, b)
	}
	if err := a.Generate(); err != nil {
		t.Fatal(err)
	}
	if err := a.Generate(); err == nil {
		t.Fatal("regenerated an owned key")
	}
	for _, p := range []string{a.Private, a.Public} {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0600 {
			t.Fatalf("%s mode %o", p, st.Mode().Perm())
		}
	}
	if _, err := os.Stat(a.Metadata); err != nil {
		t.Fatal(err)
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(a.Public, []byte("bad"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := a.Validate(); err == nil {
		t.Fatal("accepted corrupted public key")
	}
}

func TestHostIDSeparatesConnectionIdentity(t *testing.T) {
	if HostID("alice", "host", 22, "a") == HostID("bob", "host", 22, "a") {
		t.Fatal("user not included")
	}
	if HostID("alice", "host", 22, "a") == HostID("alice", "host", 23, "a") {
		t.Fatal("port not included")
	}
}

func TestDefaultRootUsesXDGConfigHome(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/tmp/rdev-config")
	root, err := DefaultRoot()
	if err != nil || root != "/tmp/rdev-config/rdev/keys" {
		t.Fatalf("root=%q err=%v", root, err)
	}
}

func TestOpenRejectsPublicDeviceID(t *testing.T) {
	root, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "device-id"), []byte("device\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, "alice", "host", 22, "state"); err == nil {
		t.Fatal("accepted public device id")
	}
}

func TestOpenRejectsSymlinkDeviceID(t *testing.T) {
	root, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("0123456789abcdef\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "device-id")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, "u", "h", 22, "s"); err == nil {
		t.Fatal("accepted symlink device id")
	}
}

func TestConcurrentOpenAndGenerateKeepOneIdentity(t *testing.T) {
	root, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids := make(chan Identity, 8)
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i, e := Open(root, "u", "h", 22, "s")
			if e == nil {
				_ = i.Generate()
				ids <- i
			}
		}()
	}
	wg.Wait()
	close(ids)
	var first Identity
	for i := range ids {
		if first.Private == "" {
			first = i
		} else if i.DeviceID != first.DeviceID || i.HostID != first.HostID {
			t.Fatal("concurrent identity diverged")
		}
	}
	if first.Private == "" {
		t.Fatal("no identity created")
	}
}
