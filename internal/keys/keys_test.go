package keys

import (
	"os"
	"path/filepath"
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
}

func TestHostIDSeparatesConnectionIdentity(t *testing.T) {
	if HostID("alice", "host", 22, "a") == HostID("bob", "host", 22, "a") {
		t.Fatal("user not included")
	}
	if HostID("alice", "host", 22, "a") == HostID("alice", "host", 23, "a") {
		t.Fatal("port not included")
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
