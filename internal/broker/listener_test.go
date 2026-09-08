package broker

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestListenPrivateAndSingleInstance(t *testing.T) {
	p := filepath.Join(listenerTestDir(t), "private", "broker.sock")
	defer os.Remove(p)
	l, err := Listen(p)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("mode %o", st.Mode().Perm())
	}
	if _, err = Listen(p); err == nil {
		t.Fatal("expected second listener to fail")
	}
}

func TestListenRecoversStaleSocket(t *testing.T) {
	p := filepath.Join(listenerTestDir(t), "private", "broker.sock")
	_ = os.Remove(p)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: p, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	_ = stale.Close()
	l, err := Listen(p)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
}

func TestListenRejectsUnsafeFilesystemObjects(t *testing.T) {
	for _, kind := range []string{"public_parent", "symlink_parent", "regular_socket", "symlink_lock", "public_lock"} {
		t.Run(kind, func(t *testing.T) {
			dir := filepath.Join(listenerTestDir(t), "private")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "broker.sock")
			switch kind {
			case "public_parent":
				os.Chmod(dir, 0o755)
			case "symlink_parent":
				os.Rename(dir, dir+".real")
				os.Symlink(dir+".real", dir)
			case "regular_socket":
				os.WriteFile(path, []byte("retain me"), 0o600)
			case "symlink_lock":
				os.WriteFile(path+".real", []byte("retain me"), 0o600)
				os.Symlink(path+".real", path+".lock")
			case "public_lock":
				os.WriteFile(path+".lock", nil, 0o644)
			}
			if l, err := Listen(path); err == nil {
				l.Close()
				t.Fatal("unsafe object accepted")
			}
			if kind == "regular_socket" {
				data, _ := os.ReadFile(path)
				if string(data) != "retain me" {
					t.Fatal("user file deleted")
				}
			}
		})
	}
}

func TestListenerKeepsLockThroughDrainAndCloseIsIdempotent(t *testing.T) {
	path := filepath.Join(listenerTestDir(t), "private", "broker.sock")
	l, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.StopAccepting(); err != nil {
		t.Fatal(err)
	}
	if second, err := Listen(path); err == nil {
		second.Close()
		t.Fatal("lock released before drain")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("old listener removed new instance's socket")
	}
}

func listenerTestDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "rdev-listener-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}
