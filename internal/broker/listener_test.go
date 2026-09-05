package broker

import (
	"os"
	"path/filepath"
	"testing"
)

func TestListenPrivateAndSingleInstance(t *testing.T) {
	p := filepath.Join("/tmp", "rdevd-test.sock")
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
