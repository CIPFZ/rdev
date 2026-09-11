//go:build darwin

package synctree

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestDarwinSymlinkPermissionsDoNotChangePortableManifest(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "file"), "payload")
	link := filepath.Join(dir, "link")
	if err := os.Symlink("file", link); err != nil {
		t.Fatal(err)
	}
	var digest string
	for _, mode := range []string{"0755", "0777", "0700"} {
		if out, err := exec.Command("chmod", "-h", mode, link).CombinedOutput(); err != nil {
			t.Fatalf("change link permissions: %v %s", err, out)
		}
		m := scan(t, dir, "preserve")
		if digest != "" && m.Digest != digest {
			t.Fatal("nonportable symlink permissions changed transfer digest")
		}
		digest = m.Digest
		info, err := os.Stat(filepath.Join(dir, "file"))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("link permission handling changed the target")
		}
	}
}
