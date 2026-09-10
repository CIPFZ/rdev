package winutil

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func TestPrivateFilesLocksAndReplacement(t *testing.T) {
	root := filepath.Join(t.TempDir(), "private")
	if err := EnsurePrivateDir(root); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "record.json")
	if err := AtomicWrite(p, []byte("first")); err != nil {
		t.Fatal(err)
	}
	before, err := Open(p, os.O_RDONLY, true)
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	firstID, err := Identity(before)
	if err != nil {
		t.Fatal(err)
	}
	if err = AtomicWrite(p, []byte("second")); err != nil {
		t.Fatal(err)
	}
	after, err := Open(p, os.O_RDONLY, true)
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	secondID, _ := Identity(after)
	if firstID == secondID {
		t.Fatal("atomic replacement retained old file identity")
	}
	lockPath := filepath.Join(root, "lock")
	a, err := Open(lockPath, os.O_RDWR|os.O_CREATE, true)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(lockPath, os.O_RDWR, true)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err = Lock(a, false, true); err != nil {
		t.Fatal(err)
	}
	if err = Lock(b, true, true); !Contended(err) {
		t.Fatalf("exclusive lock bypassed shared holder: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = LockContext(ctx, b, true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock cancellation: %v", err)
	}
	if err = Unlock(a); err != nil {
		t.Fatal(err)
	}
	if err = Lock(b, true, true); err != nil {
		t.Fatal(err)
	}
	_ = Unlock(b)
}

func TestRejectBroadACLAndReparsePaths(t *testing.T) {
	root := filepath.Join(t.TempDir(), "private")
	if err := EnsurePrivateDir(root); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "private.json")
	if err := AtomicWrite(p, []byte("private")); err != nil {
		t.Fatal(err)
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;;FA;;;WD)")
	if err != nil {
		t.Fatal(err)
	}
	acl, _, err := sd.DACL()
	if err != nil {
		t.Fatal(err)
	}
	u, err := windows.UTF16PtrFromString(p)
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(u, windows.WRITE_DAC, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = windows.SetSecurityInfo(h, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil)
	windows.CloseHandle(h)
	if err != nil {
		t.Fatal(err)
	}
	if err = CheckPrivate(p); err == nil {
		t.Fatal("world-readable private record accepted")
	}
	for _, path := range []string{`\\server\share\file`, `\\.\pipe\name`, `C:\data\NUL.txt`, `C:\data\COM¹`, `C:\data\LPT².txt`, `C:\data\CONOUT$`, `C:\data\file:stream`, `C:\data\trailing.`, `C:\data\trailing `} {
		if err := ValidatePath(path); err == nil {
			t.Errorf("accepted %q", path)
		}
	}
	// Junction creation needs no symlink developer privilege.
	link := filepath.Join(root, "junction")
	target := t.TempDir()
	cmd := execCommandJunction(link, target)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create junction: %s %v", out, err)
	}
	if err = ValidatePath(filepath.Join(link, "child")); err == nil {
		t.Fatal("junction ancestor accepted")
	}
}
