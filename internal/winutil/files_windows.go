// Package winutil implements the native Windows security and persistence
// primitives shared by the remote agent. It is not a Windows controller port.
package winutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func UserSID() (*windows.SID, error) {
	u, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, err
	}
	return u.User.Sid.Copy()
}

func PrivateDescriptor() (*windows.SECURITY_DESCRIPTOR, error) {
	u, err := UserSID()
	if err != nil {
		return nil, err
	}
	return windows.SecurityDescriptorFromString("O:" + u.String() + "D:P(A;OICI;FA;;;" + u.String() + ")(A;OICI;FA;;;SY)")
}

// CheckHandle rejects reparse points and validates the security descriptor on
// the opened object, not a second lookup of its pathname.
func CheckHandle(f *os.File, private bool) error {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(f.Fd()), &info); err != nil {
		return err
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return errors.New("reparse points are not supported in managed paths")
	}
	if !private {
		return nil
	}
	sd, err := windows.GetSecurityInfo(windows.Handle(f.Fd()), windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	owner, _, err := sd.Owner()
	if err != nil {
		return err
	}
	user, err := UserSID()
	if err != nil {
		return err
	}
	// Elevated tokens may assign Administrators as the default owner of files
	// created under our protected directory. Administrators already have the
	// OS privilege to take ownership; the DACL must still allow only user/SYSTEM.
	if owner == nil || (!owner.Equals(user) && owner.String() != "S-1-5-32-544") {
		return errors.New("managed object owner differs from the current Windows user")
	}
	acl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	if acl == nil {
		return errors.New("managed object has an unrestricted DACL")
	}
	for i := uint32(0); i < uint32(acl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		err := windows.GetAce(acl, i, &ace)
		if err != nil {
			return err
		}
		if ace.Header.AceType == windows.ACCESS_DENIED_ACE_TYPE {
			continue
		}
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
			return errors.New("unsupported managed object ACL entry")
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if !sid.Equals(user) && sid.String() != "S-1-5-18" && ace.Mask != 0 {
			return errors.New("managed object grants access to another Windows identity")
		}
	}
	return nil
}

// ValidatePath excludes UNC/device namespaces, alternate streams and reparse
// ancestors. Managed storage is deliberately limited to local disk paths.
func ValidatePath(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	volume := filepath.VolumeName(abs)
	if len(volume) != 2 || volume[1] != ':' || strings.HasPrefix(abs, `\\`) {
		return errors.New("a local Windows drive path is required")
	}
	for _, part := range strings.Split(strings.ReplaceAll(abs[len(volume):], `\`, "/"), "/") {
		if part == "" {
			continue
		}
		if strings.ContainsAny(part, ":\x00") || strings.TrimRight(part, ". ") != part {
			return errors.New("unsafe Windows path component")
		}
		base := strings.ToUpper(strings.SplitN(part, ".", 2)[0])
		name := []rune(base)
		port := len(name) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && (name[3] >= '0' && name[3] <= '9' || name[3] == '¹' || name[3] == '²' || name[3] == '³')
		if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || base == "CONIN$" || base == "CONOUT$" || port {
			return errors.New("reserved Windows device name")
		}
	}
	for p := abs; ; p = filepath.Dir(p) {
		u, err := windows.UTF16PtrFromString(p)
		if err != nil {
			return err
		}
		attr, err := windows.GetFileAttributes(u)
		if err == nil && attr&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
			return errors.New("reparse-point path component")
		}
		if err != nil && !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) && !errors.Is(err, windows.ERROR_PATH_NOT_FOUND) {
			return err
		}
		if parent := filepath.Dir(p); parent == p {
			break
		}
	}
	return nil
}

func Open(path string, flags int, private bool) (*os.File, error) {
	return open(path, flags, private, true)
}

// Lock handles deny deletion. A waiter already holding a handle therefore
// prevents unlink/recreation from splitting one logical lock into two objects.
func OpenLock(path string, flags int) (*os.File, error) { return open(path, flags, true, false) }

func open(path string, flags int, private, shareDelete bool) (*os.File, error) {
	if err := ValidatePath(path); err != nil {
		return nil, err
	}
	if private {
		release, err := pinPrivateParents(path)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	u, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	access := uint32(windows.GENERIC_READ)
	if flags&os.O_RDWR != 0 {
		access |= windows.GENERIC_WRITE
	} else if flags&os.O_WRONLY != 0 {
		access = windows.GENERIC_WRITE | windows.READ_CONTROL
	}
	disposition := uint32(windows.OPEN_EXISTING)
	if flags&os.O_CREATE != 0 {
		disposition = windows.OPEN_ALWAYS
		if flags&os.O_EXCL != 0 {
			disposition = windows.CREATE_NEW
		}
	}
	var sa *windows.SecurityAttributes
	if private {
		sd, err := PrivateDescriptor()
		if err != nil {
			return nil, err
		}
		sa = &windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	}
	sharing := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE)
	if shareDelete {
		sharing |= windows.FILE_SHARE_DELETE
	}
	h, err := windows.CreateFile(u, access, sharing, sa, disposition, windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(h), path)
	if err = CheckHandle(f, private); err != nil {
		f.Close()
		return nil, err
	}
	if flags&os.O_TRUNC != 0 {
		if err = f.Truncate(0); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

func CheckPrivate(path string) error {
	f, err := Open(path, os.O_RDONLY, true)
	if err != nil {
		return err
	}
	return f.Close()
}

func EnsurePrivateDir(path string) error {
	if err := ValidatePath(path); err != nil {
		return err
	}
	if st, err := os.Lstat(path); err == nil {
		if !st.IsDir() {
			return errors.New("managed path is not a directory")
		}
		return CheckPrivate(path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if _, err := os.Stat(parent); errors.Is(err, os.ErrNotExist) {
		if err = EnsurePrivateDir(parent); err != nil {
			return err
		}
	}
	release, err := pinPrivateParents(path)
	if err != nil {
		return err
	}
	defer release()
	sd, err := PrivateDescriptor()
	if err != nil {
		return err
	}
	u, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	sa := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
	if err = windows.CreateDirectory(u, &sa); err != nil && !errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
		return err
	}
	return CheckPrivate(path)
}

func Lock(f *os.File, exclusive, nonblocking bool) error {
	flags := uint32(0)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	if nonblocking {
		flags |= windows.LOCKFILE_FAIL_IMMEDIATELY
	}
	// Reserve a byte beyond all record content so readers do not encounter
	// mandatory byte-range lock failures while inspecting the lease marker.
	ov := windows.Overlapped{Offset: 0xffffffff, OffsetHigh: 0x7fffffff}
	return windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, &ov)
}
func Unlock(f *os.File) error {
	ov := windows.Overlapped{Offset: 0xffffffff, OffsetHigh: 0x7fffffff}
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &ov)
}
func Contended(err error) bool {
	return errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_IO_PENDING)
}
func LockContext(ctx context.Context, f *os.File, exclusive bool) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := Lock(f, exclusive, true)
		if !Contended(err) {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func Replace(from, to string) error {
	if err := ValidatePath(from); err != nil {
		return err
	}
	if err := ValidatePath(to); err != nil {
		return err
	}
	s, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	d, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(s, d, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

func Identity(f *os.File) (string, error) {
	var i windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(f.Fd()), &i); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x:%x:%x", i.VolumeSerialNumber, i.FileIndexHigh, i.FileIndexLow), nil
}

func AtomicWrite(path string, data []byte) error {
	if err := CheckPrivate(filepath.Dir(path)); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".rdev-write-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = CheckHandle(f, true); err != nil {
		f.Close()
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err != nil {
		return err
	}
	return Replace(tmp, path)
}
