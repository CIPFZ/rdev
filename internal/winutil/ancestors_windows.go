package winutil

import (
	"errors"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

func trustedOwner(sid, user *windows.SID) bool {
	if sid == nil {
		return false
	}
	s := sid.String()
	return sid.Equals(user) || s == "S-1-5-18" || s == "S-1-5-32-544" || s == "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464" // TrustedInstaller
}

// Pin trusted, non-reparse ancestors while resolving the final object. Denying
// delete sharing closes the ancestor-rename race. ACL checks reject ancestors
// another non-administrative identity can replace or retarget; creating a new
// sibling directory at a drive root does not grant access to our existing one.
func pinPrivateParents(path string) (func(), error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	user, err := UserSID()
	if err != nil {
		return nil, err
	}
	var paths []string
	for p := filepath.Dir(abs); ; p = filepath.Dir(p) {
		paths = append(paths, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	var handles []windows.Handle
	release := func() {
		for i := len(handles) - 1; i >= 0; i-- {
			windows.CloseHandle(handles[i])
		}
	}
	fail := func(e error) (func(), error) { release(); return nil, e }
	for i := len(paths) - 1; i >= 0; i-- {
		name, err := windows.UTF16PtrFromString(paths[i])
		if err != nil {
			return fail(err)
		}
		h, err := windows.CreateFile(name, windows.READ_CONTROL|windows.FILE_READ_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
		if err != nil {
			return fail(err)
		}
		handles = append(handles, h)
		var info windows.ByHandleFileInformation
		if err = windows.GetFileInformationByHandle(h, &info); err != nil {
			return fail(err)
		}
		if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY == 0 {
			return fail(errors.New("unsafe private path ancestor"))
		}
		sd, err := windows.GetSecurityInfo(h, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			return fail(err)
		}
		owner, _, err := sd.Owner()
		if err != nil || !trustedOwner(owner, user) {
			return fail(errors.New("untrusted private path ancestor owner"))
		}
		acl, _, err := sd.DACL()
		if err != nil || acl == nil {
			return fail(errors.New("unrestricted private path ancestor"))
		}
		for j := uint32(0); j < uint32(acl.AceCount); j++ {
			var ace *windows.ACCESS_ALLOWED_ACE
			if err = windows.GetAce(acl, j, &ace); err != nil {
				return fail(err)
			}
			if ace.Header.AceFlags&0x08 != 0 || ace.Header.AceType == windows.ACCESS_DENIED_ACE_TYPE {
				continue
			} // INHERIT_ONLY
			if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE {
				return fail(errors.New("unsupported ancestor ACL entry"))
			}
			sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
			if trustedOwner(sid, user) {
				continue
			}
			// Service-owner ACEs are not generally trusted; only the exact OS
			// identities above may control a private ancestor.
			const dangerous = 0x0002 | 0x0010 | 0x0040 | 0x0100 | 0x10000 | 0x40000 | 0x80000 | 0x10000000 | 0x40000000
			if uint32(ace.Mask)&dangerous != 0 {
				return fail(errors.New("private path ancestor is writable by another identity"))
			}
		}
	}
	return release, nil
}
