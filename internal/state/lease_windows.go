package state

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/CIPFZ/rdev/internal/winutil"
)

type Lease struct{ file *os.File }

func (l *Lease) Close() error   { _ = winutil.Unlock(l.file); return l.file.Close() }
func (l *Lease) File() *os.File { return l.file }

const leaseName = lockName
const leaseMarker = "rdev-state-lease-v1\n"

func acquireLease(root string, exclusive bool) (*Lease, error) {
	if err := validateRoot(root); err != nil {
		return nil, err
	}
	f, err := winutil.OpenLock(filepath.Join(root, leaseName), os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*Lease, error) { f.Close(); return nil, e }
	st, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	if st.Size() == 0 {
		if err = winutil.Lock(f, true, true); err != nil {
			return fail(ErrMigrationLocked)
		}
		st, err = f.Stat()
		if err == nil && st.Size() == 0 {
			_, err = f.WriteAt([]byte(leaseMarker), 0)
			if err == nil {
				err = f.Sync()
			}
		}
		_ = winutil.Unlock(f)
		if err != nil {
			return fail(err)
		}
	}
	if err = winutil.Lock(f, exclusive, true); err != nil {
		if winutil.Contended(err) {
			err = ErrMigrationLocked
		}
		return fail(err)
	}
	if err = checkLeaseFile(root, f); err != nil {
		return fail(err)
	}
	return &Lease{file: f}, nil
}
func checkLeaseFile(root string, f *os.File) error {
	if err := winutil.CheckHandle(f, true); err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	other, err := os.Lstat(filepath.Join(root, leaseName))
	if err != nil || !os.SameFile(st, other) {
		return errors.New("state lease pathname changed")
	}
	b := make([]byte, len(leaseMarker)+1)
	n, err := f.ReadAt(b, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if string(b[:n]) != leaseMarker {
		return ErrMigrationLocked
	}
	return nil
}
func AcquireWriter(root string) (*Lease, error) {
	l, err := acquireLease(root, false)
	if err != nil {
		return nil, err
	}
	if err = CheckCompatible(root); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}
func CheckCompatible(root string) error {
	if err := validateRoot(root); err != nil {
		return err
	}
	_, err := loadManifest(root)
	return err
}
