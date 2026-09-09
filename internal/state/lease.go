package state

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// Lease fences state writers against migration. The permanent inode is never
// unlinked: unlinking a flock file would let old and new open descriptions own
// independent locks. Closing the final descriptor (including inherited ones)
// releases the lock even after SIGKILL.
type Lease struct{ file *os.File }

func (l *Lease) Close() error { return l.file.Close() }

// File may be inherited by a detached supervisor. Close deliberately does not
// issue LOCK_UN, since that would also unlock the child's open description.
func (l *Lease) File() *os.File { return l.file }

const leaseName = lockName
const leaseMarker = "rdev-state-lease-v1\n"

func checkLeaseFile(root string, f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	var native unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &native); err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || int(native.Uid) != os.Geteuid() || native.Nlink < 1 {
		return errors.New("state lease is not a private owned regular file")
	}
	pathInfo, err := os.Lstat(filepath.Join(root, leaseName))
	if err != nil || !os.SameFile(st, pathInfo) || pathInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("state lease pathname changed")
	}
	marker := make([]byte, len(leaseMarker)+1)
	n, err := f.ReadAt(marker, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if string(marker[:n]) != leaseMarker {
		return ErrMigrationLocked // legacy O_EXCL marker: require manual recovery
	}
	return nil
}

func acquireLease(root string, exclusive bool) (*Lease, error) {
	if err := validateRoot(root); err != nil {
		return nil, err
	}
	p := filepath.Join(root, leaseName)
	if _, err := os.Lstat(p); errors.Is(err, os.ErrNotExist) {
		if err := initializeLease(root); err != nil {
			return nil, err
		}
	}
	fd, err := unix.Open(p, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), p)
	if err := checkLeaseFile(root, f); err != nil {
		f.Close()
		return nil, err
	}
	mode := unix.LOCK_SH
	if exclusive {
		mode = unix.LOCK_EX
	}
	if err := unix.Flock(fd, mode|unix.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrMigrationLocked
		}
		return nil, err
	}
	if err := checkLeaseFile(root, f); err != nil {
		f.Close()
		return nil, err
	}
	return &Lease{file: f}, nil
}

// Initialization has a fixed two-name/one-inode budget. A crash before link
// leaves only this known private initializer; the next process takes its kernel
// lock and safely finishes. No random crash debris or stale PID reclamation is
// needed. The initializer name remains a recovery hard link after publication.
func initializeLease(root string) error {
	p := filepath.Join(root, leaseName)
	initPath := p + ".init"
	fd, err := unix.Open(initPath, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), initPath)
	defer f.Close()
	var native unix.Stat_t
	if err := unix.Fstat(fd, &native); err != nil {
		return err
	}
	if native.Mode&unix.S_IFMT != unix.S_IFREG || native.Mode&0777 != 0600 || int(native.Uid) != os.Geteuid() {
		return errors.New("state lease initializer is not private and owned")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Lstat(p); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EWOULDBLOCK) {
			return err
		}
		if time.Now().After(deadline) {
			return ErrMigrationLocked
		}
		time.Sleep(time.Millisecond)
	}
	// The other initializer may already have completed publication while this
	// opener was in flight. Never truncate an inode with active shared leases.
	if _, err := os.Lstat(p); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		return err
	}
	pathInfo, err := os.Lstat(initPath)
	if err != nil || !os.SameFile(st, pathInfo) || pathInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("state lease initializer pathname changed")
	}
	partial := make([]byte, len(leaseMarker)+1)
	n, readErr := f.ReadAt(partial, 0)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return readErr
	}
	if !strings.HasPrefix(leaseMarker, string(partial[:n])) {
		return errors.New("unrecognized state lease initializer; preserve for manual recovery")
	}
	if _, err := f.WriteAt([]byte(leaseMarker), 0); err != nil {
		return err
	}
	if err := f.Truncate(int64(len(leaseMarker))); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	// Published readers may immediately acquire shared leases, so downgrade the
	// initializer before making the name visible. Keep a shared lease through
	// the directory sync; migration cannot overtake incomplete publication.
	if err := unix.Flock(fd, unix.LOCK_SH); err != nil {
		return err
	}
	if err := os.Link(initPath, p); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	d, err := os.Open(root)
	if err != nil {
		return err
	}
	err = d.Sync()
	if closeErr := d.Close(); err == nil {
		err = closeErr
	}
	return err
}

// AcquireWriter takes a shared lease and checks the manifest while fenced.
// A missing manifest remains the supported legacy namespace, not a migration.
func AcquireWriter(root string) (*Lease, error) {
	l, err := acquireLease(root, false)
	if err != nil {
		return nil, err
	}
	if err := CheckCompatible(root); err != nil {
		l.Close()
		return nil, err
	}
	return l, nil
}

// AdoptWriter validates the descriptor passed by the starter. It keeps the
// shared lease continuous across the metadata commit and supervisor startup.
func AdoptWriter(root string, f *os.File) (*Lease, error) {
	if err := checkLeaseFile(root, f); err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		return nil, fmt.Errorf("inherit state writer lease: %w", err)
	}
	if err := CheckCompatible(root); err != nil {
		return nil, err
	}
	unix.CloseOnExec(int(f.Fd()))
	return &Lease{file: f}, nil
}

// CheckCompatible is read-only and suitable for readiness. It does not create
// a namespace, repair permissions, migrate state, or change writer identity.
func CheckCompatible(root string) error {
	if err := validateRoot(root); err != nil {
		return err
	}
	_, err := loadManifest(root)
	return err
}
