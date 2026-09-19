//go:build !windows

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/fileedit"
	"github.com/CIPFZ/rdev/internal/proto"
	"golang.org/x/sys/unix"
)

// The parent directory descriptor is the cross-process lock identity, even
// across separate rdev state roots. It is pinned for all open/rename operations.
// This serializes edits in that directory. Non-cooperating writers (exec,
// editors, old write/sync) are rechecked but cannot be given a filesystem CAS.
func doEdit(ctx context.Context, p *proto.EditParams) (*proto.EditResult, error) {
	if err := fileedit.Validate(p); err != nil {
		return nil, err
	}
	path := expandHome(p.Path)
	if err := validateBusinessPath(path); err != nil {
		return nil, editError(proto.CodeInvalidRequest)
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	parent, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		if ctx.Err() != nil {
			return nil, editError(proto.CodeDeadlineExceeded)
		}
		err = unix.Flock(int(parent.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, editError(proto.CodeDeadlineExceeded)
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer unix.Flock(int(parent.Fd()), unix.LOCK_UN)
	name := filepath.Base(path)
	read := func() ([]byte, os.FileInfo, error) {
		fd, e := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if e != nil {
			if errors.Is(e, unix.ELOOP) {
				return nil, nil, editError(proto.CodeInvalidRequest)
			}
			return nil, nil, e
		}
		f := os.NewFile(uintptr(fd), name)
		defer f.Close()
		b, st, e := snapshotBytes(f)
		// Replacement must not silently break hard links or transfer another user's
		// file ownership to this process.
		if e == nil {
			stat, ok := st.Sys().(*syscall.Stat_t)
			if !ok || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() {
				return nil, nil, editError(proto.CodeInvalidRequest)
			}
		}
		return b, st, e
	}
	before, st, err := read()
	if err != nil {
		return nil, err
	}
	after, err := fileedit.Apply(before, p)
	if err != nil {
		return nil, err
	}
	res := editResult(before, after)
	if !res.Changed {
		return res, nil
	}
	id, err := proto.NewOperationID()
	if err != nil {
		return nil, err
	}
	backupName := ""
	backupKept := false
	if p.Backup {
		backupName = ".rdev-backup-" + id
		backup, openErr := root.OpenFile(backupName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if openErr != nil {
			return nil, openErr
		}
		if _, openErr = backup.Write(before); openErr == nil {
			openErr = backup.Chmod(st.Mode().Perm())
		}
		if openErr == nil {
			openErr = backup.Sync()
		}
		closeErr := backup.Close()
		if openErr == nil {
			openErr = closeErr
		}
		if openErr != nil {
			_ = root.Remove(backupName)
			return nil, openErr
		}
		if openErr = parent.Sync(); openErr != nil {
			_ = root.Remove(backupName)
			return nil, openErr
		}
	}
	defer func() {
		if backupName != "" && !backupKept {
			_ = root.Remove(backupName)
		}
	}()
	tmp := ".rdev-edit-" + id
	f, err := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer root.Remove(tmp)
	if _, err = f.Write(after); err == nil {
		err = f.Chmod(st.Mode().Perm())
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	current, currentStat, err := read()
	if err != nil {
		return nil, editError(proto.CodeEditConflict)
	}
	if !os.SameFile(st, currentStat) || !bytes.Equal(before, current) || st.Mode() != currentStat.Mode() || !st.ModTime().Equal(currentStat.ModTime()) {
		return nil, editError(proto.CodeEditConflict)
	}
	if ctx.Err() != nil {
		return nil, editError(proto.CodeDeadlineExceeded)
	}
	if err = root.Rename(tmp, name); err != nil {
		return nil, err
	}
	backupKept = backupName != ""
	res.BackupID = id
	// Publication already happened. Never imply that a post-rename failure left
	// the old content intact, and never try to roll back over another writer.
	if err = parent.Sync(); err != nil {
		return nil, proto.NewError(proto.CodeAmbiguousOutcome, "", proto.StatePossiblyExecuted)
	}
	return res, nil
}
