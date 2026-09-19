//go:build !windows

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/CIPFZ/rdev/internal/fileedit"
	"github.com/CIPFZ/rdev/internal/proto"
	"golang.org/x/sys/unix"
)

func doEditRollback(ctx context.Context, p *proto.EditRollbackParams) (*proto.EditRollbackResult, error) {
	if p == nil || p.Path == "" || proto.ValidateOperationID(p.BackupID) != nil {
		return nil, editError(proto.CodeInvalidRequest)
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
		if err = unix.Flock(int(parent.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
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
	readNamed := func(name string) ([]byte, os.FileInfo, error) {
		fd, openErr := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
		if openErr != nil {
			if errors.Is(openErr, unix.ELOOP) {
				return nil, nil, editError(proto.CodeInvalidRequest)
			}
			return nil, nil, openErr
		}
		f := os.NewFile(uintptr(fd), name)
		defer f.Close()
		return snapshotBytes(f)
	}
	current, _, err := readNamed(filepath.Base(path))
	if err != nil {
		return nil, err
	}
	if p.ExpectedDigest != "" && fileedit.Digest(current) != p.ExpectedDigest {
		return nil, editError(proto.CodeEditConflict)
	}
	backupName := ".rdev-backup-" + p.BackupID
	backup, backupStat, err := readNamed(backupName)
	if err != nil {
		return nil, err
	}
	if backupStat == nil || !backupStat.Mode().IsRegular() {
		return nil, editError(proto.CodeInvalidRequest)
	}
	tmp := ".rdev-rollback-" + p.BackupID
	f, err := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	defer root.Remove(tmp)
	if _, err = f.Write(backup); err == nil {
		err = f.Chmod(backupStat.Mode().Perm())
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
	check, _, err := readNamed(filepath.Base(path))
	if err != nil || !bytes.Equal(check, current) {
		return nil, editError(proto.CodeEditConflict)
	}
	if err = root.Rename(tmp, filepath.Base(path)); err != nil {
		return nil, err
	}
	if err = parent.Sync(); err != nil {
		return nil, proto.NewError(proto.CodeAmbiguousOutcome, "", proto.StatePossiblyExecuted)
	}
	_ = root.Remove(backupName)
	return &proto.EditRollbackResult{OldDigest: fileedit.Digest(current), NewDigest: fileedit.Digest(backup), BackupID: p.BackupID, BytesBefore: len(current), BytesAfter: len(backup), Committed: true}, nil
}
