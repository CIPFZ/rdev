package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/CIPFZ/rdev/internal/winutil"
)

const lockDirName = ".job-locks"

type jobLockTestHooks struct{ acquired, contended func(string) }

var jobLockTestSeam *jobLockTestHooks

func lockPath(dir string) string {
	return filepath.Join(filepath.Dir(filepath.Dir(dir)), lockDirName, filepath.Base(dir)+".lock")
}
func withJobLock(dir string, fn func() error) error {
	path := lockPath(dir)
	if err := secureDir(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := winutil.OpenLock(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return err
	}
	defer f.Close()
	if err = winutil.Lock(f, true, false); err != nil {
		return err
	}
	defer winutil.Unlock(f)
	return fn()
}
func tryJobLock(dir string, fn func() error) (bool, error) { return tryJobLockMode(dir, true, fn) }
func tryExistingJobLock(dir string, fn func() error) (bool, error) {
	return tryJobLockMode(dir, false, fn)
}
func probeExistingJobLock(dir string) (bool, error) {
	return tryJobLockMode(dir, false, func() error { return nil })
}
func tryJobLockMode(dir string, create bool, fn func() error) (bool, error) {
	path := lockPath(dir)
	flags := os.O_RDWR
	if create {
		if err := secureDir(filepath.Dir(path), 0700); err != nil {
			return false, err
		}
		flags |= os.O_CREATE
	}
	f, err := winutil.OpenLock(path, flags)
	if !create && errors.Is(err, os.ErrNotExist) {
		return true, fn()
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err = winutil.Lock(f, true, true); err != nil {
		if winutil.Contended(err) {
			return false, nil
		}
		return false, err
	}
	defer winutil.Unlock(f)
	return true, fn()
}
func jobExists(dir string) bool { st, err := os.Stat(dir); return err == nil && st.IsDir() }

// OpenLock denies FILE_SHARE_DELETE: removal fails while any active holder or
// waiting opener pins this exact lock object. Recreating after removal is safe.
func removeJobLock(dir string) { _ = os.Remove(lockPath(dir)) }
