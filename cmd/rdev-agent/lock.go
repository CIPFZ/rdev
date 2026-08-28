// Per-job locking.
//
// Job records are shared mutable state with more than one writer. Two sources of
// concurrency, and the second is the one that surprises people:
//
//   - Several agents. Every rdev process spawns its own agent, and they all read
//     and write the same ~/.cache/rdev/jobs/ directory.
//   - One agent. main's serve loop runs every request in its own goroutine (see
//     maxConcurrentRequests), so two job_rm calls arriving on one connection race
//     just as hard as two processes do.
//
// Without a lock, jobRm was read-then-delete with a window in between, and that
// window produced two different wrong answers. The racer that read after the
// delete surfaced a raw ENOENT from meta.json. Every racer that read before it
// reported success, because os.RemoveAll returns nil for a path that is already
// gone -- six concurrent removals reported six removals and six times the freed
// bytes, which is worse than the error precisely because it looks fine.
//
// flock is the right primitive rather than a mutex: what is being guarded is a
// directory, and the contending writers are separate processes.
package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// lockDirName holds one lock file per job. It sits beside the jobs/ tree, not
// inside it, for two independent reasons:
//
//   - A lock file within the job directory would be deleted by the very
//     os.RemoveAll it serializes, and a waiter arriving between RemoveAll's last
//     readdir and its rmdir would recreate it and turn the removal into a
//     spurious ENOTEMPTY.
//   - A lock directory inside jobs/ would be counted by jobList's Total, which
//     is a plain count of directory entries. Off-by-one in a reported total is
//     the kind of thing that gets debugged twice.
const lockDirName = ".job-locks"

// jobLockTestHooks is a package-only seam for proving lock interleavings. It is
// nil in production. When enabled, acquisition first uses a non-blocking probe
// so a test can distinguish a genuinely contended flock from a goroutine that
// has merely reached the call site; it then falls back to the same blocking
// LOCK_EX used normally.
type jobLockTestHooks struct {
	acquired  func(path string)
	contended func(path string)
}

var jobLockTestSeam *jobLockTestHooks

// lockPath returns the lock file guarding a job directory.
//
// Derived from the job directory alone rather than taking the state dir as a
// parameter, because the supervisor is handed only a job path and must end up
// locking the same file the serving agent does.
func lockPath(jobDir string) string {
	jobsRoot := filepath.Dir(jobDir)
	return filepath.Join(filepath.Dir(jobsRoot), lockDirName, filepath.Base(jobDir)+".lock")
}

// withJobLock runs fn while holding an exclusive lock on a job's records.
//
// The lock file is opened fresh on every call and closed on return. That is not
// tidiness: a flock is owned by the open file description, so two goroutines
// sharing one cached descriptor would both believe they hold the lock and see
// none of each other's exclusion -- exactly the same-process race this exists to
// stop.
//
// The lock is granted for a job whose records are already gone; fn is expected to
// check. Refusing to lock a missing job would just move the race, since the check
// would again be outside the lock.
func withJobLock(jobDir string, fn func() error) error {
	path := lockPath(jobDir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := acquireJobLock(f, path); err != nil {
		return err
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)

	return fn()
}

func acquireJobLock(f *os.File, path string) error {
	seam := jobLockTestSeam
	if seam == nil {
		return syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	}

	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			return err
		}
		if seam.contended != nil {
			seam.contended(path)
		}
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
			return err
		}
	}
	if seam.acquired != nil {
		seam.acquired(path)
	}
	return nil
}

// jobExists reports whether a job's directory survived, and is how a locked
// section re-checks what it read outside the lock.
//
// The directory rather than meta.json, for two reasons. A waiter blocked on the
// lock still holds a descriptor to it after the winner removed the job, so it
// acquires the lock normally and needs something on disk to tell it the job is
// gone. And jobStart writes meta.json only after the supervisor is spawned, so a
// child that exits immediately can reach its status write before the record
// exists -- testing meta.json would silently discard that exit code, while the
// directory is already there from jobStart's MkdirAll.
//
// Since jobRm removes the directory while holding this same lock, the two can
// never interleave, so this is not itself a TOCTOU.
func jobExists(jobDir string) bool {
	st, err := os.Stat(jobDir)
	return err == nil && st.IsDir()
}

// removeJobLock discards a job's lock file after its records are gone.
//
// Best effort by design. A racer can recreate the file immediately after this
// unlink, which leaves one empty file behind; that is harmless, and preferable to
// holding the removal open to make it impossible.
func removeJobLock(jobDir string) {
	os.Remove(lockPath(jobDir))
}
