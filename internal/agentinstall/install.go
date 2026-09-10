//go:build linux || darwin

// Package agentinstall owns a bounded, crash-reconcilable binary transaction.
// It never migrates state, replays operations, or signals an existing agent/job.
package agentinstall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/state"
	"golang.org/x/sys/unix"
)

const maxBinary = 64 << 20
const journalName = ".rdev-upgrade.json"
const currentName = ".rdev-release.json"
const previousName = ".rdev-agent.previous"
const rollbackName = ".rdev-agent.rollback"
const newName = ".rdev-agent.new"
const targetName = "rdev-agent"

type transaction struct {
	root *os.Root
	dir  string
	fd   *os.File
	hook func(string) error
}

func (t *transaction) point(s string) error {
	if t.hook != nil {
		return t.hook(s)
	}
	return nil
}
func (t *transaction) open(name string, flags int, mode uint32) (*os.File, error) {
	fd, e := unix.Openat(int(t.fd.Fd()), name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, mode)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), name)
	st, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, e
	}
	var stat unix.Stat_t
	if e = unix.Fstat(fd, &stat); e != nil || stat.Uid != uint32(os.Getuid()) || !st.Mode().IsRegular() || st.Mode().Perm()&0022 != 0 {
		f.Close()
		return nil, errors.New("unsafe transaction file")
	}
	return f, nil
}
func (t *transaction) read(name string, limit int64) ([]byte, error) {
	f, e := t.open(name, unix.O_RDONLY, 0)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(b)) > limit {
		return nil, errors.New("transaction file exceeds bound")
	}
	return b, e
}
func (t *transaction) digest(name string) (string, error) {
	b, e := t.read(name, maxBinary)
	if errors.Is(e, os.ErrNotExist) {
		return "", nil
	}
	if e != nil {
		return "", e
	}
	return artifact.Hash(b), nil
}
func (t *transaction) write(name string, value any) error {
	b, e := json.Marshal(value)
	if e != nil {
		return e
	}
	if len(b) > 32<<10 {
		return errors.New("upgrade record exceeds budget")
	}
	// Fixed scratch files are touched only with the target lock held. Refuse
	// unexpected objects; a crashed partial scratch may be removed after fencing.
	tmp := name + ".tmp"
	if _, e = t.root.Lstat(tmp); e == nil {
		if _, e = t.read(tmp, 32<<10); e != nil {
			return e
		}
		if e = t.root.Remove(tmp); e != nil {
			return e
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	f, e := t.open(tmp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(append(b, '\n'))
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = t.root.Rename(tmp, name); e != nil {
		return e
	}
	return t.fd.Sync()
}
func (t *transaction) remove(name string) error {
	st, e := t.root.Lstat(name)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	if !st.Mode().IsRegular() {
		return errors.New("unsafe cleanup object")
	}
	return t.root.Remove(name)
}
func (t *transaction) record() (Record, error) {
	var r Record
	b, e := t.read(journalName, 32<<10)
	if e != nil {
		return r, e
	}
	if e = artifact.Decode(b, &r); e != nil {
		return r, e
	}
	if e = validateRecord(r); e != nil {
		return r, e
	}
	return r, nil
}

func (t *transaction) save(r *Record, phase string) error {
	r.Phase = phase
	if e := t.write(journalName, r); e != nil {
		return e
	}
	return t.point(phase)
}

// Health performs a real unary hello against the given binary. It only sends
// ping, requires the actual platform/features and checks state without migration.

func acquire(ctx context.Context, dir string) (*transaction, func(), error) {
	if !filepath.IsAbs(dir) {
		return nil, nil, errors.New("absolute installation root required")
	}
	st, e := os.Lstat(dir)
	if e != nil {
		return nil, nil, e
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm() != 0700 {
		return nil, nil, errors.New("installation root must be private real directory")
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		return nil, nil, e
	}
	fd, e := root.Open(".")
	if e != nil {
		root.Close()
		return nil, nil, e
	}
	t := &transaction{root: root, dir: dir, fd: fd}
	closeRoot := func() { fd.Close(); root.Close() }
	lock, e := t.open(".rdev-upgrade.lock", unix.O_RDWR|unix.O_CREAT, 0600)
	if e != nil {
		closeRoot()
		return nil, nil, e
	}
	// Kernel lease lifetime is the open file description. Never steal by age/PID
	// or unlink the lock; SIGKILL releases it, and aliases share this same inode.
	for {
		e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if e == nil {
			break
		}
		if !errors.Is(e, unix.EWOULDBLOCK) && !errors.Is(e, unix.EAGAIN) {
			lock.Close()
			closeRoot()
			return nil, nil, e
		}
		select {
		case <-ctx.Done():
			lock.Close()
			closeRoot()
			return nil, nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return t, func() { _ = unix.Flock(int(lock.Fd()), unix.LOCK_UN); lock.Close(); closeRoot() }, nil
}

// Install is called by the hash-verified candidate staged by the existing secure
// SSH uploader. expectedOld is a CAS precondition checked under the remote lock.
func Install(ctx context.Context, dir, candidate string, decision artifact.Decision, expectedOld string) error {
	return install(ctx, dir, candidate, decision, expectedOld, nil)
}
func install(ctx context.Context, dir, candidate string, decision artifact.Decision, expectedOld string, hook func(string) error) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	t, unlock, e := acquire(ctx, dir)
	if e != nil {
		return &Error{"not_sent", "lock", e}
	}
	defer unlock()
	stateLease, err := state.AcquireWriter(dir)
	if err != nil {
		return &Error{"not_sent", "state_readiness", err}
	}
	defer stateLease.Close()
	t.hook = hook
	t.cleanupStaleUploads(filepath.Dir(candidate))
	if !validDigest(decision.Digest) || (expectedOld != "" && !validDigest(expectedOld)) {
		return &Error{"not_sent", "identity", errors.New("invalid digest")}
	}
	if e = t.recover(ctx); e != nil {
		return e
	}
	old, e := t.digest(targetName)
	if e != nil {
		return &Error{"not_sent", "prepare", e}
	}
	if old != expectedOld {
		return &Error{"not_sent", "prepare", errors.New("installed agent changed; re-probe required")}
	}
	if old != "" {
		if e = health(ctx, filepath.Join(dir, targetName), dir, false); e != nil {
			return &Error{"not_sent", "previous_health", e}
		}
		if b, err := t.read(currentName, 32<<10); err == nil {
			var current artifact.Decision
			if err = artifact.Decode(b, &current); err != nil || current.Digest != old {
				return &Error{"not_sent", "installed_identity", errors.New("installed release record mismatch")}
			}
			if !current.Unsigned && decision.Unsigned {
				return &Error{"not_sent", "direction", errors.New("signed installation cannot become unsigned")}
			}
			if !decision.Unsigned && !current.Unsigned && old != decision.Digest {
				cmp, err := artifact.CompareVersions(decision.Version, current.Version)
				if err != nil || cmp == 0 || (cmp < 0 && !decision.Rollback) {
					return &Error{"not_sent", "direction", errors.New("release downgrade or version reuse denied")}
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return &Error{"not_sent", "installed_identity", err}
		}
	}
	if old == decision.Digest {
		// Detached signatures do not change bytes. Persist signed activation even
		// on an identical-binary path, and never downgrade existing trust to unsigned.
		if e = t.write(currentName, decision); e != nil {
			return &Error{"ambiguous", "trust_activation", e}
		}
		r, err := t.record()
		if errors.Is(err, os.ErrNotExist) {
			r = Record{SchemaVersion: 1, OwnerPID: os.Getpid(), StartedAt: time.Now().UTC()}
		} else if err != nil {
			return &Error{"ambiguous", "trust_activation", err}
		}
		r.Candidate = decision
		if e = t.save(&r, "committed"); e != nil {
			return &Error{"ambiguous", "trust_activation", e}
		}
		return nil
	}
	b, e := artifact.ReadFile(filepath.Dir(candidate), filepath.Base(candidate), maxBinary)
	if e != nil || len(b) > maxBinary || artifact.Hash(b) != decision.Digest {
		return &Error{"not_sent", "verify", errors.New("candidate bytes mismatch")}
	}
	r := Record{SchemaVersion: 1, Candidate: decision, PreviousDigest: old, OwnerPID: os.Getpid(), StartedAt: time.Now().UTC()}
	if e = t.save(&r, "prepared"); e != nil {
		return &Error{"not_sent", "prepare", e}
	}
	for _, name := range []string{newName, rollbackName} {
		if e = t.remove(name); e != nil {
			return &Error{"not_sent", "prepare", e}
		}
	}
	f, e := t.open(newName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0700)
	if e != nil {
		return &Error{"not_sent", "stage", e}
	}
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return &Error{"not_sent", "stage", e}
	}
	if e = Health(ctx, filepath.Join(dir, newName), dir); e != nil {
		return &Error{"not_sent", "health", e}
	}
	if old != "" {
		if e = unix.Linkat(int(t.fd.Fd()), targetName, int(t.fd.Fd()), rollbackName, 0); e != nil {
			return &Error{"not_sent", "backup", e}
		}
	}
	if e = t.save(&r, "verified"); e != nil {
		return &Error{"not_sent", "verify", e}
	}
	// Durably record intent BEFORE the atomic publication window.
	if e = t.save(&r, "switching"); e != nil {
		return &Error{"not_sent", "switch", e}
	}
	if actual, err := t.digest(targetName); err != nil || actual != old {
		return &Error{"ambiguous", "switch", errors.New("target replaced before switch")}
	}
	if e = t.root.Rename(newName, targetName); e != nil {
		return &Error{"ambiguous", "switch", e}
	}
	if e = t.fd.Sync(); e != nil {
		return &Error{"ambiguous", "switch_sync", e}
	}
	if e = t.point("published"); e != nil {
		return &Error{"ambiguous", "published", e}
	}
	if got, err := t.digest(targetName); err != nil || got != decision.Digest {
		return &Error{"ambiguous", "verify_installed", errors.New("installed bytes mismatch")}
	}
	if e = Health(ctx, filepath.Join(dir, targetName), dir); e != nil {
		return t.rollback(ctx, &r, e)
	}
	return t.commit(&r)
}
func (t *transaction) commit(r *Record) error {
	if e := t.write(currentName, r.Candidate); e != nil {
		return &Error{"ambiguous", "record", e}
	}
	if e := t.save(r, "committed"); e != nil {
		return &Error{"committed", "record", e}
	}
	if r.PreviousDigest != "" {
		got, e := t.digest(rollbackName)
		if e != nil || got != r.PreviousDigest {
			return &Error{"committed", "backup", errors.New("backup digest mismatch")}
		}
		if e = t.root.Rename(rollbackName, previousName); e != nil {
			return &Error{"committed", "cleanup", e}
		}
	}
	if e := t.remove(newName); e != nil {
		return &Error{"committed", "cleanup", e}
	}
	if e := t.fd.Sync(); e != nil {
		return &Error{"committed", "cleanup_sync", e}
	}
	return nil
}
func (t *transaction) rollback(ctx context.Context, r *Record, cause error) error {
	if r.PreviousDigest == "" {
		return &Error{"ambiguous", "rollback", errors.New("first installation health failed after switch; recovery required")}
	}
	old, e := t.digest(rollbackName)
	if e != nil || old != r.PreviousDigest {
		return &Error{"ambiguous", "rollback", errors.New("rollback bytes unavailable")}
	}
	if e = health(ctx, filepath.Join(t.dir, rollbackName), t.dir, false); e != nil {
		return &Error{"ambiguous", "rollback_state", e}
	}
	got, e := t.digest(targetName)
	if e != nil || got != r.Candidate.Digest {
		return &Error{"ambiguous", "rollback", errors.New("active version changed")}
	}
	if e = t.root.Rename(rollbackName, targetName); e != nil {
		return &Error{"ambiguous", "rollback", e}
	}
	if e = t.fd.Sync(); e != nil {
		return &Error{"ambiguous", "rollback_sync", e}
	}
	if e = health(ctx, filepath.Join(t.dir, targetName), t.dir, false); e != nil {
		return &Error{"ambiguous", "rollback_health", e}
	}
	if e = t.save(r, "rolled_back"); e != nil {
		return &Error{"ambiguous", "rollback_record", e}
	}
	return &Error{"not_sent", "rolled_back", cause}
}
func (t *transaction) recover(ctx context.Context) error {
	r, e := t.record()
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return &Error{"ambiguous", "recovery", e}
	}
	active, e := t.digest(targetName)
	if e != nil {
		return &Error{"ambiguous", "recovery", e}
	}
	switch r.Phase {
	case "prepared", "verified", "rolled_back":
		if active != r.PreviousDigest {
			return &Error{"ambiguous", "recovery", errors.New("pre-switch active version changed")}
		}
		for _, n := range []string{newName, rollbackName} {
			if e = t.remove(n); e != nil {
				return &Error{"not_sent", "recovery_cleanup", e}
			}
		}
		return nil
	case "switching":
		if active == r.PreviousDigest {
			for _, n := range []string{newName, rollbackName} {
				if e = t.remove(n); e != nil {
					return &Error{"not_sent", "recovery_cleanup", e}
				}
			}
			return nil
		}
		if active != r.Candidate.Digest {
			return &Error{"ambiguous", "recovery", errors.New("unknown active version")}
		}
		if e = Health(ctx, filepath.Join(t.dir, targetName), t.dir); e != nil {
			return t.rollback(ctx, &r, e)
		}
		return t.commit(&r)
	case "committed":
		if active != r.Candidate.Digest {
			return &Error{"ambiguous", "recovery", errors.New("committed bytes changed")}
		}
		// Cleanup is idempotent; retain exactly one known-good previous inode.
		if b, e := t.digest(rollbackName); e != nil {
			return &Error{"committed", "cleanup", e}
		} else if b != "" {
			if b != r.PreviousDigest {
				return &Error{"committed", "cleanup", errors.New("backup changed")}
			}
			if e = t.root.Rename(rollbackName, previousName); e != nil {
				return &Error{"committed", "cleanup", e}
			}
		}
		return nil
	}
	return &Error{"ambiguous", "recovery", errors.New("unknown transaction")}
}

// Marker deliberately contains no paths, keys, commands or raw peer output.

// DecodeDecision is only an input bound; authority was checked locally before
// execution over host-key-verified SSH. The remote helper additionally checks bytes.

// Four upload reservations bound crashed bootstrap disk use before the helper
// can take its kernel lock. Only a dead owner, a ten-minute age, exact private
// file layout and an inactive namespace transaction permit reclamation.
func (t *transaction) cleanupStaleUploads(protected string) {
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf(".rdev-upload-slot-%d", i)
		path := filepath.Join(t.dir, name)
		if filepath.Clean(path) == filepath.Clean(protected) {
			continue
		}
		st, e := t.root.Lstat(name)
		if e != nil || !st.IsDir() || st.Mode().Perm() != 0700 || time.Since(st.ModTime()) < 10*time.Minute {
			continue
		}
		stage, e := t.root.OpenRoot(name)
		if e != nil {
			continue
		}
		func() {
			defer stage.Close()
			ownerInfo, e := stage.Lstat("owner")
			if e != nil || !ownerInfo.Mode().IsRegular() || ownerInfo.Size() > 32 || ownerInfo.Mode().Perm() != 0600 {
				return
			}
			raw, e := stage.ReadFile("owner")
			if e != nil {
				return
			}
			pid, e := strconv.Atoi(strings.TrimSpace(string(raw)))
			if e != nil || pid <= 0 {
				return
			}
			if e = syscall.Kill(pid, 0); !errors.Is(e, syscall.ESRCH) {
				return
			}
			d, e := stage.Open(".")
			if e != nil {
				return
			}
			entries, e := d.ReadDir(5)
			d.Close()
			if e != nil && e != io.EOF {
				return
			}
			if len(entries) > 3 {
				return
			}
			for _, entry := range entries {
				if entry.Name() != "owner" && entry.Name() != "agent" && entry.Name() != "ready" {
					return
				}
				info, e := stage.Lstat(entry.Name())
				if e != nil || !info.Mode().IsRegular() {
					return
				}
			}
			for _, entry := range entries {
				if e = stage.Remove(entry.Name()); e != nil {
					return
				}
			}
			_ = t.root.Remove(name)
		}()
	}
}

// Recover requires an already installed, trusted helper. It can release stale
// upload reservations without allocating another one; unknown first-bootstrap
// debris requires administrator recovery before any executable exists.
func Recover(ctx context.Context, dir string) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	t, unlock, e := acquire(ctx, dir)
	if e != nil {
		return &Error{"not_sent", "lock", e}
	}
	defer unlock()
	lease, e := state.AcquireWriter(dir)
	if e != nil {
		return &Error{"not_sent", "state_readiness", e}
	}
	defer lease.Close()
	if e = t.recover(ctx); e != nil {
		return e
	}
	t.cleanupStaleUploads("")
	return nil
}
