package agentinstall

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/state"
	"github.com/CIPFZ/rdev/internal/winutil"
)

const maxBinary = 64 << 20
const journalName = ".rdev-upgrade.json"
const currentName = ".rdev-release.json"
const previousName = ".rdev-previous-release.json"

type transaction struct {
	dir  string
	hook func(string) error
}

func (t *transaction) point(s string) error {
	if t.hook != nil {
		return t.hook(s)
	}
	return nil
}
func (t *transaction) read(name string, limit int64) ([]byte, error) {
	f, err := winutil.Open(filepath.Join(t.dir, name), os.O_RDONLY, true)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if int64(len(b)) > limit {
		return nil, errors.New("transaction file exceeds bound")
	}
	return b, err
}
func (t *transaction) write(name string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > 32<<10 {
		return errors.New("transaction record exceeds bound")
	}
	return winutil.AtomicWrite(filepath.Join(t.dir, name), b)
}
func (t *transaction) record() (Record, error) {
	var r Record
	b, err := t.read(journalName, 32<<10)
	if err == nil {
		err = artifact.Decode(b, &r)
	}
	if err == nil {
		err = validateRecord(r)
	}
	return r, err
}
func (t *transaction) save(r *Record, phase string) error {
	r.Phase = phase
	if err := t.write(journalName, r); err != nil {
		return err
	}
	return t.point(phase)
}
func (t *transaction) decision(name string) (artifact.Decision, error) {
	var d artifact.Decision
	b, err := t.read(name, 32<<10)
	if err == nil {
		err = artifact.Decode(b, &d)
	}
	if err == nil && !validDigest(d.Digest) {
		err = errors.New("invalid active release digest")
	}
	return d, err
}
func (t *transaction) binary(digest string) string {
	return filepath.Join(t.dir, "versions", "rdev-agent-"+digest+".exe")
}
func (t *transaction) checkBinary(digest string) error {
	if !validDigest(digest) {
		return errors.New("invalid version digest")
	}
	f, err := winutil.Open(t.binary(digest), os.O_RDONLY, true)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxBinary+1))
	if err != nil {
		return err
	}
	if len(b) > maxBinary || artifact.Hash(b) != digest {
		return errors.New("versioned agent bytes mismatch")
	}
	return nil
}
func acquire(ctx context.Context, dir string) (*transaction, func(), error) {
	if err := winutil.EnsurePrivateDir(dir); err != nil {
		return nil, nil, err
	}
	f, err := winutil.OpenLock(filepath.Join(dir, ".rdev-upgrade.lock"), os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, nil, err
	}
	if err = winutil.LockContext(ctx, f, true); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &transaction{dir: dir}, func() { _ = winutil.Unlock(f); _ = f.Close() }, nil
}

func Install(ctx context.Context, dir, candidate string, decision artifact.Decision, expectedOld string) error {
	return install(ctx, dir, candidate, decision, expectedOld, nil)
}
func install(ctx context.Context, dir, candidate string, decision artifact.Decision, expectedOld string, hook func(string) error) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	t, unlock, err := acquire(ctx, dir)
	if err != nil {
		return &Error{"not_sent", "lock", err}
	}
	defer unlock()
	t.hook = hook
	lease, err := state.AcquireWriter(dir)
	if err != nil {
		return &Error{"not_sent", "state_readiness", err}
	}
	defer lease.Close()
	if !validDigest(decision.Digest) || (expectedOld != "" && !validDigest(expectedOld)) {
		return &Error{"not_sent", "identity", errors.New("invalid digest")}
	}
	if err = t.recover(ctx); err != nil {
		return err
	}
	old, err := t.decision(currentName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return &Error{"not_sent", "installed_identity", err}
	}
	if old.Digest != expectedOld {
		return &Error{"not_sent", "prepare", errors.New("installed agent changed; re-probe required")}
	}
	if old.Digest != "" {
		if err = t.checkBinary(old.Digest); err != nil {
			return &Error{"not_sent", "previous_bytes", err}
		}
		if err = health(ctx, t.binary(old.Digest), dir, false); err != nil {
			return &Error{"not_sent", "previous_health", err}
		}
		if !old.Unsigned && decision.Unsigned {
			return &Error{"not_sent", "direction", errors.New("signed installation cannot become unsigned")}
		}
		if !old.Unsigned && !decision.Unsigned && old.Digest != decision.Digest {
			cmp, e := artifact.CompareVersions(decision.Version, old.Version)
			if e != nil || cmp == 0 || (cmp < 0 && !decision.Rollback) {
				return &Error{"not_sent", "direction", errors.New("release downgrade or version reuse denied")}
			}
		}
	}
	f, err := winutil.Open(candidate, os.O_RDONLY, true)
	if err != nil {
		return &Error{"not_sent", "candidate", err}
	}
	b, err := io.ReadAll(io.LimitReader(f, maxBinary+1))
	f.Close()
	if err != nil || len(b) > maxBinary || artifact.Hash(b) != decision.Digest {
		return &Error{"not_sent", "verify", errors.New("candidate bytes mismatch")}
	}
	versions := filepath.Join(dir, "versions")
	if err = winutil.EnsurePrivateDir(versions); err != nil {
		return &Error{"not_sent", "versions", err}
	}
	entries, err := os.ReadDir(versions)
	if err != nil {
		return &Error{"not_sent", "versions", err}
	}
	// Running images may be undeletable. Retain them and reject at a fixed disk
	// budget instead of terminating supervisors to make room for an upgrade.
	for _, entry := range entries {
		digest := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "rdev-agent-"), ".exe")
		if !validDigest(digest) || digest == old.Digest || digest == decision.Digest {
			continue
		}
		if previous, e := t.decision(previousName); e == nil && digest == previous.Digest {
			continue
		}
		if t.checkBinary(digest) == nil {
			_ = os.Remove(t.binary(digest))
		}
	}
	if _, err = os.Stat(t.binary(decision.Digest)); errors.Is(err, os.ErrNotExist) {
		entries, e := os.ReadDir(versions)
		if e != nil || len(entries) >= 8 {
			return &Error{"not_sent", "version_budget", errors.New("retained Windows agent versions exceed budget")}
		}
		if err = winutil.AtomicWrite(t.binary(decision.Digest), b); err != nil {
			return &Error{"not_sent", "stage", err}
		}
	}
	if err = t.checkBinary(decision.Digest); err != nil {
		return &Error{"not_sent", "verify", err}
	}
	r := Record{SchemaVersion: 1, Candidate: decision, PreviousDigest: old.Digest, OwnerPID: os.Getpid(), StartedAt: time.Now().UTC()}
	if err = t.save(&r, "prepared"); err != nil {
		return &Error{"not_sent", "prepare", err}
	}
	if err = Health(ctx, t.binary(decision.Digest), dir); err != nil {
		return &Error{"not_sent", "candidate_health", err}
	}
	if err = t.save(&r, "verified"); err != nil {
		return &Error{"not_sent", "verify", err}
	}
	if old.Digest != "" {
		if err = t.write(previousName, old); err != nil {
			return &Error{"not_sent", "previous_record", err}
		}
	}
	if err = t.save(&r, "switching"); err != nil {
		return &Error{"ambiguous", "switch_intent", err}
	}
	if err = t.write(currentName, decision); err != nil {
		return &Error{"ambiguous", "switch", err}
	}
	if err = t.point("published"); err != nil {
		return &Error{"ambiguous", "published", err}
	}
	if err = Health(ctx, t.binary(decision.Digest), dir); err != nil {
		return t.rollback(ctx, &r, err)
	}
	if err = t.save(&r, "committed"); err != nil {
		return &Error{"committed", "journal", err}
	}
	return nil
}

func (t *transaction) rollback(ctx context.Context, r *Record, cause error) error {
	if r.PreviousDigest == "" {
		return &Error{"ambiguous", "first_install_health", cause}
	}
	previous, err := t.decision(previousName)
	if err != nil || previous.Digest != r.PreviousDigest {
		return &Error{"ambiguous", "rollback_identity", errors.New("previous release record mismatch")}
	}
	if err = t.checkBinary(previous.Digest); err == nil {
		err = health(ctx, t.binary(previous.Digest), t.dir, false)
	}
	if err != nil {
		return &Error{"ambiguous", "rollback_health", err}
	}
	if err = t.write(currentName, previous); err != nil {
		return &Error{"ambiguous", "rollback_switch", err}
	}
	if err = t.save(r, "rolled_back"); err != nil {
		return &Error{"ambiguous", "rollback_record", err}
	}
	return &Error{"not_sent", "rolled_back", cause}
}
func (t *transaction) recover(ctx context.Context) error {
	r, err := t.record()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return &Error{"ambiguous", "journal", err}
	}
	if r.Phase == "committed" || r.Phase == "rolled_back" {
		want := r.Candidate.Digest
		if r.Phase == "rolled_back" {
			want = r.PreviousDigest
		}
		active, e := t.decision(currentName)
		if errors.Is(e, os.ErrNotExist) && want == "" {
			return nil
		}
		if e != nil || active.Digest != want {
			return &Error{"ambiguous", "committed_identity", errors.New("active release differs from terminal transaction")}
		}
		if e = t.checkBinary(want); e == nil {
			e = health(ctx, t.binary(want), t.dir, false)
		}
		if e != nil {
			return &Error{"ambiguous", "committed_health", e}
		}
		return nil
	}
	active, err := t.decision(currentName)
	if errors.Is(err, os.ErrNotExist) && r.PreviousDigest == "" {
		active = artifact.Decision{}
		err = nil
	}
	if err != nil {
		return &Error{"ambiguous", "active_record", err}
	}
	if active.Digest == r.Candidate.Digest {
		if err = t.checkBinary(active.Digest); err == nil {
			err = Health(ctx, t.binary(active.Digest), t.dir)
		}
		if err != nil {
			return t.rollback(ctx, &r, err)
		}
		if err = t.save(&r, "committed"); err != nil {
			return &Error{"committed", "recovery_record", err}
		}
		return nil
	}
	if active.Digest != r.PreviousDigest {
		return &Error{"ambiguous", "active_identity", errors.New("active release differs from transaction")}
	}
	return t.save(&r, "rolled_back")
}
func Recover(ctx context.Context, dir string) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	t, unlock, err := acquire(ctx, dir)
	if err != nil {
		return &Error{"not_sent", "lock", err}
	}
	defer unlock()
	lease, err := state.AcquireWriter(dir)
	if err != nil {
		return &Error{"not_sent", "state_readiness", err}
	}
	defer lease.Close()
	return t.recover(ctx)
}
