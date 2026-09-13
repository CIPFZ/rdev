// Package agentrepair defines the explicit authorization and transaction
// contract required before an agent repair may mutate a remote installation.
package agentrepair

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

type Phase string

const (
	Planned    Phase = "planned"
	Authorized Phase = "authorized"
	Prepared   Phase = "prepared"
	Committed  Phase = "committed"
	Aborted    Phase = "aborted"
)

type Plan struct{ Host, CurrentDigest, CandidateDigest string }
type Transaction struct {
	ID, PlanDigest string
	Phase          Phase
	SnapshotDigest string
}

type ReconnectPolicy struct {
	DedicatedKeyOnly  bool
	DisableAgentCache bool
}

func (p ReconnectPolicy) Validate() error {
	if !p.DedicatedKeyOnly || !p.DisableAgentCache {
		return errors.New("repair reconnect requires dedicated-key-only authentication without caches")
	}
	return nil
}

func Digest(p Plan) string {
	h := sha256.Sum256([]byte(p.Host + "\x00" + p.CurrentDigest + "\x00" + p.CandidateDigest))
	return hex.EncodeToString(h[:])
}

func DigestBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func (p Plan) Validate() error {
	if p.Host == "" || p.CurrentDigest == "" || p.CandidateDigest == "" {
		return errors.New("repair plan requires host and both digests")
	}
	for _, d := range []string{p.CurrentDigest, p.CandidateDigest} {
		if len(d) != 64 || strings.Trim(d, "0123456789abcdef") != "" {
			return errors.New("repair plan digest must be sha256")
		}
	}
	return nil
}

// ValidateCandidate binds the bytes observed during the preflight to the
// immutable plan. It is deliberately byte exact; a reconnect must re-check
// the same candidate instead of trusting a version label.
func (p Plan) ValidateCandidate(current, candidate []byte) error {
	if err := p.Validate(); err != nil {
		return err
	}
	cur := sha256.Sum256(current)
	cand := sha256.Sum256(candidate)
	if hex.EncodeToString(cur[:]) != p.CurrentDigest || hex.EncodeToString(cand[:]) != p.CandidateDigest {
		return errors.New("repair candidate bytes do not match planned digests")
	}
	if bytes.Equal(current, candidate) {
		return errors.New("repair candidate is identical to current agent")
	}
	return nil
}

func (t Transaction) Authorize(p Plan, confirm bool) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if t.ID == "" || t.PlanDigest != Digest(p) {
		return errors.New("repair authorization is not bound to this plan")
	}
	if !confirm {
		return errors.New("repair requires explicit authorization")
	}
	if t.Phase != Planned {
		return fmt.Errorf("repair authorization requires planned transaction, got %q", t.Phase)
	}
	return nil
}

func (t *Transaction) Advance(next Phase) error {
	if t == nil {
		return errors.New("nil repair transaction")
	}
	valid := (t.Phase == Planned && next == Authorized) || (t.Phase == Authorized && next == Prepared) || (t.Phase == Prepared && next == Committed) || (next == Aborted && t.Phase != Committed && t.Phase != Aborted)
	if !valid {
		return fmt.Errorf("invalid repair transition %q -> %q", t.Phase, next)
	}
	t.Phase = next
	return nil
}

func (t *Transaction) Abort() error {
	return t.Advance(Aborted)
}

// Prepare records the exact pre-repair snapshot after authorization. It keeps
// the transaction bound to bytes observed immediately before mutation.
func (t *Transaction) Prepare(p Plan, current []byte) error {
	if t == nil || t.Phase != Authorized {
		return errors.New("repair prepare requires authorized transaction")
	}
	if err := p.Validate(); err != nil {
		return err
	}
	h := sha256.Sum256(current)
	if hex.EncodeToString(h[:]) != p.CurrentDigest {
		return errors.New("repair snapshot digest mismatch")
	}
	t.SnapshotDigest = p.CurrentDigest
	return t.Advance(Prepared)
}
