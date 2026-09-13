// Package agentrepair defines the explicit authorization and transaction
// contract required before an agent repair may mutate a remote installation.
package agentrepair

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
}

func Digest(p Plan) string {
	h := sha256.Sum256([]byte(p.Host + "\x00" + p.CurrentDigest + "\x00" + p.CandidateDigest))
	return hex.EncodeToString(h[:])
}

func (p Plan) Validate() error {
	if p.Host == "" || p.CurrentDigest == "" || p.CandidateDigest == "" {
		return errors.New("repair plan requires host and both digests")
	}
	for _, d := range []string{p.CurrentDigest, p.CandidateDigest} {
		if len(d) != 64 {
			return errors.New("repair plan digest must be sha256")
		}
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
