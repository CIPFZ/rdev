package broker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Approval struct {
	Token, Digest, Owner string
	ExpiresAt            time.Time
}

func NewApproval(owner, operation, target string, ttl time.Duration) (Approval, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return Approval{}, err
	}
	d := sha256.Sum256([]byte(owner + "\x00" + operation + "\x00" + target))
	return Approval{Token: hex.EncodeToString(b), Digest: hex.EncodeToString(d[:]), Owner: owner, ExpiresAt: time.Now().Add(ttl)}, nil
}
func (a Approval) Validate(owner, operation, target string, now time.Time) error {
	d := sha256.Sum256([]byte(owner + "\x00" + operation + "\x00" + target))
	if a.Owner != owner || a.Digest != hex.EncodeToString(d[:]) {
		return errors.New("approval does not match request")
	}
	if !now.Before(a.ExpiresAt) {
		return errors.New("approval expired")
	}
	return nil
}
