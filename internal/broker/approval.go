package broker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
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
func (a Approval) Validate(token, owner, operation, target string, now time.Time) error {
	if token == "" || token != a.Token {
		return errors.New("approval token mismatch")
	}
	d := sha256.Sum256([]byte(owner + "\x00" + operation + "\x00" + target))
	if a.Owner != owner || a.Digest != hex.EncodeToString(d[:]) {
		return errors.New("approval does not match request")
	}
	if !now.Before(a.ExpiresAt) {
		return errors.New("approval expired")
	}
	return nil
}

// ApprovalStore consumes each token once, while keeping the digest-bound
// approval value immutable for audit and validation.
type ApprovalStore struct {
	mu   sync.Mutex
	used map[string]struct{}
}

func NewApprovalStore() *ApprovalStore { return &ApprovalStore{used: make(map[string]struct{})} }

func (s *ApprovalStore) Consume(a Approval, token, owner, operation, target string, now time.Time) error {
	if err := a.Validate(token, owner, operation, target, now); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.used[token]; ok {
		return errors.New("approval token already used")
	}
	s.used[token] = struct{}{}
	return nil
}
