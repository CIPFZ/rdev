package broker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type Approval struct {
	Token     string        `json:"token"`
	Digest    string        `json:"digest"`
	Owner     string        `json:"owner"`
	ExpiresAt time.Time     `json:"expires_at"`
	Plan      *ApprovalPlan `json:"plan,omitempty"`
}

func NewApproval(owner, operation, target string, ttl time.Duration) (Approval, error) {
	if ttl <= 0 || ttl > 10*time.Minute {
		return Approval{}, errors.New("approval ttl must be between zero and ten minutes")
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return Approval{}, err
	}
	d := approvalDigest(owner, operation, target)
	return Approval{Token: hex.EncodeToString(b), Digest: hex.EncodeToString(d[:]), Owner: owner, ExpiresAt: time.Now().Add(ttl)}, nil
}
func (a Approval) Validate(token, owner, operation, target string, now time.Time) error {
	if token == "" || token != a.Token {
		return errors.New("approval token mismatch")
	}
	d := approvalDigest(owner, operation, target)
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
	used map[string]time.Time
}

func NewApprovalStore() *ApprovalStore { return &ApprovalStore{used: make(map[string]time.Time)} }

func (s *ApprovalStore) Consume(a Approval, token, owner, operation, target string, now time.Time) error {
	if err := a.Validate(token, owner, operation, target, now); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.used[token]; ok {
		return errors.New("approval token already used")
	}
	for prior, expiry := range s.used {
		if !now.Before(expiry) {
			delete(s.used, prior)
		}
	}
	s.used[token] = a.ExpiresAt
	return nil
}

func approvalDigest(owner, operation, target string) [32]byte {
	data, _ := json.Marshal([3]string{owner, operation, target})
	return sha256.Sum256(data)
}
