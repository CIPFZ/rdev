package broker

import (
	"context"
	"errors"
	"strconv"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/transport"
)

// Service is the single owner of connection and secret state for local broker
// clients. Callers must share one Service instead of constructing one Client
// per frontend process.
type Service struct {
	client *client.Client
	policy *Policy
}

func NewService(lookup client.AgentLookup) *Service {
	return &Service{client: client.New(lookup), policy: NewPolicy()}
}

// Client exposes the broker-owned client for request dispatch and lifecycle
// integration. It is intentionally stable for the lifetime of Service.
func (s *Service) Client() *client.Client { return s.client }

func (s *Service) Decide(owner Owner, operation string) Decision {
	if err := owner.Validate(); err != nil {
		return Decision{Reason: err.Error()}
	}
	return s.policy.Decide(owner.Key(), operation)
}

func (s *Service) Grant(owner Owner, operation string) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	s.policy.Grant(owner.Key(), operation)
	return nil
}

// SharedConnectionKey is the canonical identity used before a host is pooled.
func SharedConnectionKey(h transport.Host) (string, error) {
	if err := transport.ValidateHost(h); err != nil {
		return "", err
	}
	return h.Addr + ":" + strconv.Itoa(h.Port) + ":" + h.RemoteDir, nil
}

var ErrClosed = errors.New("broker service is closed")

func (s *Service) Close(ctx context.Context) error {
	_ = ctx
	if s == nil || s.client == nil {
		return ErrClosed
	}
	return nil
}
