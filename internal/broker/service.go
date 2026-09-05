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
type Service struct{ client *client.Client }

func NewService(lookup client.AgentLookup) *Service { return &Service{client: client.New(lookup)} }

// Client exposes the broker-owned client for request dispatch and lifecycle
// integration. It is intentionally stable for the lifetime of Service.
func (s *Service) Client() *client.Client { return s.client }

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
