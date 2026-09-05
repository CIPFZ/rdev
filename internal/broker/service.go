package broker

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

type ProtocolDispatcher interface {
	DoProtocol(context.Context, string, *proto.Request) (*proto.Response, error)
}

// Service is the single owner of connection and secret state for local broker
// clients. Callers must share one Service instead of constructing one Client
// per frontend process.
type Service struct {
	client          *client.Client
	policy          *Policy
	lease           *Lease
	closed          atomic.Bool
	Quota           *Quota
	Lanes           *Lanes
	Watches         *WatchHub
	Audit           *AuditLog
	config          *ConfigStore
	approvals       *ApprovalStore
	approvalMu      sync.Mutex
	approvalByToken map[string]Approval
}

func NewService(lookup client.AgentLookup) *Service {
	config, _ := NewConfigStore(Config{MaxHosts: 128, IdleTTL: 5 * time.Minute})
	return &Service{client: client.New(lookup), policy: NewPolicy(), lease: NewLease(30 * time.Second), Quota: NewQuota(12, 4, 256), Lanes: NewLanes(2, 8, 1), Watches: NewWatchHub(), Audit: NewAuditLog(1024), config: config, approvals: NewApprovalStore(), approvalByToken: make(map[string]Approval)}
}

// Client exposes the broker-owned client for request dispatch and lifecycle
// integration. It is intentionally stable for the lifetime of Service.
func (s *Service) Client() *client.Client { return s.client }
func (s *Service) Dispatch(ctx context.Context, host string, req *proto.Request) (*proto.Response, error) {
	if s.closed.Load() {
		return nil, errors.New("broker service closed")
	}
	return s.client.DoProtocol(ctx, host, req)
}
func (s *Service) BeginRequest() bool { return s.config != nil && s.config.BeginRequest() }
func (s *Service) EndRequest() {
	if s.config != nil {
		s.config.EndRequest()
	}
}
func (s *Service) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if s.config == nil {
		return nil
	}
	return s.config.Drain(ctx)
}
func (s *Service) AttachClient() bool {
	if s.closed.Load() {
		return false
	}
	s.lease.Attach()
	return true
}
func (s *Service) DetachClient()               { s.lease.Detach() }
func (s *Service) Reapable(now time.Time) bool { return s.lease.Reapable(now) }

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

func (s *Service) CreateApproval(owner Owner, operation, target string, ttl time.Duration) (Approval, error) {
	if err := owner.Validate(); err != nil {
		return Approval{}, err
	}
	a, err := NewApproval(owner.Key(), operation, target, ttl)
	if err != nil {
		return Approval{}, err
	}
	s.approvalMu.Lock()
	s.approvalByToken[a.Token] = a
	s.approvalMu.Unlock()
	return a, nil
}

func (s *Service) ConsumeApproval(token, owner, operation, target string) error {
	s.approvalMu.Lock()
	a, ok := s.approvalByToken[token]
	if ok {
		delete(s.approvalByToken, token)
	}
	s.approvalMu.Unlock()
	if !ok {
		return errors.New("approval token unknown")
	}
	return s.approvals.Consume(a, token, owner, operation, target, time.Now())
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
	if s.closed.Swap(true) {
		return ErrClosed
	}
	_ = s.Drain(ctx)
	s.client.Close()
	return nil
}
