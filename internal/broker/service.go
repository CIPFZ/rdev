package broker

import (
	"context"
	"errors"
	"strconv"
	"strings"
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
	client           *client.Client
	policy           *Policy
	lease            *Lease
	closed           atomic.Bool
	Scheduler        *Scheduler
	Watches          *WatchHub
	Audit            *AuditLog
	config           *ConfigStore
	approvalMu       sync.Mutex
	approvalByToken  map[string]Approval
	readiness        Readiness
	sharedMu         sync.Mutex
	shared           map[sharedKey]*sharedDispatch
	observationCtx   context.Context
	stopObservations context.CancelFunc
	Jobs             *JobRegistry
	Mutations        *MutationRegistry
	Principals       PrincipalAuthority
	dispatchMu       sync.RWMutex
	dispatchOverride func(context.Context, string, *proto.Request) (*proto.Response, error)
}

type sharedDispatch struct {
	done        chan struct{}
	resp        *proto.Response
	err         error
	subscribers int
}

type sharedKey struct{ owner, request string }
type SharedWaitStatus struct {
	Observers   int `json:"observers"`
	Subscribers int `json:"subscribers"`
}

const maxSharedSubscribers = 512
const maxOwnerSubscribers = 128

func NewService(lookup client.AgentLookup) *Service {
	config, _ := NewConfigStore(Config{MaxHosts: 128, IdleTTL: 5 * time.Minute})
	observationCtx, stopObservations := context.WithCancel(context.Background())
	s := &Service{client: client.New(lookup), policy: NewPolicy(), lease: NewLease(30 * time.Second), Scheduler: NewScheduler(QoSConfig{}, 128), Watches: NewWatchHub(), Audit: NewAuditLog(1024), config: config, approvalByToken: make(map[string]Approval), shared: make(map[sharedKey]*sharedDispatch), Jobs: NewJobRegistry(), observationCtx: observationCtx, stopObservations: stopObservations}
	s.SetReady(true)
	s.Mutations = NewMutationRegistry()
	return s
}

// Client exposes the broker-owned client for request dispatch and lifecycle
// integration. It is intentionally stable for the lifetime of Service.
func (s *Service) Client() *client.Client { return s.client }
func (s *Service) SetReady(v bool)        { s.readiness.SetReady(v) }
func (s *Service) Ready() bool            { return s.readiness.Ready() }
func (s *Service) Dispatch(ctx context.Context, host string, req *proto.Request) (*proto.Response, error) {
	if s.closed.Load() {
		return nil, errors.New("broker service closed")
	}
	s.dispatchMu.RLock()
	override := s.dispatchOverride
	s.dispatchMu.RUnlock()
	if override != nil {
		return override(ctx, host, req)
	}
	if LaneForOperation(req.Op) == LaneBulk {
		return s.client.DoProtocolBulk(ctx, host, req, "")
	}
	return s.client.DoProtocol(ctx, host, req)
}
func (s *Service) DispatchApproved(ctx context.Context, host string, req *proto.Request, target string) (*proto.Response, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	current, err := s.client.ProtocolTargetIdentity(host)
	if err != nil || current != target {
		return nil, errors.New("approved target changed before dispatch")
	}
	s.dispatchMu.RLock()
	override := s.dispatchOverride
	s.dispatchMu.RUnlock()
	if override != nil {
		return override(ctx, host, req)
	}
	if LaneForOperation(req.Op) == LaneBulk {
		return s.client.DoProtocolBulk(ctx, host, req, target)
	}
	return s.client.DoProtocolApproved(ctx, host, req, target)
}
func (s *Service) SetDispatcher(fn func(context.Context, string, *proto.Request) (*proto.Response, error)) {
	s.dispatchMu.Lock()
	s.dispatchOverride = fn
	s.dispatchMu.Unlock()
}
func (s *Service) DispatchScheduled(ctx context.Context, host, owner string, lane Lane, fn func(context.Context) (*proto.Response, error)) (*proto.Response, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	return s.Scheduler.Do(ctx, host, owner, lane, fn)
}
func (s *Service) DispatchFair(ctx context.Context, owner string, lane Lane, fn func() (*proto.Response, error)) (*proto.Response, error) {
	return s.DispatchScheduled(ctx, "", owner, lane, func(context.Context) (*proto.Response, error) { return fn() })
}
func (s *Service) SetOwnerWeight(owner string, weight int) { s.Scheduler.SetOwnerWeight(owner, weight) }
func (s *Service) SubscribeJob(key string) (<-chan any, func()) {
	return s.Watches.Subscribe(key)
}

// DispatchShared coalesces concurrent observations of the same detached job
// set. Only the first caller performs remote work; followers receive its result.
func (s *Service) DispatchShared(ctx context.Context, owner, request string, fn func(context.Context) (*proto.Response, error)) (*proto.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := sharedKey{owner, request}
	s.sharedMu.Lock()
	if s.closed.Load() {
		s.sharedMu.Unlock()
		return nil, ErrClosed
	}
	ownerObservers, ownerSubscribers, totalSubscribers := 0, 0, 0
	for k, current := range s.shared {
		totalSubscribers += current.subscribers
		if k.owner == owner {
			ownerObservers++
			ownerSubscribers += current.subscribers
		}
	}
	if totalSubscribers >= maxSharedSubscribers || ownerSubscribers >= maxOwnerSubscribers {
		s.sharedMu.Unlock()
		return nil, ErrQueueFull
	}
	current := s.shared[key]
	if current == nil {
		limits := s.config.Get().QoS.effective()
		if len(s.shared) >= limits.MaxActive+limits.MaxQueued || ownerObservers >= limits.PerOwner+limits.PerOwnerQueued {
			s.sharedMu.Unlock()
			return nil, ErrQueueFull
		}
		// The observer has its own lease and drain accounting. Losing every
		// frontend must neither release its active transport nor strand shutdown.
		if !s.BeginRequest() {
			s.sharedMu.Unlock()
			return nil, ErrClosed
		}
		current = &sharedDispatch{done: make(chan struct{})}
		s.shared[key] = current
		go func() {
			defer s.EndRequest()
			current.resp, current.err = fn(s.observationCtx)
			if current.err == nil && current.resp != nil {
				s.Watches.Publish(owner+"\x00"+request, current.resp)
			}
			s.sharedMu.Lock()
			delete(s.shared, key)
			close(current.done)
			s.sharedMu.Unlock()
		}()
	}
	current.subscribers++
	s.sharedMu.Unlock()
	defer func() { s.sharedMu.Lock(); current.subscribers--; s.sharedMu.Unlock() }()
	select {
	case <-current.done:
		return current.resp, current.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) SharedWaitStatus(owner string) SharedWaitStatus {
	s.sharedMu.Lock()
	defer s.sharedMu.Unlock()
	var result SharedWaitStatus
	for key, current := range s.shared {
		if key.owner == owner {
			result.Observers++
			result.Subscribers += current.subscribers
		}
	}
	return result
}
func (s *Service) BeginRequest() bool {
	if s.config == nil || !s.config.BeginRequest() {
		return false
	}
	s.lease.Begin()
	return true
}
func (s *Service) EndRequest() {
	s.lease.End()
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
func (s *Service) ReloadConfig(c Config) error {
	if s.config == nil {
		return errors.New("broker config unavailable")
	}
	if err := s.config.Reload(c); err != nil {
		return err
	}
	s.Scheduler.Configure(c.QoS, c.MaxHosts, c.OwnerWeights)
	s.lease.SetGrace(c.IdleTTL)
	return nil
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
func (s *Service) ReapIdle(now time.Time) bool {
	return s.lease.Reap(now, s.client.DetachConnections)
}

func (s *Service) ReapBulkIdle(now time.Time) int {
	ttl := s.config.Get().BulkIdleTTL
	if ttl == 0 {
		ttl = 30 * time.Second
	}
	return s.client.ReapBulkIdle(now, ttl)
}

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
	return s.policy.Grant(owner.Key(), operation)
}
func (s *Service) GrantCapability(owner Owner, capability, operation string) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if capability == "" || operation == "" {
		return errors.New("capability and operation required")
	}
	return s.policy.GrantCapability(owner.Key(), capability, operation)
}
func (s *Service) Revoke(owner Owner, operation string) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	return s.policy.Revoke(owner.Key(), operation)
}
func (s *Service) RevokeCapability(owner Owner, capability, operation string) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if capability == "" || operation == "" {
		return errors.New("capability and operation required")
	}
	return s.policy.Revoke(owner.Key(), capabilityKey(capability, operation))
}
func (s *Service) ConfigurePolicy(path string) error { return s.policy.ConfigurePersistence(path) }
func (s *Service) DecideRequest(owner Owner, operation, host string) Decision {
	if err := owner.Validate(); err != nil {
		return Decision{Reason: err.Error()}
	}
	return s.policy.DecideRequest(owner.Key(), operation, host)
}
func (s *Service) GrantHost(owner Owner, host, capability, operation string, revoke bool) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	expected := CapabilityForOperation(operation)
	if capability == "" {
		capability = expected
	}
	if capability != expected {
		return errors.New("capability does not match operation")
	}
	if revoke {
		return s.policy.RevokeHost(owner.Key(), host, capability, operation)
	}
	return s.policy.GrantHost(owner.Key(), host, capability, operation)
}
func (s *Service) LoadPolicy(path string) error { return s.policy.Load(path) }
func (s *Service) SavePolicy(path string) error { return s.policy.Save(path) }
func (s *Service) PolicyDecisionForCapability(owner Owner, capability, operation string) Decision {
	if err := owner.Validate(); err != nil {
		return Decision{Reason: err.Error()}
	}
	return s.policy.DecideCapability(owner.Key(), capability, operation)
}

// RecoverJobs probes persisted ownership without interpreting transport errors
// or missing remote files as permission to destroy ownership. Even unavailable
// records remain manageable after a later reconnect; only explicit owned job_rm
// outcomes remove them. The context bounds startup probing, not the job itself.
func (s *Service) RecoverJobs(ctx context.Context) {
	for _, ref := range s.Jobs.Snapshot() {
		if ctx.Err() != nil {
			break
		}
		clientID, projectID, _ := strings.Cut(ref.Owner, "\x00")
		req := &proto.Request{Op: proto.OpJobStatus, ClientID: clientID, ProjectID: projectID, Job: &proto.JobParams{ID: ref.ID}}
		resp, err := s.Dispatch(ctx, ref.Host, req)
		result := "recovery_unreachable"
		if err == nil && resp != nil && resp.OK && resp.Job != nil && resp.Job.Info != nil && resp.Job.Info.ID == ref.ID {
			result = "recovery_found"
		}
		s.Audit.Append(AuditEvent{Owner: ref.Owner, Operation: proto.OpJobStatus, Result: result})
	}
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
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.client == nil {
		return ErrClosed
	}
	if s.closed.Swap(true) {
		return ErrClosed
	}
	s.stopObservations()
	// Reserve time for cancellation, transport teardown and durable outcome
	// publication. Giving Drain the entire shutdown deadline leaves no budget
	// for those steps when a remote terminal response is stuck.
	grace := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		grace = min(grace, max(time.Duration(0), time.Until(deadline)/2))
	}
	graceCtx, cancel := context.WithTimeout(ctx, grace)
	_ = s.Drain(graceCtx)
	cancel()
	schedulerErr := s.Scheduler.Close(ctx)
	transportDone := make(chan struct{})
	go func() {
		s.client.Close()
		close(transportDone)
	}()
	select {
	case <-transportDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Scheduler completion precedes the caller's durable mutation transition.
	// Wait for EndRequest too, so shutdown does not race that final commit.
	return errors.Join(schedulerErr, s.Drain(ctx))
}
