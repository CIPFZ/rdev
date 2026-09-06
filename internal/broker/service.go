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
	Quota            *Quota
	Lanes            *Lanes
	Watches          *WatchHub
	Audit            *AuditLog
	config           *ConfigStore
	approvals        *ApprovalStore
	approvalMu       sync.Mutex
	approvalByToken  map[string]Approval
	readiness        Readiness
	sharedMu         sync.Mutex
	shared           map[string]*sharedDispatch
	Jobs             *JobRegistry
	fair             map[Lane]*fairDispatcher
	weightMu         sync.RWMutex
	weights          map[string]int
	dispatchMu       sync.RWMutex
	dispatchOverride func(context.Context, string, *proto.Request) (*proto.Response, error)
}

type sharedDispatch struct {
	done chan struct{}
	resp *proto.Response
	err  error
}

type fairItem struct {
	ctx    context.Context
	fn     func() (*proto.Response, error)
	result chan dispatchResult
}
type dispatchResult struct {
	resp *proto.Response
	err  error
}
type fairDispatcher struct {
	queue *FairQueue
	wake  chan struct{}
	stop  chan struct{}
}

func newFairDispatcher(workers int) *fairDispatcher {
	f := &fairDispatcher{queue: NewFairQueue(), wake: make(chan struct{}, 1), stop: make(chan struct{})}
	worker := func() {
		for {
			select {
			case <-f.wake:
			case <-f.stop:
				return
			}
			for {
				value, ok := f.queue.Next()
				if !ok {
					break
				}
				item := value.(fairItem)
				if err := item.ctx.Err(); err != nil {
					item.result <- dispatchResult{err: err}
					continue
				}
				resp, err := item.fn()
				item.result <- dispatchResult{resp: resp, err: err}
			}
		}
	}
	for i := 0; i < workers; i++ {
		go worker()
	}
	return f
}

func NewService(lookup client.AgentLookup) *Service {
	config, _ := NewConfigStore(Config{MaxHosts: 128, IdleTTL: 5 * time.Minute})
	s := &Service{client: client.New(lookup), policy: NewPolicy(), lease: NewLease(30 * time.Second), Quota: NewQuota(12, 4, 256), Lanes: NewLanes(2, 8, 1), Watches: NewWatchHub(), Audit: NewAuditLog(1024), config: config, approvals: NewApprovalStore(), approvalByToken: make(map[string]Approval), shared: make(map[string]*sharedDispatch), Jobs: NewJobRegistry(), fair: map[Lane]*fairDispatcher{LaneControl: newFairDispatcher(2), LaneExec: newFairDispatcher(8), LaneBulk: newFairDispatcher(1)}, weights: make(map[string]int)}
	s.SetReady(true)
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
	return s.client.DoProtocol(ctx, host, req)
}
func (s *Service) SetDispatcher(fn func(context.Context, string, *proto.Request) (*proto.Response, error)) {
	s.dispatchMu.Lock()
	s.dispatchOverride = fn
	s.dispatchMu.Unlock()
}
func (s *Service) DispatchFair(ctx context.Context, owner string, lane Lane, fn func() (*proto.Response, error)) (*proto.Response, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	f := s.fair[lane]
	if f == nil {
		return fn()
	}
	result := make(chan dispatchResult, 1)
	s.weightMu.RLock()
	weight := s.weights[owner]
	s.weightMu.RUnlock()
	f.queue.Enqueue(owner, fairItem{ctx: ctx, fn: fn, result: result}, weight)
	select {
	case f.wake <- struct{}{}:
	default:
	}
	select {
	case out := <-result:
		return out.resp, out.err
	case <-f.stop:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *Service) SetOwnerWeight(owner string, weight int) {
	if weight < 1 {
		weight = 1
	}
	s.weightMu.Lock()
	s.weights[owner] = weight
	s.weightMu.Unlock()
}
func (s *Service) SubscribeJob(key string) (<-chan any, func()) {
	return s.Watches.Subscribe(key)
}

// DispatchShared coalesces concurrent observations of the same detached job
// set. Only the first caller performs remote work; followers receive its result.
func (s *Service) DispatchShared(ctx context.Context, key string, fn func() (*proto.Response, error)) (*proto.Response, error) {
	s.sharedMu.Lock()
	if current := s.shared[key]; current != nil {
		s.sharedMu.Unlock()
		select {
		case <-current.done:
			return current.resp, current.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	current := &sharedDispatch{done: make(chan struct{})}
	s.shared[key] = current
	s.sharedMu.Unlock()
	current.resp, current.err = fn()
	if current.err == nil && current.resp != nil {
		s.Watches.Publish(key, current.resp)
	}
	s.sharedMu.Lock()
	delete(s.shared, key)
	close(current.done)
	s.sharedMu.Unlock()
	return current.resp, current.err
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
func (s *Service) ReloadConfig(c Config) error {
	if s.config == nil {
		return errors.New("broker config unavailable")
	}
	return s.config.Reload(c)
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
	if !s.Reapable(now) {
		return false
	}
	s.client.Close()
	return true
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
	s.policy.Grant(owner.Key(), operation)
	return nil
}
func (s *Service) GrantCapability(owner Owner, capability, operation string) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if capability == "" || operation == "" {
		return errors.New("capability and operation required")
	}
	s.policy.GrantCapability(owner.Key(), capability, operation)
	return nil
}
func (s *Service) LoadPolicy(path string) error { return s.policy.Load(path) }
func (s *Service) SavePolicy(path string) error { return s.policy.Save(path) }
func (s *Service) PolicyDecisionForCapability(owner Owner, capability, operation string) Decision {
	if err := owner.Validate(); err != nil {
		return Decision{Reason: err.Error()}
	}
	return s.policy.DecideCapability(owner.Key(), capability, operation)
}

// RecoverJobs revalidates persisted detached jobs against their remote hosts
// after broker restart. Missing jobs are removed; live records remain owned by
// their original principal.
func (s *Service) RecoverJobs(ctx context.Context) {
	for _, ref := range s.Jobs.Snapshot() {
		clientID, projectID, ok := strings.Cut(ref.Owner, "\x00")
		if !ok {
			s.Jobs.Remove(ref.ID)
			continue
		}
		req := &proto.Request{Op: proto.OpJobStatus, ClientID: clientID, ProjectID: projectID, Job: &proto.JobParams{ID: ref.ID}}
		resp, err := s.client.DoProtocol(ctx, ref.Host, req)
		if err != nil || resp == nil || resp.Job == nil || resp.Job.Info == nil {
			s.Jobs.Remove(ref.ID)
			s.Audit.Append(AuditEvent{At: time.Now(), Owner: ref.Owner, Operation: proto.OpJobStatus, Result: "recovery_missing"})
		}
	}
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
	for _, f := range s.fair {
		close(f.stop)
	}
	s.client.Close()
	return nil
}
