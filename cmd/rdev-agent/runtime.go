package main

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const (
	dedupeCapacity     = 1024
	dedupeTTL          = 5 * time.Minute
	dedupeByteCapacity = 64 << 20
	earlyCancelCap     = 1024
)

type runtimeClock interface {
	Now() time.Time
}

type realRuntimeClock struct{}

func (realRuntimeClock) Now() time.Time { return time.Now() }

type operationRecord struct {
	clientID    string
	operationID string
	op          string
	class       proto.OperationClass
	digest      string
	created     time.Time
	lastUsed    time.Time
	done        chan struct{}
	cancel      context.CancelFunc
	final       *proto.Response
	finalBytes  int64
	finished    bool
	canceled    bool
}

// operationCache is intentionally process-local. A retry marked Replay that
// misses this cache is safe for read-only/idempotent operations, but a mutation
// is returned as ambiguous because an agent restart or eviction may have erased
// proof that it already ran.
type operationCache struct {
	mu       sync.Mutex
	clock    runtimeClock
	capacity int
	bytes    int64
	byteCap  int64
	ttl      time.Duration
	records  map[string]*operationRecord
}

func newOperationCache(clock runtimeClock, capacity int, ttl time.Duration) *operationCache {
	if clock == nil {
		clock = realRuntimeClock{}
	}
	if capacity <= 0 || capacity > int(proto.AbsoluteDedupeEntries) {
		capacity = dedupeCapacity
	}
	if ttl <= 0 || ttl > time.Duration(proto.AbsoluteDedupeTTLSeconds)*time.Second {
		ttl = dedupeTTL
	}
	return &operationCache{
		clock: clock, capacity: capacity, ttl: ttl, byteCap: dedupeByteCapacity,
		records: make(map[string]*operationRecord),
	}
}

func operationCacheKey(clientID, operationID string) string {
	return clientID + "\x00" + operationID
}

type beginResult struct {
	record   *operationRecord
	cached   *proto.Response
	join     bool
	envelope *proto.ErrorEnvelope
}

func (c *operationCache) begin(req *proto.Request, cancel context.CancelFunc) beginResult {
	descriptor, descriptorErr := proto.RequireOperation(req.Op)
	if descriptorErr != nil {
		return beginResult{envelope: proto.NewError(proto.CodeUnknownOperation, req.OperationID, proto.StateNotSent)}
	}
	if req.OperationID == "" || req.ClientID == "" {
		return beginResult{envelope: proto.NewError(proto.CodeInvalidRequest, req.OperationID, proto.StateNotSent)}
	}
	if proto.ValidateOperationID(req.OperationID) != nil || proto.ValidateOperationID(req.ClientID) != nil {
		return beginResult{envelope: proto.NewError(proto.CodeInvalidRequest, req.OperationID, proto.StateNotSent)}
	}
	digest, err := proto.CanonicalRequestDigest(req)
	if err != nil {
		return beginResult{envelope: proto.NewError(proto.CodeInvalidRequest, req.OperationID, proto.StateNotSent)}
	}

	now := c.clock.Now()
	key := operationCacheKey(req.ClientID, req.OperationID)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(now)
	if record := c.records[key]; record != nil {
		record.lastUsed = now
		if record.op != req.Op || record.digest != digest {
			return beginResult{envelope: proto.NewError(proto.CodeOperationIDConflict, req.OperationID, proto.StateNotSent)}
		}
		if record.finished {
			return beginResult{record: record, cached: cloneResponse(record.final)}
		}
		return beginResult{record: record, join: true}
	}
	if req.Replay && descriptor.Class == proto.ClassMutating {
		return beginResult{envelope: proto.NewError(proto.CodeAmbiguousOutcome, req.OperationID, proto.StatePossiblyExecuted)}
	}
	if len(c.records) >= c.capacity {
		c.evictOneLocked()
	}
	if len(c.records) >= c.capacity {
		return beginResult{envelope: proto.NewError(proto.CodeQueueFull, req.OperationID, proto.StateNotSent)}
	}
	record := &operationRecord{
		clientID: req.ClientID, operationID: req.OperationID, op: req.Op,
		class: descriptor.Class, digest: digest, created: now, lastUsed: now,
		done: make(chan struct{}), cancel: cancel,
	}
	c.records[key] = record
	return beginResult{record: record}
}

func (c *operationCache) finish(record *operationRecord, response *proto.Response) bool {
	if record == nil {
		return false
	}
	originalResponse := response
	c.mu.Lock()
	defer c.mu.Unlock()
	if record.finished {
		return false
	}
	if record.canceled && (response == nil || response.OK) {
		requestID := ""
		seq := uint64(1)
		if response != nil {
			requestID, seq = response.ID, max(response.Seq, 1)
		}
		code, state := proto.CodeCanceled, proto.StateCanceled
		// Once a mutating handler has been accepted, a cooperative cancellation
		// request cannot prove that no side effect occurred. A handler that races
		// cancellation and reports success is therefore ambiguous, never safely
		// canceled.
		if record.class == proto.ClassMutating {
			code, state = proto.CodeAmbiguousOutcome, proto.StatePossiblyExecuted
		}
		envelope := proto.NewError(code, record.operationID, state)
		response = &proto.Response{
			ID: requestID, OperationID: record.operationID, Type: proto.EventError,
			Seq: seq, Terminal: true, Execution: state,
			OK: false, Err: envelope.Message, Error: envelope,
		}
	}
	clone := cloneResponse(response)
	encoded, marshalErr := json.Marshal(clone)
	finalBytes := int64(len(encoded))
	for marshalErr == nil && c.bytes+finalBytes > c.byteCap && c.evictOneLocked(record) {
	}
	if clone == nil || marshalErr != nil || finalBytes <= 0 || finalBytes > c.byteCap || c.bytes+finalBytes > c.byteCap {
		code, state := proto.CodeLimitExceeded, proto.StateFailed
		if record.class == proto.ClassMutating {
			code, state = proto.CodeAmbiguousOutcome, proto.StatePossiblyExecuted
		}
		envelope := proto.NewError(code, record.operationID, state)
		requestID := ""
		seq := uint64(1)
		if response != nil {
			requestID, seq = response.ID, max(response.Seq, 1)
		}
		clone = &proto.Response{
			ID: requestID, OperationID: record.operationID, Type: proto.EventError,
			Seq: seq, Terminal: true, Execution: state,
			OK: false, Err: envelope.Message, Error: envelope,
		}
		encoded, _ = json.Marshal(clone)
		finalBytes = int64(len(encoded))
	}
	record.finished = true
	record.final = clone
	if originalResponse != nil && clone != nil {
		*originalResponse = *clone
	}
	record.finalBytes = finalBytes
	c.bytes += finalBytes
	record.lastUsed = c.clock.Now()
	record.cancel = nil
	close(record.done)
	return true
}

func (c *operationCache) cancel(clientID, operationID, targetOp string) (found, terminal, eligible bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record := c.records[operationCacheKey(clientID, operationID)]
	if record == nil {
		return false, false, false
	}
	descriptor, known := proto.LookupOperation(record.op)
	if !known || descriptor.Disconnect != proto.DisconnectCancel || (targetOp != "" && targetOp != record.op) {
		return true, record.finished, false
	}
	if record.finished {
		return true, true, true
	}
	record.canceled = true
	if record.cancel != nil {
		record.cancel()
	}
	return true, false, true
}

func (c *operationCache) expireLocked(now time.Time) {
	for key, record := range c.records {
		if record.finished && now.Sub(record.lastUsed) >= c.ttl {
			c.bytes -= record.finalBytes
			delete(c.records, key)
		}
	}
}

func (c *operationCache) evictOneLocked(exclude ...*operationRecord) bool {
	var oldestKey string
	var oldest time.Time
	for key, record := range c.records {
		if !record.finished {
			continue
		}
		if len(exclude) > 0 && record == exclude[0] {
			continue
		}
		if oldestKey == "" || record.lastUsed.Before(oldest) {
			oldestKey, oldest = key, record.lastUsed
		}
	}
	if oldestKey != "" {
		c.bytes -= c.records[oldestKey].finalBytes
		delete(c.records, oldestKey)
		return true
	}
	return false
}

func cloneResponse(response *proto.Response) *proto.Response {
	if response == nil {
		return nil
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return &proto.Response{
			OperationID: response.OperationID, Type: proto.EventError, Terminal: true,
			Execution: proto.StateFailed, Error: proto.NewError(proto.CodeInternalFailure, response.OperationID, proto.StateFailed),
		}
	}
	var clone proto.Response
	if json.Unmarshal(encoded, &clone) != nil {
		return nil
	}
	return &clone
}

type earlyCancelStore struct {
	mu       sync.Mutex
	clock    runtimeClock
	capacity int
	ttl      time.Duration
	items    map[string]earlyCancelEntry
}

type earlyCancelEntry struct {
	created  time.Time
	targetOp string
}

func newEarlyCancelStore(clock runtimeClock, capacity int, ttl time.Duration) *earlyCancelStore {
	if clock == nil {
		clock = realRuntimeClock{}
	}
	return &earlyCancelStore{clock: clock, capacity: capacity, ttl: ttl, items: make(map[string]earlyCancelEntry)}
}

func (s *earlyCancelStore) add(clientID, operationID, targetOp string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now()
	s.expireLocked(now)
	key := operationCacheKey(clientID, operationID)
	if _, exists := s.items[key]; exists {
		return nil
	}
	if len(s.items) >= s.capacity {
		return errors.New("early cancel capacity reached")
	}
	s.items[key] = earlyCancelEntry{created: now, targetOp: targetOp}
	return nil
}

func (s *earlyCancelStore) take(clientID, operationID, targetOp string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked(s.clock.Now())
	key := operationCacheKey(clientID, operationID)
	entry, ok := s.items[key]
	delete(s.items, key)
	return ok && entry.targetOp == targetOp
}

func (s *earlyCancelStore) expireLocked(now time.Time) {
	for key, entry := range s.items {
		if now.Sub(entry.created) >= s.ttl {
			delete(s.items, key)
		}
	}
}
