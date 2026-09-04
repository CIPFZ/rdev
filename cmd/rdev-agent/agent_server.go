package main

import (
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const (
	maxWaitWorkers = 8
	maxWaitQueue   = 32
	maxNormalQueue = 64
)

type queuedRequest struct {
	request proto.Request
}

// agentServer owns all per-connection mutable state. Admission happens before
// goroutine creation: fixed worker sets consume bounded queues, so a hostile
// caller cannot turn queueing into an unbounded goroutine allocation.
type agentServer struct {
	ctx          context.Context
	cancel       context.CancelFunc
	state        string
	writer       *respWriter
	cache        *operationCache
	early        *earlyCancelStore
	waits        *waitHub
	normalQ      chan queuedRequest
	waitQ        chan queuedRequest
	workers      sync.WaitGroup
	modeMu       sync.RWMutex
	version      int
	features     map[proto.Feature]bool
	withDeadline func(context.Context, time.Time) (context.Context, context.CancelFunc)
}

func newAgentServer(parent context.Context, state string, writer *respWriter) *agentServer {
	ctx, cancel := context.WithCancel(parent)
	s := &agentServer{
		ctx: ctx, cancel: cancel, state: state, writer: writer,
		cache:   newOperationCache(realRuntimeClock{}, dedupeCapacity, dedupeTTL),
		early:   newEarlyCancelStore(realRuntimeClock{}, earlyCancelCap, dedupeTTL),
		waits:   newWaitHub(realWaitClock{}),
		normalQ: make(chan queuedRequest, maxNormalQueue),
		waitQ:   make(chan queuedRequest, maxWaitQueue),
		version: proto.Version, features: make(map[proto.Feature]bool),
		withDeadline: context.WithDeadline,
	}
	for _, feature := range proto.SupportedFeatures() {
		s.features[feature] = true
	}
	for i := 0; i < maxConcurrentRequests; i++ {
		s.workers.Add(1)
		go s.worker(s.normalQ)
	}
	for i := 0; i < maxWaitWorkers; i++ {
		s.workers.Add(1)
		go s.worker(s.waitQ)
	}
	return s
}

func (s *agentServer) submit(request proto.Request) {
	descriptor, err := proto.RequireOperation(request.Op)
	if err != nil {
		s.writeError(&request, proto.CodeUnknownOperation, proto.StateNotSent, 1)
		return
	}
	if envelope := s.validateRequestControls(&request); envelope != nil {
		s.writeEnvelope(&request, envelope, 1)
		return
	}
	if request.Op == proto.OpCancel {
		s.handleCancel(&request)
		return
	}
	queue := s.normalQ
	if descriptor.Execution == proto.ExecutionWatcher {
		queue = s.waitQ
	}
	select {
	case queue <- queuedRequest{request: request}:
	case <-s.ctx.Done():
		s.writeError(&request, proto.CodeCanceled, proto.StateCanceled, 1)
	default:
		s.writeError(&request, proto.CodeQueueFull, proto.StateNotSent, 1)
	}
}

func (s *agentServer) worker(queue <-chan queuedRequest) {
	defer s.workers.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case queued := <-queue:
			if s.ctx.Err() != nil {
				return
			}
			s.process(&queued.request)
		}
	}
}

func (s *agentServer) process(request *proto.Request) {
	var activeRecord *operationRecord
	var emitter *execStreamEmitter
	accepted := false
	lastSeq := uint64(0)
	defer func() {
		if recover() == nil {
			return
		}
		// A malformed request must not take down the whole multiplexed agent or
		// leave an accepted dedupe record waiting forever. The public error is
		// registry-backed and deliberately contains no panic value or stack.
		state, nextSeq := proto.StateNotSent, uint64(1)
		if accepted {
			state = proto.StatePossiblyExecuted
			nextSeq = lastSeq + 1
			if emitter != nil {
				nextSeq = emitter.finalSeq()
			}
		}
		envelope := proto.NewError(proto.CodeInternalFailure, request.OperationID, state)
		response := errorResponse(request, envelope, nextSeq)
		if activeRecord == nil || s.cache.finish(activeRecord, response) {
			s.writer.write(response)
		}
	}()
	if s.ctx.Err() != nil {
		s.writeError(request, proto.CodeCanceled, proto.StateCanceled, 1)
		return
	}
	if envelope := s.validateRequestControls(request); envelope != nil {
		s.writeEnvelope(request, envelope, 1)
		return
	}
	// The initial hello is unary-shaped for N-1 compatibility. Every later
	// fallback is selected from the negotiated mode, never inferred from a
	// response/request shape that a malformed v3 peer could mimic.
	if request.Op == proto.OpPing && request.Hello != nil {
		response := handleContext(s.ctx, request, s.state, s.waits)
		s.rememberNegotiation(response)
		s.writer.write(response)
		return
	}
	if s.negotiatedVersion() < 3 {
		response := handleContext(s.ctx, request, s.state, s.waits)
		s.writer.write(response)
		return
	}
	if request.OperationID == "" || request.ClientID == "" {
		s.writeError(request, proto.CodeInvalidRequest, proto.StateNotSent, 1)
		return
	}
	if !s.negotiatedFeature(proto.FeatureStreaming) {
		request.StreamWindowBytes = 0
	}
	descriptor, _ := proto.LookupOperation(request.Op)
	base := s.ctx
	if descriptor.Disconnect == proto.DisconnectContinue {
		base = context.Background()
	}
	opCtx, cancel := context.WithCancel(base)
	if request.DeadlineUnixMilli != 0 {
		deadline := time.UnixMilli(request.DeadlineUnixMilli)
		var deadlineCancel context.CancelFunc
		withDeadline := s.withDeadline
		if withDeadline == nil {
			withDeadline = context.WithDeadline
		}
		opCtx, deadlineCancel = withDeadline(opCtx, deadline)
		previous := cancel
		cancel = func() { deadlineCancel(); previous() }
	}
	defer cancel()

	begin := s.cache.begin(request, cancel)
	if begin.envelope != nil {
		s.writeEnvelope(request, begin.envelope, 1)
		return
	}
	activeRecord = begin.record
	if begin.cached != nil {
		s.writer.write(acceptedResponse(request, 1))
		begin.cached.ID = request.ID
		begin.cached.OperationID = request.OperationID
		begin.cached.Seq = 2
		begin.cached.Terminal = true
		s.writer.write(begin.cached)
		return
	}
	if begin.join {
		s.writer.write(acceptedResponse(request, 1))
		select {
		case <-begin.record.done:
			cached := cloneResponse(begin.record.final)
			if cached == nil {
				s.writeError(request, proto.CodeInternalFailure, proto.StatePossiblyExecuted, 2)
				return
			}
			cached.ID, cached.OperationID, cached.Seq = request.ID, request.OperationID, 2
			s.writer.write(cached)
		case <-opCtx.Done():
			s.writeContextError(request, opCtx.Err(), 2)
		}
		return
	}

	if s.early.take(request.ClientID, request.OperationID, request.Op) {
		envelope := proto.NewError(proto.CodeCanceled, request.OperationID, proto.StateCanceled)
		response := errorResponse(request, envelope, 1)
		s.cache.finish(begin.record, response)
		s.writer.write(response)
		return
	}

	if !s.writer.write(acceptedResponse(request, 1)) {
		return
	}
	accepted = true
	lastSeq = 1
	if s.negotiatedFeature(proto.FeatureStreaming) {
		if s.writer.write(&proto.Response{
			ID: request.ID, OperationID: request.OperationID, Type: proto.EventProgress,
			Seq: 2, OK: true, Execution: proto.StateAccepted,
			Progress: &proto.ProgressFrame{Phase: "running"},
		}) {
			lastSeq = 2
		}
	}
	emitter = newExecStreamEmitter(s.writer, request, lastSeq)
	response := handleContextStream(opCtx, request, s.state, s.waits, emitter.emit)
	response.OperationID = request.OperationID
	response.Type = proto.EventFinal
	response.Terminal = true
	if response.Error != nil {
		normalizeAcceptedTerminalError(response.Error)
		response.Execution = response.Error.ExecutionState
	} else {
		response.Execution = proto.StateCompleted
	}
	response.Seq = emitter.finalSeq()
	if opCtx.Err() != nil {
		code := proto.CodeCanceled
		if errors.Is(opCtx.Err(), context.DeadlineExceeded) {
			code = proto.CodeDeadlineExceeded
		}
		response = errorResponse(request, proto.NewError(code, request.OperationID, proto.StateCanceled), emitter.finalSeq())
	}
	if response.Error != nil {
		response.Type = proto.EventError
	}
	stampResultMetadata(response)
	if s.cache.finish(begin.record, response) {
		s.writer.write(response)
	}
}

func (s *agentServer) validateRequestControls(request *proto.Request) *proto.ErrorEnvelope {
	if request == nil {
		return proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	if request.StreamWindowBytes < 0 || request.StreamWindowBytes > proto.AbsoluteStreamWindowBytes {
		return proto.NewError(proto.CodeLimitExceeded, request.OperationID, proto.StateNotSent)
	}
	descriptor, ok := proto.LookupOperation(request.Op)
	if !ok {
		return proto.NewError(proto.CodeUnknownOperation, request.OperationID, proto.StateNotSent)
	}
	version := s.negotiatedVersion()
	if version < 3 {
		if request.Op == proto.OpCancel || request.DeadlineUnixMilli != 0 {
			return proto.NewError(proto.CodeUnsupportedFeature, request.OperationID, proto.StateNotSent)
		}
		return nil
	}
	deadlineCapable := false
	for _, feature := range descriptor.RequiredFeatures {
		deadlineCapable = deadlineCapable || feature == proto.FeatureDeadline
		if !s.negotiatedFeature(feature) {
			return proto.NewError(proto.CodeUnsupportedFeature, request.OperationID, proto.StateNotSent)
		}
	}
	if request.DeadlineUnixMilli != 0 && !deadlineCapable {
		return proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
	}
	return nil
}

// normalizeAcceptedTerminalError keeps the terminal state coherent with the
// already-emitted accepted frame. Validation/admission errors discovered by a
// handler did not execute the requested work, but after protocol acceptance
// their terminal phase is "failed", not the pre-admission "not_sent" state.
func normalizeAcceptedTerminalError(envelope *proto.ErrorEnvelope) {
	if envelope == nil {
		return
	}
	switch envelope.Code {
	case proto.CodeCanceled, proto.CodeDeadlineExceeded:
		envelope.ExecutionState = proto.StateCanceled
	case proto.CodeAmbiguousOutcome:
		envelope.ExecutionState = proto.StatePossiblyExecuted
	default:
		envelope.ExecutionState = proto.StateFailed
	}
}

func (s *agentServer) negotiatedVersion() int {
	s.modeMu.RLock()
	defer s.modeMu.RUnlock()
	return s.version
}

func (s *agentServer) negotiatedFeature(feature proto.Feature) bool {
	s.modeMu.RLock()
	defer s.modeMu.RUnlock()
	return s.version >= 3 && s.features[feature]
}

func (s *agentServer) rememberNegotiation(response *proto.Response) {
	if response == nil || !response.OK || response.Ping == nil {
		return
	}
	s.modeMu.Lock()
	defer s.modeMu.Unlock()
	s.version = response.Ping.NegotiatedVersion
	s.features = make(map[proto.Feature]bool)
	if s.version >= 3 {
		for _, feature := range response.Ping.Features {
			s.features[feature] = true
		}
	}
}

func stampResultMetadata(response *proto.Response) {
	if response == nil {
		return
	}
	stamp := func(operationID *string, terminal *bool, execution *proto.ExecutionState) {
		if response.OperationID != "" {
			*operationID = response.OperationID
		}
		if response.Terminal {
			*terminal = true
		}
		if response.Execution != "" {
			*execution = response.Execution
		}
	}
	if response.Exec != nil {
		stamp(&response.Exec.OperationID, &response.Exec.Terminal, &response.Exec.Execution)
	}
	if response.Read != nil {
		stamp(&response.Read.OperationID, &response.Read.Terminal, &response.Read.Execution)
	}
	if response.Cat != nil {
		stamp(&response.Cat.OperationID, &response.Cat.Terminal, &response.Cat.Execution)
	}
	if response.Job != nil {
		stamp(&response.Job.OperationID, &response.Job.Terminal, &response.Job.Execution)
		stampJobInfo := func(info *proto.JobInfo) {
			if info != nil {
				stamp(&info.OperationID, &info.Terminal, &info.Execution)
			}
		}
		stampJobInfo(response.Job.Info)
		for _, info := range response.Job.List {
			stampJobInfo(info)
		}
		for _, waited := range response.Job.Waited {
			if waited != nil {
				stampJobInfo(waited.Info)
			}
		}
	}
	if response.Storage != nil {
		stamp(&response.Storage.OperationID, &response.Storage.Terminal, &response.Storage.Execution)
	}
	if response.List != nil {
		stamp(&response.List.OperationID, &response.List.Terminal, &response.List.Execution)
	}
}

type execStreamEmitter struct {
	mu        sync.Mutex
	writer    *respWriter
	request   *proto.Request
	seq       uint64
	remaining int64
	seen      map[string]int64
	emitted   map[string]int64
}

func newExecStreamEmitter(writer *respWriter, request *proto.Request, lastSeq uint64) *execStreamEmitter {
	window := request.StreamWindowBytes
	if window < 0 || window > proto.AbsoluteStreamWindowBytes {
		window = 0
	}
	return &execStreamEmitter{
		writer: writer, request: request, seq: lastSeq, remaining: window,
		seen: make(map[string]int64), emitted: make(map[string]int64),
	}
}

// emit is called directly from os/exec's stdout/stderr copy goroutines. It does
// not retain chunks: the fixed credit is consumed whether a best-effort data
// write succeeds or loses to a waiting control frame, so a slow consumer cannot
// turn process output into a queue. The final ExecResult still carries exact
// retained/original/dropped totals for the bounded aggregate result.
func (e *execStreamEmitter) emit(stream string, data []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen[stream] += int64(len(data))
	const chunkBytes = 16 << 10
	for len(data) > 0 && e.remaining > 0 {
		count := min(int64(chunkBytes), e.remaining, int64(len(data)))
		chunk := data[:int(count)]
		data = data[int(count):]
		e.remaining -= count
		nextSeq := e.seq + 1
		retained := e.emitted[stream]
		truncation, _ := proto.NewTruncation(e.seen[stream], retained+count)
		written := e.writer.write(&proto.Response{
			ID: e.request.ID, OperationID: e.request.OperationID, Type: proto.EventData,
			Seq: nextSeq, OK: true, Execution: proto.StateAccepted,
			Data: &proto.DataFrame{
				Stream: stream, Content: base64.StdEncoding.EncodeToString(chunk),
				ContentB64: true, Truncation: truncation,
			},
		})
		if written {
			e.seq = nextSeq
			e.emitted[stream] += count
		}
	}
}

func (e *execStreamEmitter) finalSeq() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seq + 1
}

func acceptedResponse(request *proto.Request, seq uint64) *proto.Response {
	return &proto.Response{
		ID: request.ID, OperationID: request.OperationID, Type: proto.EventAccepted,
		Seq: seq, OK: true, Execution: proto.StateAccepted,
	}
}

func errorResponse(request *proto.Request, envelope *proto.ErrorEnvelope, seq uint64) *proto.Response {
	return &proto.Response{
		ID: request.ID, OperationID: request.OperationID, Type: proto.EventError,
		Seq: seq, Terminal: true, Execution: envelope.ExecutionState,
		OK: false, Err: envelope.Message, Error: envelope,
	}
}

func (s *agentServer) writeEnvelope(request *proto.Request, envelope *proto.ErrorEnvelope, seq uint64) {
	s.writer.write(errorResponse(request, envelope, seq))
}

func (s *agentServer) writeError(request *proto.Request, code proto.ErrorCode, state proto.ExecutionState, seq uint64) {
	s.writeEnvelope(request, proto.NewError(code, request.OperationID, state), seq)
}

func (s *agentServer) writeContextError(request *proto.Request, err error, seq uint64) {
	code := proto.CodeCanceled
	if errors.Is(err, context.DeadlineExceeded) {
		code = proto.CodeDeadlineExceeded
	}
	s.writeError(request, code, proto.StateCanceled, seq)
}

func (s *agentServer) handleCancel(request *proto.Request) {
	if request.Cancel == nil || request.Cancel.OperationID == "" || request.ClientID == "" {
		s.writeError(request, proto.CodeInvalidRequest, proto.StateNotSent, 1)
		return
	}
	if proto.ValidateOperationID(request.ClientID) != nil ||
		proto.ValidateOperationID(request.OperationID) != nil ||
		proto.ValidateOperationID(request.Cancel.OperationID) != nil {
		s.writeError(request, proto.CodeInvalidRequest, proto.StateNotSent, 1)
		return
	}
	if request.Cancel.TargetOp != "" {
		descriptor, ok := proto.LookupOperation(request.Cancel.TargetOp)
		if !ok || descriptor.Disconnect != proto.DisconnectCancel {
			s.writeError(request, proto.CodeProcessInvalidState, proto.StateNotSent, 1)
			return
		}
	}
	found, _, eligible := s.cache.cancel(request.ClientID, request.Cancel.OperationID, request.Cancel.TargetOp)
	if found && !eligible {
		s.writeError(request, proto.CodeProcessInvalidState, proto.StateNotSent, 1)
		return
	}
	if !found {
		if request.Cancel.TargetOp == "" {
			s.writeError(request, proto.CodeInvalidRequest, proto.StateNotSent, 1)
			return
		}
		if err := s.early.add(request.ClientID, request.Cancel.OperationID, request.Cancel.TargetOp); err != nil {
			s.writeError(request, proto.CodeQueueFull, proto.StateNotSent, 1)
			return
		}
	}
	s.writer.write(acceptedResponse(request, 1))
	s.writer.write(&proto.Response{
		ID: request.ID, OperationID: request.OperationID, Type: proto.EventFinal,
		Seq: 2, Terminal: true, Execution: proto.StateCompleted, OK: true,
	})
}

func (s *agentServer) close() {
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownDrainTimeout):
	}
}

func handleContext(ctx context.Context, request *proto.Request, state string, waits *waitHub) *proto.Response {
	return handleContextStream(ctx, request, state, waits, nil)
}

func handleContextStream(ctx context.Context, request *proto.Request, state string, waits *waitHub, emit func(string, []byte)) *proto.Response {
	response := &proto.Response{ID: request.ID, OperationID: request.OperationID}
	var err error
	switch request.Op {
	case proto.OpPing:
		response.Ping = doPing()
		if request.Hello != nil {
			if version, ok := proto.NegotiateVersion(
				proto.ProtocolRange{Min: proto.MinVersion, Max: proto.Version},
				proto.ProtocolRange{Min: request.Hello.MinVersion, Max: request.Hello.MaxVersion},
			); ok {
				response.Ping.NegotiatedVersion = version
				if version >= 3 {
					response.Ping.Features = proto.NegotiateFeatures(proto.SupportedFeatures(), request.Hello.Features)
				}
			} else {
				err = proto.NewError(proto.CodeUnsupportedFeature, request.OperationID, proto.StateNotSent)
			}
		}
	case proto.OpExec:
		if request.Exec == nil {
			err = proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		} else {
			var stdoutHook, stderrHook func([]byte)
			if emit != nil {
				stdoutHook = func(data []byte) { emit("stdout", data) }
				stderrHook = func(data []byte) { emit("stderr", data) }
			}
			response.Exec, err = doExecContextStream(ctx, request.Exec, stdoutHook, stderrHook)
		}
	case proto.OpReadFile:
		if request.Read == nil {
			err = proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		} else {
			response.Read, err = doRead(request.Read)
		}
	case proto.OpWriteFile:
		if request.Cat == nil {
			err = proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		} else {
			response.Cat, err = doWrite(request.Cat)
		}
	case proto.OpList:
		if request.List == nil {
			err = proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		} else {
			response.List, err = doList(request.List)
		}
	case proto.OpStorageStatus, proto.OpStorageGC, proto.OpStorageDoctor:
		if request.Storage == nil {
			err = proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		} else {
			response.Storage, err = doStorage(request.Op, request.Storage, state)
		}
	default:
		descriptor, ok := proto.LookupOperation(request.Op)
		if !ok || descriptor.Execution == proto.ExecutionControl {
			err = proto.NewError(proto.CodeUnknownOperation, request.OperationID, proto.StateNotSent)
		} else if request.Job == nil {
			err = proto.NewError(proto.CodeInvalidRequest, request.OperationID, proto.StateNotSent)
		} else if request.Op == proto.OpJobWait {
			response.Job, err = jobWaitContext(ctx, waits, request.Job, state)
		} else {
			response.Job, err = doJob(request.Op, request.Job, state)
		}
	}
	if err != nil {
		envelope := classifyAgentError(err, request.OperationID)
		response.OK = false
		response.Err = envelope.Message
		response.Error = envelope
		response.Execution = envelope.ExecutionState
		return response
	}
	response.OK = true
	return response
}
