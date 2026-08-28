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
	ctx     context.Context
	cancel  context.CancelFunc
	state   string
	writer  *respWriter
	cache   *operationCache
	early   *earlyCancelStore
	waits   *waitHub
	normalQ chan queuedRequest
	waitQ   chan queuedRequest
	workers sync.WaitGroup
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
	if request.Op == proto.OpCancel {
		s.handleCancel(&request)
		return
	}
	descriptor, err := proto.RequireOperation(request.Op)
	if err != nil {
		s.writeError(&request, proto.CodeUnknownOperation, proto.StateNotSent, 1)
		return
	}
	queue := s.normalQ
	if descriptor.Execution == proto.ExecutionWatcher {
		queue = s.waitQ
	}
	select {
	case queue <- queuedRequest{request: request}:
	case <-s.ctx.Done():
		s.writeError(&request, proto.CodeCanceled, proto.StateNotSent, 1)
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
	defer func() {
		if recover() == nil {
			return
		}
		// A malformed request must not take down the whole multiplexed agent or
		// leave an accepted dedupe record waiting forever. The public error is
		// registry-backed and deliberately contains no panic value or stack.
		envelope := proto.NewError(proto.CodeInternalFailure, request.OperationID, proto.StatePossiblyExecuted)
		response := errorResponse(request, envelope, 1)
		if activeRecord == nil || s.cache.finish(activeRecord, response) {
			s.writer.write(response)
		}
	}()
	if s.ctx.Err() != nil {
		s.writeError(request, proto.CodeCanceled, proto.StateNotSent, 1)
		return
	}
	if request.StreamWindowBytes < 0 || request.StreamWindowBytes > proto.AbsoluteStreamWindowBytes {
		s.writeError(request, proto.CodeLimitExceeded, proto.StateNotSent, 1)
		return
	}
	// Protocol-2 fallback remains unary. It is accepted only for compatibility;
	// it deliberately receives none of the dedupe guarantees advertised by v3.
	if request.OperationID == "" || request.ClientID == "" {
		response := handleContext(s.ctx, request, s.state, s.waits)
		s.writer.write(response)
		return
	}

	base := s.ctx
	if descriptor, _ := proto.LookupOperation(request.Op); descriptor.Disconnect == proto.DisconnectContinue {
		base = context.Background()
	}
	opCtx, cancel := context.WithCancel(base)
	if request.DeadlineUnixMilli != 0 {
		deadline := time.UnixMilli(request.DeadlineUnixMilli)
		var deadlineCancel context.CancelFunc
		opCtx, deadlineCancel = context.WithDeadline(opCtx, deadline)
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

	if s.early.take(request.ClientID, request.OperationID) {
		envelope := proto.NewError(proto.CodeCanceled, request.OperationID, proto.StateCanceled)
		response := errorResponse(request, envelope, 1)
		s.cache.finish(begin.record, response)
		s.writer.write(response)
		return
	}

	s.writer.write(acceptedResponse(request, 1))
	s.writer.write(&proto.Response{
		ID: request.ID, OperationID: request.OperationID, Type: proto.EventProgress,
		Seq: 2, OK: true, Execution: proto.StateAccepted,
		Progress: &proto.ProgressFrame{Phase: "running"},
	})
	emitter := newExecStreamEmitter(s.writer, request, 2)
	response := handleContextStream(opCtx, request, s.state, s.waits, emitter.emit)
	response.OperationID = request.OperationID
	response.Type = proto.EventFinal
	response.Terminal = true
	if response.Error != nil {
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

func stampResultMetadata(response *proto.Response) {
	if response == nil {
		return
	}
	if response.Exec != nil {
		response.Exec.OperationID, response.Exec.Terminal, response.Exec.Execution = response.OperationID, response.Terminal, response.Execution
	}
	if response.Read != nil {
		response.Read.OperationID, response.Read.Terminal, response.Read.Execution = response.OperationID, response.Terminal, response.Execution
	}
	if response.Cat != nil {
		response.Cat.OperationID, response.Cat.Terminal, response.Cat.Execution = response.OperationID, response.Terminal, response.Execution
	}
	if response.Job != nil {
		response.Job.OperationID, response.Job.Terminal, response.Job.Execution = response.OperationID, response.Terminal, response.Execution
		stampJobInfo := func(info *proto.JobInfo) {
			if info != nil {
				info.OperationID, info.Terminal, info.Execution = response.OperationID, response.Terminal, response.Execution
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
	if response.List != nil {
		response.List.OperationID, response.List.Terminal, response.List.Execution = response.OperationID, response.Terminal, response.Execution
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
	found, _ := s.cache.cancel(request.ClientID, request.Cancel.OperationID)
	if !found {
		if err := s.early.add(request.ClientID, request.Cancel.OperationID); err != nil {
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
				response.Ping.Features = proto.NegotiateFeatures(proto.SupportedFeatures(), request.Hello.Features)
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
		var envelope *proto.ErrorEnvelope
		if !errors.As(err, &envelope) {
			envelope = proto.NewError(proto.CodeInternalFailure, request.OperationID, proto.StateFailed)
		} else if envelope.OperationID == "" {
			copy := *envelope
			copy.OperationID = request.OperationID
			envelope = &copy
		}
		response.OK = false
		response.Err = envelope.Message
		response.Error = envelope
		response.Execution = envelope.ExecutionState
		return response
	}
	response.OK = true
	return response
}
