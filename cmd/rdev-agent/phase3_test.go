package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

type fakeRuntimeClock struct {
	mu  sync.Mutex
	now time.Time
}

type panicWaitClock struct{}

func (panicWaitClock) Now() time.Time                       { panic("synthetic wait clock panic") }
func (panicWaitClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

func (c *fakeRuntimeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeRuntimeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func mutationRequest(operationID, content string, replay bool) *proto.Request {
	return &proto.Request{
		OperationID: operationID, ClientID: "client_0123456789abcdef", Replay: replay,
		Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: "synthetic", Content: content},
	}
}

func TestOperationCacheExactlyOnceConflictRestartAndEviction(t *testing.T) {
	clock := &fakeRuntimeClock{now: time.Unix(100, 0)}
	cache := newOperationCache(clock, 1, time.Minute)
	request := mutationRequest("op_0000000000000001", "first", false)
	begin := cache.begin(request, func() {})
	if begin.record == nil || begin.envelope != nil {
		t.Fatalf("first begin = %+v", begin)
	}
	final := &proto.Response{OperationID: request.OperationID, OK: true, Type: proto.EventFinal, Terminal: true}
	if !cache.finish(begin.record, final) {
		t.Fatal("first terminal was not stored")
	}

	replay := mutationRequest(request.OperationID, "first", true)
	hit := cache.begin(replay, func() {})
	if hit.cached == nil || hit.envelope != nil {
		t.Fatalf("dedupe replay = %+v, want cached final", hit)
	}
	conflict := cache.begin(mutationRequest(request.OperationID, "different", true), func() {})
	if conflict.envelope == nil || conflict.envelope.Code != proto.CodeOperationIDConflict {
		t.Fatalf("digest conflict = %+v", conflict)
	}
	typeConflict := cache.begin(&proto.Request{
		OperationID: request.OperationID, ClientID: request.ClientID, Replay: true,
		Op: proto.OpReadFile, Read: &proto.ReadParams{Path: "synthetic"},
	}, func() {})
	if typeConflict.envelope == nil || typeConflict.envelope.Code != proto.CodeOperationIDConflict {
		t.Fatalf("operation type conflict = %+v", typeConflict)
	}

	second := mutationRequest("op_0000000000000002", "second", false)
	secondBegin := cache.begin(second, func() {})
	cache.finish(secondBegin.record, final)
	evicted := cache.begin(replay, func() {})
	if evicted.envelope == nil || evicted.envelope.Code != proto.CodeAmbiguousOutcome {
		t.Fatalf("evicted mutation replay = %+v, want ambiguous", evicted)
	}

	restarted := newOperationCache(clock, 2, time.Minute)
	miss := restarted.begin(replay, func() {})
	if miss.envelope == nil || miss.envelope.Code != proto.CodeAmbiguousOutcome {
		t.Fatalf("post-restart mutation replay = %+v, want ambiguous", miss)
	}
	readReplay := &proto.Request{
		OperationID: "op_0000000000000003", ClientID: request.ClientID,
		Op: proto.OpReadFile, Replay: true, Read: &proto.ReadParams{Path: "synthetic"},
	}
	if got := restarted.begin(readReplay, func() {}); got.record == nil || got.envelope != nil {
		t.Fatalf("read-only replay should be controlled re-execution: %+v", got)
	}
}

func TestOperationCacheTTLDoesNotEvictAccepted(t *testing.T) {
	clock := &fakeRuntimeClock{now: time.Unix(100, 0)}
	cache := newOperationCache(clock, 2, time.Second)
	request := mutationRequest("op_0000000000000010", "running", false)
	first := cache.begin(request, func() {})
	clock.Advance(10 * time.Second)
	joined := cache.begin(mutationRequest(request.OperationID, "running", true), func() {})
	if !joined.join || joined.record != first.record {
		t.Fatalf("accepted record was evicted: %+v", joined)
	}
}

func TestOperationCacheHasHardByteBudgetAndKeepsMutationTombstone(t *testing.T) {
	clock := &fakeRuntimeClock{now: time.Unix(100, 0)}
	cache := newOperationCache(clock, 2, time.Minute)
	cache.byteCap = 512
	request := mutationRequest("op_0000000000000011", "payload", false)
	begin := cache.begin(request, func() {})
	final := &proto.Response{
		ID: "1", OperationID: request.OperationID, OK: true,
		Type: proto.EventFinal, Terminal: true,
		Exec: &proto.ExecResult{Stdout: strings.Repeat("x", 4096)},
	}
	if !cache.finish(begin.record, final) {
		t.Fatal("large mutation terminal was not finalized")
	}
	if cache.bytes > cache.byteCap {
		t.Fatalf("dedupe bytes=%d exceed cap=%d", cache.bytes, cache.byteCap)
	}
	replay := cache.begin(mutationRequest(request.OperationID, "payload", true), func() {})
	if replay.cached == nil || replay.cached.Error == nil || replay.cached.Error.Code != proto.CodeAmbiguousOutcome {
		t.Fatalf("oversized cached mutation result = %+v, want ambiguous tombstone", replay)
	}
}

func testResponseWriter(buffer *bytes.Buffer) *respWriter {
	return newRespWriter(buffer, nil)
}

func decodeResponses(t *testing.T, buffer *bytes.Buffer) []proto.Response {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(buffer.Bytes()))
	var responses []proto.Response
	for {
		var response proto.Response
		if err := decoder.Decode(&response); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		responses = append(responses, response)
	}
	return responses
}

func TestCancelBeforeAcceptedProducesOneTerminal(t *testing.T) {
	var output bytes.Buffer
	server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
	t.Cleanup(server.close)
	target := &proto.Request{
		OperationID: "op_0000000000000020", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"true"}},
	}
	target.ID = "target"
	server.handleCancel(&proto.Request{
		ID: "cancel", OperationID: "op_0000000000000021", ClientID: target.ClientID,
		Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: target.OperationID, TargetOp: target.Op},
	})
	server.process(target)

	terminals := 0
	accepted := 0
	for _, response := range decodeResponses(t, &output) {
		if response.ID != target.ID {
			continue
		}
		if response.Type == proto.EventAccepted {
			accepted++
		}
		if response.Terminal {
			terminals++
			if response.Error == nil || response.Error.Code != proto.CodeCanceled {
				t.Fatalf("target terminal = %+v", response)
			}
		}
	}
	if accepted != 0 || terminals != 1 {
		t.Fatalf("accepted=%d terminals=%d, want 0/1", accepted, terminals)
	}
}

func TestCancelIdentityIsBoundedBeforeEarlyTombstone(t *testing.T) {
	var output bytes.Buffer
	server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
	t.Cleanup(server.close)
	server.handleCancel(&proto.Request{
		ID: "cancel", OperationID: "op_0000000000000022",
		ClientID: strings.Repeat("x", 1<<20), Op: proto.OpCancel,
		Cancel: &proto.CancelParams{OperationID: "op_0000000000000023"},
	})
	responses := decodeResponses(t, &output)
	if len(responses) != 1 || responses[0].Error == nil || responses[0].Error.Code != proto.CodeInvalidRequest {
		t.Fatalf("invalid cancel identity response = %+v", responses)
	}
	if len(server.early.items) != 0 {
		t.Fatalf("invalid cancel consumed early tombstone capacity: %d", len(server.early.items))
	}
}

func TestAgentBusinessErrorsUseStableEnvelopes(t *testing.T) {
	secretPath := filepath.Join(t.TempDir(), "sensitive-name")
	tests := []struct {
		name  string
		code  proto.ErrorCode
		state proto.ExecutionState
		req   *proto.Request
	}{
		{
			name: "output_hard_limit", code: proto.CodeLimitExceeded, state: proto.StateFailed,
			req: &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"true"}, MaxOutputBytes: 600000}},
		},
		{
			name: "missing_file", code: proto.CodeObjectNotFound, state: proto.StateFailed,
			req: &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: secretPath}},
		},
		{
			name: "invalid_offset", code: proto.CodeInvalidRequest, state: proto.StateFailed,
			req: &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: "unused", Offset: -1}},
		},
		{
			name: "missing_job", code: proto.CodeObjectNotFound, state: proto.StateFailed,
			req: &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: "absent-job"}},
		},
		{
			name: "missing_job_wait", code: proto.CodeObjectNotFound, state: proto.StateFailed,
			req: &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: "absent-job", WaitTimeoutSec: 1}},
		},
		{
			name: "process_start", code: proto.CodeProcessStartFailure, state: proto.StateFailed,
			req: &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"/definitely/not/a/real/rdev-program"}}},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
			t.Cleanup(server.close)
			tt.req.ID = fmt.Sprintf("request-%d", i)
			tt.req.OperationID = fmt.Sprintf("op_%016x", i+500)
			tt.req.ClientID = "client_0123456789abcdef"
			server.process(tt.req)
			responses := decodeResponses(t, &output)
			terminals := 0
			for _, response := range responses {
				if response.ID != tt.req.ID || !response.Terminal {
					continue
				}
				terminals++
				envelope := response.Error
				if envelope == nil || envelope.Code != tt.code || envelope.ExecutionState != tt.state ||
					envelope.OperationID != tt.req.OperationID || !envelope.Terminal || response.OK {
					t.Fatalf("terminal envelope = %+v response=%+v", envelope, response)
				}
				descriptor, ok := proto.LookupError(tt.code)
				if !ok || envelope.Category != descriptor.Category || envelope.Retry != descriptor.Retry ||
					envelope.Retryable != descriptor.Retryable || envelope.Message != descriptor.Message {
					t.Fatalf("unstable descriptor projection: %+v want %+v", envelope, descriptor)
				}
				if strings.Contains(envelope.Message, secretPath) || strings.Contains(envelope.Message, "absent-job") ||
					strings.Contains(envelope.Message, "/definitely/") {
					t.Fatalf("public envelope leaked private diagnostics: %+v", envelope)
				}
			}
			if terminals != 1 {
				t.Fatalf("terminal count = %d, want 1; responses=%+v", terminals, responses)
			}
		})
	}
}

func TestPanicRecoveryUsesPhaseCoherentTypedTerminal(t *testing.T) {
	t.Run("before_accepted", func(t *testing.T) {
		var output bytes.Buffer
		server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
		server.cache = nil
		server.process(&proto.Request{
			ID: "pre", OperationID: "op_0000000000000600", ClientID: "client_0123456789abcdef",
			Op: proto.OpReadFile, Read: &proto.ReadParams{Path: "synthetic"},
		})
		responses := decodeResponses(t, &output)
		server.close()
		if len(responses) != 1 || responses[0].Seq != 1 || responses[0].Error == nil ||
			responses[0].Error.Code != proto.CodeInternalFailure || responses[0].Execution != proto.StateNotSent {
			t.Fatalf("pre-accepted panic = %+v", responses)
		}
	})

	t.Run("after_progress", func(t *testing.T) {
		state := t.TempDir()
		dir := jobDir(state, "panic-job")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		meta, _ := json.Marshal(&jobMeta{ID: "panic-job", PID: 0, StartedAt: "synthetic"})
		if err := os.WriteFile(filepath.Join(dir, "meta.json"), meta, 0o600); err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		server := newAgentServer(context.Background(), state, testResponseWriter(&output))
		server.waits = newWaitHub(panicWaitClock{})
		server.process(&proto.Request{
			ID: "post", OperationID: "op_0000000000000601", ClientID: "client_0123456789abcdef",
			Op: proto.OpJobWait, Job: &proto.JobParams{ID: "panic-job", WaitTimeoutSec: 1},
		})
		responses := decodeResponses(t, &output)
		server.close()
		if len(responses) != 3 || responses[2].Seq != 3 || responses[2].Error == nil ||
			responses[2].Error.Code != proto.CodeInternalFailure || responses[2].Execution != proto.StatePossiblyExecuted {
			t.Fatalf("post-accepted panic = %+v", responses)
		}
	})
}

func TestNegotiatedV3WithoutStreamingUsesTypedTerminalWithoutData(t *testing.T) {
	var output bytes.Buffer
	writer := testResponseWriter(&output)
	server := newAgentServer(context.Background(), t.TempDir(), writer)
	t.Cleanup(server.close)
	features := make([]proto.Feature, 0)
	for _, feature := range proto.SupportedFeatures() {
		if feature != proto.FeatureStreaming {
			features = append(features, feature)
		}
	}
	server.process(&proto.Request{
		ID: "hello", Op: proto.OpPing,
		Hello: &proto.HelloParams{MinVersion: 3, MaxVersion: 3, Features: features},
	})
	server.process(&proto.Request{
		ID: "target", OperationID: "op_0000000000000510", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, StreamWindowBytes: 1024,
		Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "printf data"}},
	})
	var accepted, terminal, streamed int
	for _, response := range decodeResponses(t, &output) {
		if response.ID != "target" {
			continue
		}
		switch response.Type {
		case proto.EventAccepted:
			accepted++
		case proto.EventData, proto.EventProgress:
			streamed++
		case proto.EventFinal:
			if response.Terminal && response.Execution == proto.StateCompleted {
				terminal++
			}
		}
	}
	if accepted != 1 || terminal != 1 || streamed != 0 {
		t.Fatalf("accepted=%d terminal=%d streamed=%d", accepted, terminal, streamed)
	}
	writer.flush()
}

func TestRunningCancelKillsOnlyTargetProcessGroup(t *testing.T) {
	root := t.TempDir()
	pidFile := filepath.Join(root, "pid")
	var output bytes.Buffer
	server := newAgentServer(context.Background(), root, testResponseWriter(&output))
	t.Cleanup(server.close)
	request := &proto.Request{
		ID: "slow", OperationID: "op_0000000000000030", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, StreamWindowBytes: 1024,
		Exec: &proto.ExecParams{Argv: []string{"sh", "-c", fmt.Sprintf("echo $$ > %q; sleep 30 & wait", pidFile)}},
	}
	done := make(chan struct{})
	go func() { server.process(request); close(done) }()

	var pgid int
	deadline := time.Now().Add(5 * time.Second)
	for pgid == 0 && time.Now().Before(deadline) {
		data, err := os.ReadFile(pidFile)
		if err == nil {
			_, _ = fmt.Sscanf(string(data), "%d", &pgid)
		}
		runtimeYield()
	}
	if pgid == 0 {
		t.Fatal("foreground command did not reach the start barrier")
	}
	server.handleCancel(&proto.Request{
		ID: "cancel", OperationID: "op_0000000000000031", ClientID: request.ClientID,
		Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: request.OperationID},
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled foreground operation did not terminate")
	}
	if err := syscall.Kill(-pgid, 0); err == nil {
		t.Fatalf("target process group %d is still alive", pgid)
	}

	quick := &proto.Request{
		ID: "quick", OperationID: "op_0000000000000032", ClientID: request.ClientID,
		Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "printf alive"}},
	}
	server.process(quick)
	var targetTerminals, quickTerminals int
	for _, response := range decodeResponses(t, &output) {
		if response.ID == request.ID && response.Terminal {
			targetTerminals++
		}
		if response.ID == quick.ID && response.Terminal && response.OK {
			quickTerminals++
		}
	}
	if targetTerminals != 1 || quickTerminals != 1 {
		t.Fatalf("target terminals=%d quick successful terminals=%d", targetTerminals, quickTerminals)
	}
}

func TestForegroundEscalationOutlivesLeaderForCancelDeadlineAndDisconnect(t *testing.T) {
	originalGrace := processGroupGrace
	processGroupGrace = 10 * time.Millisecond
	t.Cleanup(func() { processGroupGrace = originalGrace })

	for _, trigger := range []string{"cancel", "deadline", "disconnect"} {
		t.Run(trigger, func(t *testing.T) {
			root := t.TempDir()
			leaderFile := filepath.Join(root, "leader")
			childFile := filepath.Join(root, "child")
			var output bytes.Buffer
			responseWriter := testResponseWriter(&output)
			server := newAgentServer(context.Background(), root, responseWriter)
			t.Cleanup(server.close)
			var deadlineReady chan func()
			request := &proto.Request{
				ID: "target", OperationID: "op_2000000000000001", ClientID: "client_0123456789abcdef",
				Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", fmt.Sprintf(`
trap 'exit 0' TERM
(trap '' TERM; exec </dev/null >/dev/null 2>&1; sleep 30) &
child=$!
printf '%%s' "$$" > %q
printf '%%s' "$child" > %q
wait
`, leaderFile, childFile)}},
			}
			if trigger == "deadline" {
				deadlineReady = make(chan func(), 1)
				request.DeadlineUnixMilli = 1
				server.withDeadline = func(parent context.Context, _ time.Time) (context.Context, context.CancelFunc) {
					ctx := newManualDeadline(parent)
					deadlineReady <- ctx.expire
					return ctx, ctx.cancel
				}
			}

			done := make(chan struct{})
			go func() { server.process(request); close(done) }()
			pgid := waitForPIDFile(t, leaderFile)
			child := waitForPIDFile(t, childFile)
			switch trigger {
			case "cancel":
				server.handleCancel(&proto.Request{
					ID: "cancel", OperationID: "op_2000000000000002", ClientID: request.ClientID,
					Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: request.OperationID},
				})
			case "deadline":
				(<-deadlineReady)()
			case "disconnect":
				server.close()
			}
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("foreground operation did not terminate")
			}
			if err := syscall.Kill(-pgid, 0); !errors.Is(err, syscall.ESRCH) {
				t.Fatalf("original process group %d survived escalation: %v", pgid, err)
			}
			if processAlive(child) {
				t.Fatalf("TERM-ignoring child %d survived escalation", child)
			}
			quickOutput := &output
			quickServer := server
			var quickWriter *respWriter
			if trigger == "disconnect" {
				quickOutput = &bytes.Buffer{}
				quickWriter = testResponseWriter(quickOutput)
				quickServer = newAgentServer(context.Background(), t.TempDir(), quickWriter)
				t.Cleanup(quickServer.close)
			}
			quickServer.process(&proto.Request{
				ID: "quick", OperationID: "op_2000000000000003", ClientID: request.ClientID,
				Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "printf alive"}},
			})
			terminals := 0
			for _, response := range decodeResponses(t, &output) {
				if response.ID == request.ID && response.Terminal {
					terminals++
				}
			}
			if terminals != 1 {
				t.Fatalf("target terminal frames = %d, want 1", terminals)
			}
			quickTerminals := 0
			for _, response := range decodeResponses(t, quickOutput) {
				if response.ID == "quick" && response.Terminal && response.OK {
					quickTerminals++
				}
			}
			if quickTerminals != 1 {
				t.Fatalf("healthy request terminals = %d, want 1", quickTerminals)
			}
			if quickWriter != nil {
				quickWriter.flush()
			}
			responseWriter.flush()
		})
	}
}

type manualDeadline struct {
	context.Context
	done chan struct{}
	once sync.Once
	err  error
}

func newManualDeadline(parent context.Context) *manualDeadline {
	return &manualDeadline{Context: parent, done: make(chan struct{})}
}

func (c *manualDeadline) Done() <-chan struct{}       { return c.done }
func (c *manualDeadline) Err() error                  { return c.err }
func (c *manualDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *manualDeadline) expire() {
	c.once.Do(func() { c.err = context.DeadlineExceeded; close(c.done) })
}
func (c *manualDeadline) cancel() { c.once.Do(func() { c.err = context.Canceled; close(c.done) }) }

func TestExecDataFrameArrivesWhileProcessIsRunning(t *testing.T) {
	root := t.TempDir()
	gate := filepath.Join(root, "release")
	reader, writer := io.Pipe()
	responseWriter := newRespWriter(writer, writer.Close)
	server := newAgentServer(context.Background(), root, responseWriter)
	t.Cleanup(server.close)
	request := &proto.Request{
		ID: "stream", OperationID: "op_0000000000000024", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, StreamWindowBytes: 64 << 10,
		Exec: &proto.ExecParams{
			Argv:           []string{"sh", "-c", `printf first; while [ ! -f "$1" ]; do sleep 0.01; done; printf second`, "sh", gate},
			MaxOutputBytes: 64 << 10,
		},
	}
	done := make(chan struct{})
	go func() {
		server.process(request)
		responseWriter.flush()
		close(done)
	}()
	events := make(chan proto.Response, 8)
	go func() {
		decoder := json.NewDecoder(reader)
		for {
			var response proto.Response
			if decoder.Decode(&response) != nil {
				close(events)
				return
			}
			events <- response
		}
	}()

	deadline := time.After(3 * time.Second)
	sawData := false
	for !sawData {
		select {
		case response := <-events:
			if response.Terminal {
				t.Fatal("terminal arrived before the command's release barrier")
			}
			if response.Type == proto.EventData {
				if response.Data == nil {
					t.Fatal("data event has no data frame")
				}
				decoded, err := base64.StdEncoding.DecodeString(response.Data.Content)
				if err != nil || string(decoded) != "first" {
					t.Fatalf("first data frame = %+v decoded=%q err=%v", response, decoded, err)
				}
				sawData = true
			}
		case <-deadline:
			t.Fatal("no data frame arrived while command was blocked")
		}
	}
	if err := os.WriteFile(gate, []byte("release"), 0o600); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case response := <-events:
			if response.Terminal {
				if response.Exec == nil || response.Exec.Stdout != "firstsecond" {
					t.Fatalf("final stream result = %+v", response)
				}
				<-done
				return
			}
		case <-deadline:
			t.Fatal("no final frame after releasing command")
		}
	}
}

func TestSlowDataConsumerCannotBlockTerminalControlFrame(t *testing.T) {
	blocked := newBlockingFrameWriter()
	writer := &respWriter{out: blocked, closeOut: blocked.Close, writeTimeout: 25 * time.Millisecond}
	request := &proto.Request{
		ID: "slow", OperationID: "op_0000000000000025",
		StreamWindowBytes: 64 << 10,
	}
	emitter := newExecStreamEmitter(writer, request, 2)
	dataDone := make(chan struct{})
	go func() {
		emitter.emit("stdout", bytes.Repeat([]byte("x"), 1<<20))
		close(dataDone)
	}()
	<-blocked.entered
	terminalDone := make(chan bool, 1)
	go func() {
		terminalDone <- writer.write(&proto.Response{
			ID: request.ID, OperationID: request.OperationID, Type: proto.EventFinal,
			Seq: emitter.finalSeq(), Terminal: true, OK: true, Execution: proto.StateCompleted,
		})
	}()
	select {
	case wrote := <-terminalDone:
		if wrote {
			t.Fatal("terminal reported success after the underlying pipe stalled")
		}
	case <-time.After(time.Second):
		t.Fatal("terminal remained blocked behind a stalled data write")
	}
	<-dataDone

	var output bytes.Buffer
	healthy := testResponseWriter(&output)
	if !healthy.write(&proto.Response{ID: "healthy", Type: proto.EventFinal, Terminal: true, OK: true, Execution: proto.StateCompleted}) {
		t.Fatal("an independent normal writer stopped working")
	}
	healthy.flush()
}

type blockingFrameWriter struct {
	entered chan struct{}
	release chan struct{}
	enter   sync.Once
	close   sync.Once
}

func newBlockingFrameWriter() *blockingFrameWriter {
	return &blockingFrameWriter{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingFrameWriter) Write(p []byte) (int, error) {
	w.enter.Do(func() { close(w.entered) })
	<-w.release
	return 0, io.ErrClosedPipe
}

func (w *blockingFrameWriter) Close() error {
	w.close.Do(func() { close(w.release) })
	return nil
}

func TestCancelAfterFinalDoesNotCreateSecondTargetTerminal(t *testing.T) {
	var output bytes.Buffer
	server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
	t.Cleanup(server.close)
	target := &proto.Request{
		ID: "target", OperationID: "op_0000000000000040", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"sh", "-c", "true"}},
	}
	server.process(target)
	server.handleCancel(&proto.Request{
		ID: "cancel", OperationID: "op_0000000000000041", ClientID: target.ClientID,
		Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: target.OperationID},
	})
	terminals := 0
	for _, response := range decodeResponses(t, &output) {
		if response.ID == target.ID && response.Terminal {
			terminals++
		}
	}
	if terminals != 1 {
		t.Fatalf("target received %d terminal frames after late cancel", terminals)
	}
}

func TestCanceledOperationCacheCannotPublishSuccess(t *testing.T) {
	cache := newOperationCache(realRuntimeClock{}, 4, time.Minute)
	request := &proto.Request{
		ID: "exec", OperationID: "op_0000000000000042", ClientID: "client_0123456789abcdef",
		Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"true"}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	begin := cache.begin(request, cancel)
	if begin.record == nil || begin.envelope != nil {
		t.Fatalf("begin = %+v", begin)
	}
	if found, terminal, eligible := cache.cancel(request.ClientID, request.OperationID, request.Op); !found || terminal || !eligible {
		t.Fatalf("cancel found=%v terminal=%v eligible=%v", found, terminal, eligible)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("accepted cancel did not cancel operation context")
	}
	success := &proto.Response{
		ID: request.ID, OperationID: request.OperationID, Type: proto.EventFinal,
		Seq: 3, Terminal: true, Execution: proto.StateCompleted, OK: true,
	}
	if !cache.finish(begin.record, success) {
		t.Fatal("finish rejected the first terminal")
	}
	if success.OK || success.Type != proto.EventError || success.Error == nil ||
		success.Error.Code != proto.CodeAmbiguousOutcome || success.Execution != proto.StatePossiblyExecuted {
		t.Fatalf("cancel was overwritten by success: %+v", success)
	}
	if cache.finish(begin.record, &proto.Response{OK: true}) {
		t.Fatal("operation cache accepted a second terminal")
	}
}

func TestIneligibleOperationCacheCannotBeCanceled(t *testing.T) {
	for _, request := range []*proto.Request{
		mutationRequest("op_0000000000000043", "payload", false),
		{ID: "read", OperationID: "op_0000000000000044", ClientID: "client_0123456789abcdef", Op: proto.OpReadFile, Read: &proto.ReadParams{Path: "synthetic"}},
	} {
		cache := newOperationCache(realRuntimeClock{}, 4, time.Minute)
		canceled := false
		begin := cache.begin(request, func() { canceled = true })
		if begin.record == nil || begin.envelope != nil {
			t.Fatalf("begin = %+v", begin)
		}
		if found, _, eligible := cache.cancel(request.ClientID, request.OperationID, request.Op); !found || eligible {
			t.Fatalf("ineligible cancel found=%v eligible=%v op=%s", found, eligible, request.Op)
		}
		if canceled || begin.record.canceled {
			t.Fatalf("ineligible operation %s was canceled", request.Op)
		}
	}
}

func TestProtocolDeadlineRejectedForIndependentMutation(t *testing.T) {
	var output bytes.Buffer
	server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
	t.Cleanup(server.close)
	request := mutationRequest("op_0000000000000044", "payload", false)
	request.ID = "deadline-write"
	request.DeadlineUnixMilli = time.Now().Add(time.Minute).UnixMilli()
	server.process(request)
	responses := decodeResponses(t, &output)
	if len(responses) != 1 || responses[0].Type != proto.EventError || responses[0].Error == nil ||
		responses[0].Error.Code != proto.CodeInvalidRequest || responses[0].Execution != proto.StateNotSent {
		t.Fatalf("independent mutation deadline = %+v", responses)
	}
}

func TestCancelFastPathCannotBypassControlValidation(t *testing.T) {
	for _, mutate := range []func(*proto.Request){
		func(request *proto.Request) { request.DeadlineUnixMilli = time.Now().Add(time.Minute).UnixMilli() },
		func(request *proto.Request) { request.StreamWindowBytes = proto.AbsoluteStreamWindowBytes + 1 },
	} {
		var output bytes.Buffer
		server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
		request := &proto.Request{
			ID: "cancel", OperationID: "op_0000000000000045", ClientID: "client_0123456789abcdef",
			Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: "op_0000000000000046"},
		}
		mutate(request)
		server.submit(*request)
		responses := decodeResponses(t, &output)
		server.close()
		if len(responses) != 1 || responses[0].Error == nil || responses[0].OK ||
			(responses[0].Error.Code != proto.CodeInvalidRequest && responses[0].Error.Code != proto.CodeLimitExceeded) {
			t.Fatalf("cancel control validation = %+v", responses)
		}
	}
}

func TestCancelPolicyCannotTargetOrPoisonIndependentOperation(t *testing.T) {
	t.Run("live_target", func(t *testing.T) {
		var output bytes.Buffer
		server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
		t.Cleanup(server.close)
		target := mutationRequest("op_0000000000000047", "payload", false)
		canceled := false
		begin := server.cache.begin(target, func() { canceled = true })
		if begin.record == nil {
			t.Fatal("target was not admitted")
		}
		server.handleCancel(&proto.Request{
			ID: "cancel", OperationID: "op_0000000000000048", ClientID: target.ClientID,
			Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: target.OperationID, TargetOp: target.Op},
		})
		responses := decodeResponses(t, &output)
		if len(responses) != 1 || responses[0].Error == nil ||
			responses[0].Error.Code != proto.CodeProcessInvalidState || canceled || begin.record.canceled {
			t.Fatalf("ineligible live cancel responses=%+v canceled=%v record=%+v", responses, canceled, begin.record)
		}
	})

	t.Run("early_target_op_binding", func(t *testing.T) {
		state := t.TempDir()
		var output bytes.Buffer
		server := newAgentServer(context.Background(), state, testResponseWriter(&output))
		t.Cleanup(server.close)
		targetID := "op_0000000000000049"
		server.handleCancel(&proto.Request{
			ID: "cancel", OperationID: "op_000000000000004a", ClientID: "client_0123456789abcdef",
			Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: targetID, TargetOp: proto.OpExec},
		})
		server.process(&proto.Request{
			ID: "write", OperationID: targetID, ClientID: "client_0123456789abcdef",
			Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: filepath.Join(state, "kept"), Content: "payload"},
		})
		responses := decodeResponses(t, &output)
		terminals := 0
		for _, response := range responses {
			if response.ID == "write" && response.Terminal {
				terminals++
				if !response.OK || response.Execution != proto.StateCompleted {
					t.Fatalf("mismatched early cancel poisoned write: %+v", response)
				}
			}
		}
		if terminals != 1 {
			t.Fatalf("write terminals=%d responses=%+v", terminals, responses)
		}
	})
}

func TestNegotiatedFeaturesGateDeadlineAndCancel(t *testing.T) {
	for _, tt := range []struct {
		name     string
		version  int
		omit     proto.Feature
		request  *proto.Request
		wantCode proto.ErrorCode
	}{
		{
			name: "v3_deadline_omitted", version: 3, omit: proto.FeatureDeadline,
			request: &proto.Request{
				ID: "target", OperationID: "op_0000000000000060", ClientID: "client_0123456789abcdef",
				Op: proto.OpExec, DeadlineUnixMilli: time.Now().Add(time.Minute).UnixMilli(),
				Exec: &proto.ExecParams{Argv: []string{"true"}},
			},
			wantCode: proto.CodeUnsupportedFeature,
		},
		{
			name: "v3_cancel_omitted", version: 3, omit: proto.FeatureCancel,
			request: &proto.Request{
				ID: "target", OperationID: "op_0000000000000061", ClientID: "client_0123456789abcdef",
				Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: "op_0000000000000062", TargetOp: proto.OpExec},
			},
			wantCode: proto.CodeUnsupportedFeature,
		},
		{
			name: "v2_cancel", version: 2,
			request: &proto.Request{
				ID: "target", OperationID: "op_0000000000000063", ClientID: "client_0123456789abcdef",
				Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: "op_0000000000000064", TargetOp: proto.OpExec},
			},
			wantCode: proto.CodeUnsupportedFeature,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			server := newAgentServer(context.Background(), t.TempDir(), testResponseWriter(&output))
			features := make([]proto.Feature, 0, len(proto.SupportedFeatures()))
			for _, feature := range proto.SupportedFeatures() {
				if feature != tt.omit {
					features = append(features, feature)
				}
			}
			server.process(&proto.Request{
				ID: "hello", Op: proto.OpPing,
				Hello: &proto.HelloParams{MinVersion: tt.version, MaxVersion: tt.version, Features: features},
			})
			server.submit(*tt.request)
			responses := decodeResponses(t, &output)
			server.close()
			var terminal *proto.Response
			for i := range responses {
				if responses[i].ID == tt.request.ID && responses[i].Terminal {
					terminal = &responses[i]
				}
			}
			if terminal == nil || terminal.Error == nil || terminal.Error.Code != tt.wantCode || terminal.Execution != proto.StateNotSent {
				t.Fatalf("negotiated feature response = %+v all=%+v", terminal, responses)
			}
		})
	}
}

func TestDisconnectCancelsForegroundAndKeepsAcceptedDetachedJob(t *testing.T) {
	t.Run("foreground", func(t *testing.T) {
		root := t.TempDir()
		pidFile := filepath.Join(root, "foreground-pid")
		var output bytes.Buffer
		server := newAgentServer(context.Background(), root, testResponseWriter(&output))
		request := &proto.Request{
			ID: "foreground", OperationID: "op_0000000000000050", ClientID: "client_0123456789abcdef",
			Op:   proto.OpExec,
			Exec: &proto.ExecParams{Argv: []string{"sh", "-c", fmt.Sprintf("echo $$ > %q; sleep 30 & wait", pidFile)}},
		}
		done := make(chan struct{})
		go func() { server.process(request); close(done) }()
		pgid := waitForPIDFile(t, pidFile)
		server.close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("foreground process did not stop after disconnect")
		}
		if err := syscall.Kill(-pgid, 0); err == nil {
			t.Fatalf("foreground process group %d survived disconnect", pgid)
		}
	})

	t.Run("detached", func(t *testing.T) {
		root := t.TempDir()
		var output bytes.Buffer
		server := newAgentServer(context.Background(), root, testResponseWriter(&output))
		request := &proto.Request{
			ID: "detached", OperationID: "op_0000000000000051", ClientID: "client_0123456789abcdef",
			Op:  proto.OpJobStart,
			Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "sleep 30"}}},
		}
		done := make(chan struct{})
		go func() { server.process(request); close(done) }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			server.cache.mu.Lock()
			record := server.cache.records[operationCacheKey(request.ClientID, request.OperationID)]
			server.cache.mu.Unlock()
			if record != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("detached request was not accepted")
			}
			runtimeYield()
		}
		server.close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("accepted detached start did not finish after disconnect")
		}
		var info *proto.JobInfo
		for _, response := range decodeResponses(t, &output) {
			if response.ID == request.ID && response.Terminal && response.Job != nil {
				info = response.Job.Info
			}
		}
		if info == nil || !processAlive(info.PID) {
			t.Fatalf("detached job did not survive disconnect: %+v", info)
		}
		if _, err := jobStop(&proto.JobParams{ID: info.ID, Signal: "KILL"}, root); err != nil {
			t.Fatalf("cleanup detached job: %v", err)
		}
	})
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			var pid int
			if _, scanErr := fmt.Sscanf(string(data), "%d", &pid); scanErr == nil && pid > 0 {
				return pid
			}
		}
		runtimeYield()
	}
	t.Fatal("process did not reach pid-file barrier")
	return 0
}

func runtimeYield() { runtime.Gosched() }

func TestWaitHubFanoutLimitsAndCancellationCleanup(t *testing.T) {
	state := t.TempDir()
	id := "synthetic-job"
	dir := jobDir(state, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
		ID: id, PID: os.Getpid(), StartedAt: time.Now().UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatal(err)
	}
	hub := newWaitHub(realWaitClock{})
	hub.maxWatchers, hub.maxWaiters, hub.perJob = 2, 4, 4
	var cancels []func()
	for i := 0; i < 4; i++ {
		_, cancel, err := hub.subscribe(id, state)
		if err != nil {
			t.Fatal(err)
		}
		cancels = append(cancels, cancel)
	}
	if watchers, waiters := hub.counts(); watchers != 1 || waiters != 4 {
		t.Fatalf("fanout counts=%d/%d, want 1/4", watchers, waiters)
	}
	if _, _, err := hub.subscribe(id, state); err == nil {
		t.Fatal("waiter limit was not enforced")
	}
	for _, cancel := range cancels {
		cancel()
	}
	if watchers, waiters := hub.counts(); watchers != 0 || waiters != 0 {
		t.Fatalf("canceled subscriptions leaked: %d/%d", watchers, waiters)
	}
}

func TestAgentFrameAndOutputHardLimitsPreserveBinaryMetadata(t *testing.T) {
	t.Run("inbound no newline", func(t *testing.T) {
		input := bytes.NewReader(bytes.Repeat([]byte{'x'}, maxRequestLineLen+1))
		if _, err := readLine(bufio.NewReaderSize(input, 1<<20)); err == nil {
			t.Fatal("delimiter-free oversized request was accepted")
		}
	})

	t.Run("outbound marshal budget", func(t *testing.T) {
		var output bytes.Buffer
		writer := testResponseWriter(&output)
		writer.write(&proto.Response{
			ID: "oversized", OperationID: "op_0000000000000060", Type: proto.EventFinal,
			Seq: 2, Terminal: true, OK: true,
			Exec: &proto.ExecResult{Stdout: strings.Repeat("x", int(proto.AbsoluteResponseFrameBytes)+1)},
		})
		if int64(output.Len()) > proto.AbsoluteResponseFrameBytes {
			t.Fatalf("writer emitted %d-byte oversized frame", output.Len())
		}
		responses := decodeResponses(t, &output)
		if len(responses) != 1 || responses[0].Error == nil || responses[0].Error.Code != proto.CodeFrameTooLarge ||
			responses[0].Error.Truncation == nil || !responses[0].Error.Truncation.Truncated {
			t.Fatalf("oversized response fallback = %+v", responses)
		}
	})

	t.Run("binary and infinite output", func(t *testing.T) {
		binary, err := doExec(&proto.ExecParams{Argv: []string{"sh", "-c", "printf '\\377\\000A'"}, MaxOutputBytes: 32})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := base64.StdEncoding.DecodeString(binary.Stdout)
		if err != nil || !binary.StdoutB64 || !bytes.Equal(decoded, []byte{0xff, 0x00, 'A'}) {
			t.Fatalf("binary output was not preserved: b64=%t data=%x err=%v", binary.StdoutB64, decoded, err)
		}
		large, err := doExec(&proto.ExecParams{
			Argv:           []string{"sh", "-c", "yes x | head -c 2000000; yes e | head -c 2000000 >&2"},
			MaxOutputBytes: 4096,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !large.StdoutTruncation.Truncated || !large.StderrTruncation.Truncated ||
			large.StdoutTruncation.RetainedBytes > 4096 || large.StderrTruncation.RetainedBytes > 4096 {
			t.Fatalf("unbounded output metadata = %+v / %+v", large.StdoutTruncation, large.StderrTruncation)
		}
	})
}

func TestAdmissionQueueDoesNotCreateGoroutinePerRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output bytes.Buffer
	server := &agentServer{
		ctx: ctx, cancel: cancel, state: t.TempDir(), writer: testResponseWriter(&output),
		normalQ: make(chan queuedRequest, 4), waitQ: make(chan queuedRequest, 2),
	}
	before := runtime.NumGoroutine()
	for i := 0; i < 1000; i++ {
		server.submit(proto.Request{ID: fmt.Sprint(i), Op: proto.OpPing})
	}
	after := runtime.NumGoroutine()
	if len(server.normalQ) != cap(server.normalQ) {
		t.Fatalf("queue depth=%d, want cap %d", len(server.normalQ), cap(server.normalQ))
	}
	if after-before > 2 {
		t.Fatalf("1000 queued requests created %d goroutines", after-before)
	}
}
