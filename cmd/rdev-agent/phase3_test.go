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
	return &respWriter{out: bufio.NewWriter(buffer)}
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
	target := mutationRequest("op_0000000000000020", "payload", false)
	target.ID = "target"
	server.handleCancel(&proto.Request{
		ID: "cancel", OperationID: "op_0000000000000021", ClientID: target.ClientID,
		Op: proto.OpCancel, Cancel: &proto.CancelParams{OperationID: target.OperationID},
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

func TestExecDataFrameArrivesWhileProcessIsRunning(t *testing.T) {
	root := t.TempDir()
	gate := filepath.Join(root, "release")
	reader, writer := io.Pipe()
	server := newAgentServer(context.Background(), root, &respWriter{out: bufio.NewWriter(writer)})
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
		_ = writer.Close()
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
	var output bytes.Buffer
	writer := testResponseWriter(&output)
	request := &proto.Request{
		ID: "slow", OperationID: "op_0000000000000025",
		StreamWindowBytes: 64 << 10,
	}
	emitter := newExecStreamEmitter(writer, request, 2)
	writer.mu.Lock() // model a full/contended data path
	start := time.Now()
	emitter.emit("stdout", bytes.Repeat([]byte("x"), 1<<20))
	if elapsed := time.Since(start); elapsed > time.Second {
		writer.mu.Unlock()
		t.Fatalf("slow data path blocked emitter for %v", elapsed)
	}
	writer.mu.Unlock()
	if !writer.write(&proto.Response{
		ID: request.ID, OperationID: request.OperationID, Type: proto.EventFinal,
		Seq: emitter.finalSeq(), Terminal: true, OK: true, Execution: proto.StateCompleted,
	}) {
		t.Fatal("terminal control frame was not written")
	}
	responses := decodeResponses(t, &output)
	if len(responses) != 1 || !responses[0].Terminal || responses[0].Type != proto.EventFinal {
		t.Fatalf("slow consumer isolation responses = %+v", responses)
	}
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
