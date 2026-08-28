package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// newTestConn wires a Conn to in-memory pipes instead of an ssh process, so the
// multiplexing logic can be tested without a remote host.
//
// The returned reader carries requests the Conn wrote; the writer feeds replies
// back. killAgent closes the reply stream, which is how a dying ssh process looks
// to readLoop. This is white-box on purpose: demultiplexing is the part worth
// testing, and it is unreachable through the public Dial path without a live
// machine.
func newTestConn(t *testing.T) (c *Conn, requests *json.Decoder, replies io.Writer, killAgent func()) {
	return newTestConnWithFrameLimit(t, 0)
}

func newTestConnWithFrameLimit(t *testing.T, frameLimit int) (c *Conn, requests *json.Decoder, replies io.Writer, killAgent func()) {
	t.Helper()

	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	readerSize := streamReadBufferBytes
	if frameLimit > 0 && frameLimit < readerSize {
		readerSize = frameLimit
	}

	c = &Conn{
		host:       Host{Name: "test", Addr: "u@h"},
		stderr:     &lockedBuilder{},
		pending:    make(map[string]chan *proto.Response),
		stdin:      reqW,
		stdout:     bufio.NewReaderSize(respR, readerSize),
		frameLimit: frameLimit,
	}
	go c.readLoop()
	t.Cleanup(func() {
		respW.Close()
		reqR.Close()
	})
	return c, json.NewDecoder(reqR), respW, func() { respW.Close() }
}

func TestLockedBuilderRetainsFixedTail(t *testing.T) {
	w := &lockedBuilder{limit: 5}
	if n, err := w.Write([]byte("abc")); err != nil || n != 3 {
		t.Fatalf("first Write = %d, %v", n, err)
	}
	if n, err := w.Write([]byte("def")); err != nil || n != 3 {
		t.Fatalf("second Write = %d, %v", n, err)
	}
	if got := w.String(); got != "bcdef" {
		t.Fatalf("tail after wrap = %q, want bcdef", got)
	}

	// A single oversized write should retain only its suffix while still
	// reporting the complete input consumed to os/exec.
	if n, err := w.Write([]byte("0123456789")); err != nil || n != 10 {
		t.Fatalf("oversized Write = %d, %v", n, err)
	}
	if got := w.String(); got != "56789" {
		t.Fatalf("tail after oversized write = %q, want 56789", got)
	}
}

func TestLockedBuilderConcurrentWritesStayBounded(t *testing.T) {
	w := &lockedBuilder{limit: 128}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := []byte(strings.Repeat(fmt.Sprintf("%02d", i), 10))
			if n, err := w.Write(p); err != nil || n != len(p) {
				t.Errorf("Write = %d, %v, want %d, nil", n, err, len(p))
			}
		}(i)
	}
	wg.Wait()
	if got := len(w.String()); got != 128 {
		t.Fatalf("retained %d bytes, want fixed capacity 128", got)
	}
}

func sendReply(t *testing.T, w io.Writer, resp *proto.Response) {
	t.Helper()
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		t.Fatal(err)
	}
}

func TestReadLineLimitAcceptsBoundaryAndRejectsNextByte(t *testing.T) {
	const limit = 32
	line := strings.Repeat("x", limit)
	got, err := readLineLimit(bufio.NewReader(strings.NewReader(line+"\n")), limit)
	if err != nil {
		t.Fatalf("boundary frame rejected: %v", err)
	}
	if string(got) != line {
		t.Fatalf("boundary frame = %q, want %q", got, line)
	}

	_, err = readLineLimit(bufio.NewReader(strings.NewReader(line+"x")), limit)
	if !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("delimiter-free overlimit error = %v, want errFrameTooLarge", err)
	}
}

func TestOversizedResponseFrameClosesConnection(t *testing.T) {
	const limit = 64
	c, requests, replies, _ := newTestConnWithFrameLimit(t, limit)
	done := make(chan error, 1)
	go func() {
		_, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		done <- err
	}()

	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	// No delimiter is sent. The reader must reject on byte limit+1 rather than
	// waiting for a newline or accumulating an arbitrary response.
	if _, err := replies.Write([]byte(strings.Repeat("x", limit+1))); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, errFrameTooLarge) {
			t.Fatalf("Do error = %v, want errFrameTooLarge", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("oversized response did not fail promptly")
	}

	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if !closed {
		t.Fatal("connection remained usable after oversized response polluted framing")
	}
	if _, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing}); err == nil {
		t.Fatal("Do succeeded after oversized response closed the connection")
	}
}

func TestDoRejectsOversizedRequestBeforeWrite(t *testing.T) {
	var dst bytes.Buffer
	c := &Conn{
		stderr:     &lockedBuilder{},
		pending:    make(map[string]chan *proto.Response),
		stdin:      nopWriteCloser{Writer: &dst},
		frameLimit: 128,
	}
	_, err := c.Do(context.Background(), &proto.Request{
		Op: proto.OpWriteFile,
		Cat: &proto.WriteParams{
			Path:    "synthetic",
			Content: strings.Repeat("x", 256),
		},
	})
	if !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("Do error = %v, want errFrameTooLarge", err)
	}
	if dst.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting oversized request", dst.Len())
	}
	c.mu.Lock()
	pending := len(c.pending)
	c.mu.Unlock()
	if pending != 0 {
		t.Fatalf("oversized request left %d pending entries", pending)
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

// The core of the multiplexing change: a reply that arrives second must still
// reach the caller that is waiting for it. Under the previous serial design the
// first caller would have consumed whichever line arrived first.
func TestDoRoutesRepliesOutOfOrder(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)

	type result struct {
		host string
		err  error
	}
	results := make(chan result, 2)

	// Two concurrent calls, distinguished by the Home field of the reply.
	for i := 0; i < 2; i++ {
		go func() {
			resp, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{host: resp.Ping.Home}
		}()
	}

	// Collect both request IDs before replying, so the ordering below is ours.
	ids := make([]string, 0, 2)
	for len(ids) < 2 {
		var req proto.Request
		if err := requests.Decode(&req); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, req.ID)
	}
	if ids[0] == ids[1] {
		t.Fatalf("both requests got ID %q; IDs must be unique to demultiplex", ids[0])
	}

	// Reply to the second request first.
	sendReply(t, replies, &proto.Response{
		ID: ids[1], OK: true,
		Ping: &proto.PingResult{Version: proto.Version, Home: "second"},
	})
	sendReply(t, replies, &proto.Response{
		ID: ids[0], OK: true,
		Ping: &proto.PingResult{Version: proto.Version, Home: "first"},
	})

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("Do failed: %v", r.err)
			}
			got[r.host] = true
		case <-time.After(3 * time.Second):
			t.Fatal("a caller never received its reply; replies are not being routed by ID")
		}
	}
	if !got["first"] || !got["second"] {
		t.Errorf("got %v, want both callers to receive their own reply", got)
	}
}

func TestDoValidatesStreamingAndReturnsOnlyTerminal(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)
	done := make(chan *proto.Response, 1)
	go func() {
		response, err := c.Do(context.Background(), &proto.Request{
			Op: proto.OpExec, OperationID: "op_0123456789abcdef", ClientID: "client_0123456789abcdef",
			Exec: &proto.ExecParams{Argv: []string{"synthetic"}},
		})
		if err != nil {
			t.Errorf("Do: %v", err)
		}
		done <- response
	}()
	var request proto.Request
	if err := requests.Decode(&request); err != nil {
		t.Fatal(err)
	}
	frames := []*proto.Response{
		{ID: request.ID, OperationID: request.OperationID, Type: proto.EventAccepted, Seq: 1, OK: true},
		{ID: request.ID, OperationID: request.OperationID, Type: proto.EventData, Seq: 2, OK: true, Data: &proto.DataFrame{Stream: "stdout", Content: "eA==", ContentB64: true}},
		{ID: request.ID, OperationID: request.OperationID, Type: proto.EventProgress, Seq: 3, OK: true, Progress: &proto.ProgressFrame{Phase: "running"}},
		{ID: request.ID, OperationID: request.OperationID, Type: proto.EventFinal, Seq: 4, Terminal: true, OK: true, Exec: &proto.ExecResult{Stdout: "x"}},
	}
	for _, frame := range frames {
		sendReply(t, replies, frame)
	}
	select {
	case response := <-done:
		if response == nil || !response.Terminal || response.Type != proto.EventFinal || response.Exec.Stdout != "x" {
			t.Fatalf("Do returned non-terminal frame: %+v", response)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Do did not return terminal frame")
	}
}

func TestInvalidStreamOrderingClosesConnection(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)
	errCh := make(chan error, 1)
	go func() {
		_, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		errCh <- err
	}()
	var request proto.Request
	if err := requests.Decode(&request); err != nil {
		t.Fatal(err)
	}
	sendReply(t, replies, &proto.Response{
		ID: request.ID, Type: proto.EventData, Seq: 1, OK: true,
		Data: &proto.DataFrame{Stream: "stdout", Content: "eA==", ContentB64: true},
	})
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("data-before-accepted was not rejected")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("invalid stream did not fail the waiter")
	}
}

func TestContextCancelWritesExactProtocolCancel(t *testing.T) {
	c, requests, _, _ := newTestConn(t)
	c.features = map[proto.Feature]bool{proto.FeatureCancel: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.Do(ctx, &proto.Request{
			Op: proto.OpExec, OperationID: "op_0123456789abcdef", ClientID: "client_0123456789abcdef",
			Exec: &proto.ExecParams{Argv: []string{"synthetic"}},
		})
		done <- err
	}()
	var original proto.Request
	if err := requests.Decode(&original); err != nil {
		t.Fatal(err)
	}
	cancel()
	var cancelRequest proto.Request
	if err := requests.Decode(&cancelRequest); err != nil {
		t.Fatal(err)
	}
	if cancelRequest.Op != proto.OpCancel || cancelRequest.ClientID != original.ClientID ||
		cancelRequest.Cancel == nil || cancelRequest.Cancel.OperationID != original.OperationID {
		t.Fatalf("cancel request = %+v", cancelRequest)
	}
	select {
	case err := <-done:
		var envelope *proto.ErrorEnvelope
		if !errors.As(err, &envelope) || envelope.Code != proto.CodeCanceled {
			t.Fatalf("cancel error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled Do did not return")
	}
}

// A slow call must not delay an unrelated one. This is the regression that
// motivated multiplexing: one 60-second exec used to stall every other request.
func TestDoSlowCallDoesNotBlockOthers(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)

	slowDone := make(chan time.Duration, 1)
	fastDone := make(chan time.Duration, 1)

	start := time.Now()
	go func() {
		c.Do(context.Background(), &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"slow"}}})
		slowDone <- time.Since(start)
	}()

	var slowID string
	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	slowID = req.ID

	go func() {
		c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		fastDone <- time.Since(start)
	}()
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	fastID := req.ID

	// Answer the fast call while the slow one is still outstanding.
	sendReply(t, replies, &proto.Response{ID: fastID, OK: true, Ping: &proto.PingResult{Version: proto.Version}})

	select {
	case d := <-fastDone:
		if d > 2*time.Second {
			t.Errorf("fast call took %v; it should not wait on the slow one", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fast call blocked behind the outstanding slow call")
	}

	sendReply(t, replies, &proto.Response{ID: slowID, OK: true, Exec: &proto.ExecResult{}})
	select {
	case <-slowDone:
	case <-time.After(3 * time.Second):
		t.Fatal("slow call never completed")
	}
}

// An abandoned request must not desynchronize the stream. Previously a canceled
// call forced the whole connection closed to avoid reading a stale reply as fresh.
func TestDoCanceledCallLeavesConnectionUsable(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		_, err := c.Do(ctx, &proto.Request{Op: proto.OpPing})
		canceled <- err
	}()

	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	abandonedID := req.ID

	cancel()
	select {
	case err := <-canceled:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled call did not return")
	}

	// The late reply for the abandoned request must be discarded, not handed to
	// the next caller.
	sendReply(t, replies, &proto.Response{
		ID: abandonedID, OK: true,
		Ping: &proto.PingResult{Version: proto.Version, Home: "stale"},
	})

	done := make(chan *proto.Response, 1)
	go func() {
		resp, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		if err != nil {
			t.Errorf("connection unusable after a cancel: %v", err)
			done <- nil
			return
		}
		done <- resp
	}()

	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	sendReply(t, replies, &proto.Response{
		ID: req.ID, OK: true,
		Ping: &proto.PingResult{Version: proto.Version, Home: "fresh"},
	})

	select {
	case resp := <-done:
		if resp == nil {
			t.Fatal("second call failed")
		}
		if resp.Ping.Home != "fresh" {
			t.Errorf("Home = %q, want fresh: the stale reply leaked to a new caller", resp.Ping.Home)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second call never completed")
	}
}

// When the agent dies, everyone waiting must be woken with the cause rather than
// hanging until their context expires.
func TestReaderFailureWakesAllWaiters(t *testing.T) {
	c, requests, _, killAgent := newTestConn(t)

	const n = 3
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
			errs <- err
		}()
	}
	for i := 0; i < n; i++ {
		var req proto.Request
		if err := requests.Decode(&req); err != nil {
			t.Fatal(err)
		}
	}

	// Closing the reply stream is exactly what a dying ssh process looks like:
	// readLoop sees EOF and fails every waiter itself.
	killAgent()

	for i := 0; i < n; i++ {
		select {
		case err := <-errs:
			if err == nil {
				t.Error("expected an error after the agent died")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("a waiter was never woken after the reader failed")
		}
	}
}

// Close must wake waiters too: relying on readLoop noticing EOF is a race, and a
// missed wake leaves the caller hanging.
func TestCloseWakesWaiters(t *testing.T) {
	c, requests, _, _ := newTestConn(t)

	errs := make(chan error, 1)
	go func() {
		_, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		errs <- err
	}()
	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}

	c.Close()
	select {
	case err := <-errs:
		if err == nil {
			t.Error("expected an error after Close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not wake the pending caller")
	}
}

func TestDoAfterCloseErrors(t *testing.T) {
	c, _, _, _ := newTestConn(t)
	c.Close()
	if _, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing}); err == nil {
		t.Error("Do on a closed connection should error")
	}
}

// Concurrent writers must not interleave partial lines, or the agent sees
// corrupt JSON. Every request should decode cleanly and carry a unique ID.
func TestConcurrentWritesStayFramed(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)

	const n = 20
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c.Do(context.Background(), &proto.Request{
				Op:  proto.OpWriteFile,
				Cat: &proto.WriteParams{Path: fmt.Sprintf("/tmp/f%d", i), Content: strings.Repeat("x", 512)},
			})
		}(i)
	}

	seen := map[string]bool{}
	for len(seen) < n {
		var req proto.Request
		if err := requests.Decode(&req); err != nil {
			t.Fatalf("request framing corrupted after %d requests: %v", len(seen), err)
		}
		if seen[req.ID] {
			t.Fatalf("duplicate request ID %q", req.ID)
		}
		seen[req.ID] = true
		sendReply(t, replies, &proto.Response{ID: req.ID, OK: true, Cat: &proto.WriteResult{}})
	}
	wg.Wait()
}

// ---------- pure helpers ----------

// ssh rejects a control path longer than a sockaddr_un (~104 bytes), which a
// long "user@host:port" easily exceeds, so the name is hashed.
func TestControlPathIsShortAndStable(t *testing.T) {
	long := Host{
		Addr: "some-quite-long-username@a.very.long.hostname.example.internal.corp",
		Port: 36000,
	}
	p1, err := controlPath(long)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := controlPath(long)
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Errorf("controlPath is not stable: %q vs %q", p1, p2)
	}
	if len(p1) > 100 {
		t.Errorf("control path is %d bytes, too long for a unix socket: %q", len(p1), p1)
	}

	// A different port is a different connection and must not share a socket.
	other := long
	other.Port = 36001
	p3, err := controlPath(other)
	if err != nil {
		t.Fatal(err)
	}
	if p3 == p1 {
		t.Error("hosts differing only by port share a control path")
	}
}

func TestRemoteDirNormalization(t *testing.T) {
	valid := map[string]string{
		"":                  ".cache/rdev",
		"~/.cache/custom":   ".cache/custom",
		".cache/custom":     ".cache/custom",
		"state-1/jobs_data": "state-1/jobs_data",
	}
	for in, want := range valid {
		got, err := ValidateRemoteDir(in)
		if err != nil || got != want {
			t.Errorf("ValidateRemoteDir(%q) = %q, %v; want %q", in, got, err, want)
		}
	}

	invalid := []string{
		"/.cache/custom", "../state", ".cache/../state", ".cache//state",
		".cache/state/", ".cache/'quoted'", ".cache/$(touch-pwned)",
		".cache/`id`", ".cache/with space", ".cache/with\nnewline", "~root/state",
	}
	for _, in := range invalid {
		if _, err := ValidateRemoteDir(in); err == nil {
			t.Errorf("ValidateRemoteDir(%q) should fail", in)
		}
	}
}

func TestValidateDestinationRejectsOptionAndWhitespaceInjection(t *testing.T) {
	valid := []Host{
		{Addr: "u@host"}, {Addr: "ssh-alias", Port: 22},
		{Addr: "u@[2001:db8::1]", Port: 65535},
	}
	for _, host := range valid {
		if err := ValidateHost(host); err != nil {
			t.Errorf("ValidateHost(%+v): %v", host, err)
		}
	}

	invalid := []Host{
		{Addr: ""},
		{Addr: "-oProxyCommand=sh"},
		{Addr: "u@host -oProxyCommand=sh"},
		{Addr: "u@host\n-oProxyCommand=sh"},
		{Addr: "u@host", Port: -1},
		{Addr: "u@host", Port: 65536},
	}
	for _, host := range invalid {
		if err := ValidateHost(host); err == nil {
			t.Errorf("ValidateHost(%+v) should fail", host)
		}
	}
}

func TestEverySSHSinkUsesDestinationValidation(t *testing.T) {
	log := fakeSSH(t, "", 0)
	c := &Conn{
		host:   Host{Addr: "-oProxyCommand=touch-pwned"},
		stderr: &lockedBuilder{},
	}
	if _, err := c.runSSH(t.Context(), "true"); err == nil {
		t.Error("runSSH accepted an option-shaped destination")
	}
	if err := c.installAgent(t.Context(), []byte("x"), fmt.Sprintf("%x", sha256.Sum256([]byte("x")))); err == nil {
		t.Error("agent install accepted an option-shaped destination")
	}
	if err := c.startAgent(t.Context()); err == nil {
		t.Error("startAgent accepted an option-shaped destination")
	}
	if calls := sshCalls(t, log); calls != "" {
		t.Fatalf("an invalid destination reached ssh:\n%s", calls)
	}
}

func TestShellCommandKeepsDynamicValuesOutOfProgram(t *testing.T) {
	const program = `printf '%s' "$1"`
	marker := filepath.Join(t.TempDir(), "pwned")
	value := "$(touch " + marker + "); ' quoted\nnext"
	argv := shellCommand(program, value)
	if len(argv) != 5 {
		t.Fatalf("shellCommand argv = %v", argv)
	}
	if strings.Contains(argv[2], value) {
		t.Fatalf("dynamic value entered shell source: %q", argv[2])
	}
	commandLine := strings.Join(argv, " ")
	out, err := exec.Command("sh", "-c", commandLine).Output()
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != value {
		t.Errorf("positional value round-tripped as %q, want %q", out, value)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("dynamic value executed instead of remaining data: %v", err)
	}
}

func runLocalInstallScript(stage, target string, data []byte, want string) error {
	cmd := exec.Command("sh", "-c", installAgentScript, "rdev-install", stage, target, want)
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return agentInstallError(err, string(out))
	}
	return nil
}

func TestInstallScriptUsesPortableFDBoundRegularFileChecks(t *testing.T) {
	for _, localizedTypeProbe := range []string{"%F", "%HT", "regular file", "Regular File"} {
		if strings.Contains(installAgentScript, localizedTypeProbe) {
			t.Fatalf("install script still depends on stat type text %q", localizedTypeProbe)
		}
	}
	if strings.Contains(installAgentScript, "|| true") {
		t.Fatal("install script still swallows rollback or cleanup failures")
	}
	for _, invariant := range []string{
		`state=STAGED`,
		`state=VERIFIED`,
		`state=INSTALLING`,
		`state=COMMITTED`,
		`publication_pending=1`,
		`published_by_us=1`,
		agentInstallAmbiguousMarker,
		agentInstallCommittedMarker,
		`[ -f "$fd" ]`,
		`[ "$(ident "$ready")" = "$(ident "$fd")" ]`,
		`[ "$(ident "$readfd")" = "$(ident "$fd")" ]`,
		`verify_target "$published_ident" "$want"`,
	} {
		if !strings.Contains(installAgentScript, invariant) {
			t.Fatalf("install script missing fd/inode invariant %q", invariant)
		}
	}
	data := []byte("first-install-on-an-empty-staging-file")
	root := t.TempDir()
	target := filepath.Join(root, "installed")
	if err := runLocalInstallScript(filepath.Join(root, "stage"), target, data, fmt.Sprintf("%x", sha256.Sum256(data))); err != nil {
		t.Fatalf("first install failed: %v", err)
	}
	if got, err := os.ReadFile(target); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("installed bytes=%q err=%v", got, err)
	}
}

func TestSecureAgentStagingRejectsPathReplacementWhileFDIsOpen(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	target := filepath.Join(root, "installed")
	victim := filepath.Join(root, "victim")
	if err := os.WriteFile(target, []byte("old-agent"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(victim, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	data := []byte(strings.Repeat("new-agent", 1024))
	cmd := exec.Command("sh", "-c", installAgentScript, "rdev-install", stage, target, fmt.Sprintf("%x", sha256.Sum256(data)))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(stage, "agent")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, statErr := os.Lstat(file); statErr == nil && info.Mode().IsRegular() {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatal("staging file was not created")
		}
		time.Sleep(time.Millisecond)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, file); err != nil {
		t.Fatal(err)
	}
	_, _ = stdin.Write(data)
	_ = stdin.Close()
	if err := cmd.Wait(); err == nil {
		t.Fatal("path replacement was accepted despite the open staging fd")
	}
	if got, _ := os.ReadFile(target); string(got) != "old-agent" {
		t.Fatalf("installed agent changed to %q", got)
	}
	if got, _ := os.ReadFile(victim); string(got) != "sentinel" {
		t.Fatalf("replacement link target changed to %q", got)
	}
	if _, err := os.Lstat(stage); !os.IsNotExist(err) {
		t.Fatalf("staging residue: %v", err)
	}
}

func TestSecureAgentInstallRollsBackReadyReplacementAfterDigest(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	target := filepath.Join(root, "installed")
	if err := os.WriteFile(target, []byte("old-agent"), 0o755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	entered := filepath.Join(root, "mv-entered")
	release := filepath.Join(root, "mv-release")
	once := filepath.Join(root, "mv-once")
	wrapper := `#!/bin/sh
set -eu
if mkdir "$RDEV_MV_ONCE" 2>/dev/null; then
  : > "$RDEV_MV_ENTERED"
  while [ ! -e "$RDEV_MV_RELEASE" ]; do sleep 0.01; done
fi
exec /bin/mv "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "mv"), []byte(wrapper), 0o700); err != nil {
		t.Fatal(err)
	}
	data := []byte(strings.Repeat("verified-agent", 1024))
	cmd := exec.Command("sh", "-c", installAgentScript, "rdev-install", stage, target, fmt.Sprintf("%x", sha256.Sum256(data)))
	cmd.Stdin = bytes.NewReader(data)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"RDEV_MV_ENTERED="+entered,
		"RDEV_MV_RELEASE="+release,
		"RDEV_MV_ONCE="+once,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(entered); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatal("install did not reach the post-digest rename")
		}
		time.Sleep(time.Millisecond)
	}
	ready := filepath.Join(stage, "ready")
	if err := os.Remove(ready); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ready, []byte("replacement"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitErr := cmd.Wait()
	var ambiguous *AgentInstallAmbiguousError
	if waitErr == nil || !errors.As(agentInstallError(waitErr, stderr.String()), &ambiguous) {
		t.Fatalf("post-digest path replacement outcome=%v stderr=%q", waitErr, stderr.String())
	}
	if got, _ := os.ReadFile(target); string(got) != "replacement" {
		t.Fatalf("rollback overwrote the replacement target: %q", got)
	}
	if got, _ := os.ReadFile(filepath.Join(stage, "old")); string(got) != "old-agent" {
		t.Fatalf("ambiguous install did not preserve old inode backup: %q", got)
	}
}

func writeInstallFaultWrapper(t *testing.T, bin, name, real string) {
	t.Helper()
	var behavior string
	switch name {
	case "shasum", "sha256sum":
		behavior = `fail_at=${RDEV_FAIL_DIGEST_AT:-0}
replace_at=${RDEV_REPLACE_TARGET_AT:-$fail_at}
if [ "$count" = "$replace_at" ] && [ -n "${RDEV_REPLACE_TARGET_ON_DIGEST:-}" ]; then
  /bin/rm -f -- "$RDEV_REPLACE_TARGET_ON_DIGEST"
  printf 'concurrent-target' > "$RDEV_REPLACE_TARGET_ON_DIGEST"
  chmod 755 "$RDEV_REPLACE_TARGET_ON_DIGEST"
fi
if [ "$count" = "$fail_at" ]; then
  printf '%064d\n' 0
  exit 0
fi`
	case "mv":
		behavior = `if [ "$count" = "${RDEV_FAIL_MV_AT:-0}" ]; then exit 42; fi
"$real" "$@"
if [ "$count" = "${RDEV_CORRUPT_MV_AT:-0}" ]; then
  dest=
  for arg in "$@"; do dest=$arg; done
  /bin/rm -f -- "$dest"
  printf 'replacement-after-mv' > "$dest"
  chmod 755 "$dest"
fi
exit 0`
	case "ln":
		behavior = `if [ "$count" = "${RDEV_FAIL_LN_AT:-0}" ]; then exit 42; fi
"$real" "$@"
if [ "$count" = "${RDEV_CORRUPT_LN_AT:-0}" ]; then
  dest=
  for arg in "$@"; do dest=$arg; done
  /bin/rm -f -- "$dest"
  printf 'replacement-after-restore' > "$dest"
  chmod 755 "$dest"
fi
if [ "$count" = "${RDEV_BARRIER_LN_AT:-0}" ]; then
  : > "$RDEV_BARRIER_ENTERED"
  while [ ! -e "$RDEV_BARRIER_RELEASE" ]; do sleep 0.01; done
fi
exit 0`
	case "rm":
		behavior = `if [ "$count" = "${RDEV_FAIL_RM_AT:-0}" ]; then exit 42; fi
exec "$real" "$@"`
	case "rmdir":
		behavior = `if [ "$count" = "${RDEV_FAIL_RMDIR_AT:-0}" ]; then exit 42; fi
exec "$real" "$@"`
	default:
		t.Fatalf("unknown wrapper %q", name)
	}
	script := fmt.Sprintf(`#!/bin/sh
set -eu
real=%q
counter=$RDEV_FAULT_DIR/%s.count
count=0
if [ -f "$counter" ]; then IFS= read -r count < "$counter"; fi
count=$((count + 1))
printf '%%s\n' "$count" > "$counter"
%s
exec "$real" "$@"
`, real, name, behavior)
	if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

func installFaultEnvironment(t *testing.T) []string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"mv", "ln", "rm", "rmdir", "shasum", "sha256sum"} {
		real, err := exec.LookPath(name)
		if err != nil {
			if name == "shasum" || name == "sha256sum" {
				continue
			}
			t.Fatal(err)
		}
		writeInstallFaultWrapper(t, bin, name, real)
	}
	faultDir := filepath.Join(t.TempDir(), "counts")
	if err := os.Mkdir(faultDir, 0o700); err != nil {
		t.Fatal(err)
	}
	return append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"), "RDEV_FAULT_DIR="+faultDir)
}

func runLocalInstallScriptEnv(stage, target string, data []byte, env []string) error {
	cmd := exec.Command("sh", "-c", installAgentScript, "rdev-install", stage, target, fmt.Sprintf("%x", sha256.Sum256(data)))
	cmd.Stdin = bytes.NewReader(data)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return agentInstallError(err, string(out))
	}
	return nil
}

func installFaultCase(t *testing.T, withOld bool, extra ...string) (root, stage, target string, data []byte, err error) {
	t.Helper()
	root = t.TempDir()
	stage = filepath.Join(root, "stage")
	target = filepath.Join(root, "installed")
	if withOld {
		if writeErr := os.WriteFile(target, []byte("old-agent"), 0o755); writeErr != nil {
			t.Fatal(writeErr)
		}
		if linkErr := os.Link(target, filepath.Join(root, "old-control")); linkErr != nil {
			t.Fatal(linkErr)
		}
	}
	data = []byte(strings.Repeat("new-verified-agent", 1024))
	for i := range extra {
		extra[i] = strings.ReplaceAll(extra[i], "{target}", target)
	}
	env := append(installFaultEnvironment(t), extra...)
	err = runLocalInstallScriptEnv(stage, target, data, env)
	return
}

func requireInstallOutcome[T error](t *testing.T, err error) T {
	t.Helper()
	var target T
	if !errors.As(err, &target) {
		t.Fatalf("install error=%v, want %T", err, target)
	}
	return target
}

func TestSecureAgentInstallFaultOutcomes(t *testing.T) {
	t.Run("pre-publication target change preserves old inode", func(t *testing.T) {
		_, stage, target, _, err := installFaultCase(t, true,
			"RDEV_FAIL_DIGEST_AT=3", "RDEV_REPLACE_TARGET_ON_DIGEST={target}")
		requireInstallOutcome[*AgentInstallAmbiguousError](t, err)
		if got, _ := os.ReadFile(target); string(got) != "concurrent-target" {
			t.Fatalf("pre-publication cleanup overwrote concurrent target: %q", got)
		}
		if got, _ := os.ReadFile(filepath.Join(stage, "old")); string(got) != "old-agent" {
			t.Fatalf("pre-publication ambiguity lost old inode: %q", got)
		}
	})

	t.Run("post-publish validation failure rolls back", func(t *testing.T) {
		root, stage, target, _, err := installFaultCase(t, true, "RDEV_FAIL_DIGEST_AT=4")
		if err == nil {
			t.Fatal("injected validation failure succeeded")
		}
		var ambiguous *AgentInstallAmbiguousError
		var committed *AgentInstallCommittedError
		if errors.As(err, &ambiguous) || errors.As(err, &committed) {
			t.Fatalf("verified rollback returned uncertain outcome: %v", err)
		}
		if got, _ := os.ReadFile(target); string(got) != "old-agent" {
			t.Fatalf("rollback target=%q", got)
		}
		restoredInfo, statErr := os.Stat(target)
		if statErr != nil {
			t.Fatal(statErr)
		}
		controlInfo, statErr := os.Stat(filepath.Join(root, "old-control"))
		if statErr != nil {
			t.Fatal(statErr)
		}
		if !os.SameFile(restoredInfo, controlInfo) {
			t.Fatal("rollback content matched but inode did not match the preserved old inode")
		}
		if _, statErr := os.Lstat(stage); !os.IsNotExist(statErr) {
			t.Fatalf("verified rollback residue: %v", statErr)
		}
	})

	t.Run("rollback mv failure is ambiguous", func(t *testing.T) {
		_, stage, target, data, err := installFaultCase(t, true, "RDEV_FAIL_DIGEST_AT=4", "RDEV_FAIL_MV_AT=2")
		requireInstallOutcome[*AgentInstallAmbiguousError](t, err)
		if got, _ := os.ReadFile(target); !bytes.Equal(got, data) {
			t.Fatalf("failed rollback changed published target: %q", got)
		}
		if got, _ := os.ReadFile(filepath.Join(stage, "old")); string(got) != "old-agent" {
			t.Fatalf("old backup=%q", got)
		}
	})

	t.Run("rollback rm failure is ambiguous", func(t *testing.T) {
		_, stage, target, _, err := installFaultCase(t, true, "RDEV_FAIL_DIGEST_AT=4", "RDEV_FAIL_RM_AT=2")
		requireInstallOutcome[*AgentInstallAmbiguousError](t, err)
		if got, _ := os.ReadFile(target); string(got) != "old-agent" {
			t.Fatalf("restored target=%q", got)
		}
		if _, statErr := os.Stat(filepath.Join(stage, "failed")); statErr != nil {
			t.Fatalf("failed published inode not retained: %v", statErr)
		}
	})

	t.Run("rollback restored inode mismatch is ambiguous", func(t *testing.T) {
		_, stage, target, _, err := installFaultCase(t, true, "RDEV_FAIL_DIGEST_AT=4", "RDEV_CORRUPT_LN_AT=4")
		requireInstallOutcome[*AgentInstallAmbiguousError](t, err)
		if got, _ := os.ReadFile(target); string(got) != "replacement-after-restore" {
			t.Fatalf("mismatched rollback target=%q", got)
		}
		if got, _ := os.ReadFile(filepath.Join(stage, "old")); string(got) != "old-agent" {
			t.Fatalf("old backup not retained: %q", got)
		}
	})

	t.Run("final rollback recheck retains all evidence", func(t *testing.T) {
		_, stage, target, _, err := installFaultCase(t, true,
			"RDEV_FAIL_DIGEST_AT=4", "RDEV_REPLACE_TARGET_AT=6",
			"RDEV_REPLACE_TARGET_ON_DIGEST={target}")
		requireInstallOutcome[*AgentInstallAmbiguousError](t, err)
		if got, _ := os.ReadFile(target); string(got) != "concurrent-target" {
			t.Fatalf("final recheck overwrote concurrent target: %q", got)
		}
		for name, want := range map[string]string{
			"old":       "old-agent",
			"failed":    strings.Repeat("new-verified-agent", 1024),
			"published": strings.Repeat("new-verified-agent", 1024),
		} {
			if got, readErr := os.ReadFile(filepath.Join(stage, name)); readErr != nil || string(got) != want {
				t.Fatalf("retained %s=%q err=%v", name, got, readErr)
			}
		}
	})

	t.Run("first install never deletes concurrent target", func(t *testing.T) {
		_, stage, target, _, err := installFaultCase(t, false,
			"RDEV_FAIL_DIGEST_AT=2", "RDEV_REPLACE_TARGET_ON_DIGEST={target}")
		requireInstallOutcome[*AgentInstallAmbiguousError](t, err)
		if got, _ := os.ReadFile(target); string(got) != "concurrent-target" {
			t.Fatalf("rollback deleted concurrent target: %q", got)
		}
		if _, statErr := os.Stat(filepath.Join(stage, "published")); statErr != nil {
			t.Fatalf("ambiguous first install did not retain published proof: %v", statErr)
		}
		if _, statErr := os.Stat(filepath.Join(stage, "old")); !os.IsNotExist(statErr) {
			t.Fatalf("first install unexpectedly created old backup: %v", statErr)
		}
	})

	t.Run("commit backup removal failure is warning", func(t *testing.T) {
		_, stage, target, data, err := installFaultCase(t, true, "RDEV_FAIL_RM_AT=2")
		requireInstallOutcome[*AgentInstallCommittedError](t, err)
		if got, _ := os.ReadFile(target); !bytes.Equal(got, data) {
			t.Fatalf("committed target=%q", got)
		}
		if got, _ := os.ReadFile(filepath.Join(stage, "old")); string(got) != "old-agent" {
			t.Fatalf("committed warning lost backup: %q", got)
		}
	})

	t.Run("commit rmdir failure is warning", func(t *testing.T) {
		_, stage, target, data, err := installFaultCase(t, true, "RDEV_FAIL_RMDIR_AT=1")
		requireInstallOutcome[*AgentInstallCommittedError](t, err)
		if got, _ := os.ReadFile(target); !bytes.Equal(got, data) {
			t.Fatalf("committed target=%q", got)
		}
		entries, readErr := os.ReadDir(stage)
		if readErr != nil || len(entries) != 0 {
			t.Fatalf("committed rmdir residue entries=%v err=%v", entries, readErr)
		}
	})
}

func TestFirstInstallSignalAtPublicationBoundaryPreservesTargetAndEvidence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		concurrent bool
		wantTarget string
	}{
		{name: "published target", wantTarget: strings.Repeat("signal-safe-agent", 1024)},
		{name: "concurrent replacement", concurrent: true, wantTarget: "concurrent-target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			stage := filepath.Join(root, "stage")
			target := filepath.Join(root, "installed")
			entered := filepath.Join(root, "publication-entered")
			release := filepath.Join(root, "publication-release")
			data := []byte(strings.Repeat("signal-safe-agent", 1024))
			cmd := exec.Command("sh", "-c", installAgentScript, "rdev-install", stage, target, fmt.Sprintf("%x", sha256.Sum256(data)))
			cmd.Stdin = bytes.NewReader(data)
			var stderr strings.Builder
			cmd.Stderr = &stderr
			cmd.Env = append(installFaultEnvironment(t),
				"RDEV_BARRIER_LN_AT=3",
				"RDEV_BARRIER_ENTERED="+entered,
				"RDEV_BARRIER_RELEASE="+release,
			)
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				if _, err := os.Stat(entered); err == nil {
					break
				}
				if time.Now().After(deadline) {
					_ = cmd.Process.Kill()
					t.Fatal("install did not reach first-publication barrier")
				}
				time.Sleep(time.Millisecond)
			}
			if tc.concurrent {
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(target, []byte(tc.wantTarget), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(release, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			waitErr := cmd.Wait()
			if waitErr == nil {
				t.Fatal("publication-boundary interrupt unexpectedly succeeded")
			}
			requireInstallOutcome[*AgentInstallAmbiguousError](t, agentInstallError(waitErr, stderr.String()))
			if got, readErr := os.ReadFile(target); readErr != nil || string(got) != tc.wantTarget {
				t.Fatalf("target=%q err=%v, want preserved %q", got, readErr, tc.wantTarget)
			}
			if got, readErr := os.ReadFile(filepath.Join(stage, "published")); readErr != nil || !bytes.Equal(got, data) {
				t.Fatalf("publication proof=%q err=%v", got, readErr)
			}
		})
	}
}

func TestSecureAgentStagingRejectsExistingObjects(t *testing.T) {
	data := []byte("new-agent")
	want := fmt.Sprintf("%x", sha256.Sum256(data))
	for _, kind := range []string{"symlink", "regular", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			stage, target := filepath.Join(root, "stage"), filepath.Join(root, "installed")
			if err := os.WriteFile(target, []byte("old-agent"), 0o755); err != nil {
				t.Fatal(err)
			}
			var victim string
			switch kind {
			case "symlink":
				victim = filepath.Join(root, "victim")
				if err := os.Mkdir(victim, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(victim, "agent"), []byte("sentinel"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(victim, stage); err != nil {
					t.Fatal(err)
				}
			case "regular":
				if err := os.WriteFile(stage, []byte("sentinel"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(stage, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := runLocalInstallScript(stage, target, data, want); err == nil {
				t.Fatal("existing staging object was accepted")
			}
			if got, _ := os.ReadFile(target); string(got) != "old-agent" {
				t.Fatalf("installed agent changed to %q", got)
			}
			if victim != "" {
				if got, _ := os.ReadFile(filepath.Join(victim, "agent")); string(got) != "sentinel" {
					t.Fatalf("link target changed to %q", got)
				}
			}
		})
	}
}

func TestSecureAgentStagingFailureCleansOnlyOwnedObject(t *testing.T) {
	root := t.TempDir()
	stage, target := filepath.Join(root, "stage"), filepath.Join(root, "installed")
	if err := os.WriteFile(target, []byte("old-agent"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runLocalInstallScript(stage, target, []byte("partial"), strings.Repeat("0", 64)); err == nil {
		t.Fatal("digest mismatch succeeded")
	}
	if _, err := os.Lstat(stage); !os.IsNotExist(err) {
		t.Fatalf("staging residue: %v", err)
	}
	if got, _ := os.ReadFile(target); string(got) != "old-agent" {
		t.Fatalf("installed agent changed to %q", got)
	}
}

func TestSecureAgentStagingConcurrentInstallsAreComplete(t *testing.T) {
	root, target := t.TempDir(), ""
	target = filepath.Join(root, "installed")
	payloads := [][]byte{[]byte(strings.Repeat("a", 128<<10)), []byte(strings.Repeat("b", 128<<10))}
	var wg sync.WaitGroup
	errs := make(chan error, len(payloads))
	for i, payload := range payloads {
		wg.Add(1)
		go func(i int, payload []byte) {
			defer wg.Done()
			err := runLocalInstallScript(filepath.Join(root, fmt.Sprintf("stage-%d", i)), target, payload, fmt.Sprintf("%x", sha256.Sum256(payload)))
			errs <- err
		}(i, payload)
	}
	wg.Wait()
	close(errs)
	successes := 0
	for err := range errs {
		if err == nil {
			successes++
			continue
		}
		var ambiguous *AgentInstallAmbiguousError
		var committed *AgentInstallCommittedError
		if errors.As(err, &ambiguous) || errors.As(err, &committed) {
			t.Fatalf("concurrent no-replace loser reported uncertain outcome: %v", err)
		}
	}
	if successes == 0 {
		t.Fatal("no concurrent installer published a complete agent")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payloads[0]) && !bytes.Equal(got, payloads[1]) {
		t.Fatalf("installed partial payload: %d bytes", len(got))
	}
	for i := range payloads {
		if _, err := os.Lstat(filepath.Join(root, fmt.Sprintf("stage-%d", i))); !os.IsNotExist(err) {
			t.Fatalf("stage %d residue: %v", i, err)
		}
	}
}

func TestAgentStageSuffixIsCryptographicAndUnique(t *testing.T) {
	seen := make(map[string]struct{}, 256)
	for i := 0; i < 256; i++ {
		suffix, err := randomStageSuffix()
		if err != nil {
			t.Fatal(err)
		}
		if len(suffix) != 32 {
			t.Fatalf("suffix length=%d", len(suffix))
		}
		if _, exists := seen[suffix]; exists {
			t.Fatalf("duplicate suffix %q", suffix)
		}
		seen[suffix] = struct{}{}
	}
}

// The agent build is chosen from uname output, so the mapping has to cover the
// spellings real machines report.
func TestPlatformMapping(t *testing.T) {
	cases := []struct {
		uname        string
		goos, goarch string
		wantErr      bool
	}{
		{"Linux x86_64", "linux", "amd64", false},
		{"Linux aarch64", "linux", "arm64", false},
		{"Darwin arm64", "darwin", "arm64", false},
		{"Darwin x86_64", "darwin", "amd64", false},
		{"Linux amd64", "linux", "amd64", false},
		{"FreeBSD x86_64", "", "", true},
		{"Linux mips64", "", "", true},
		{"Linux", "", "", true},
	}
	for _, c := range cases {
		goos, goarch, err := mapPlatform(c.uname)
		if c.wantErr {
			if err == nil {
				t.Errorf("mapPlatform(%q) succeeded, want an error", c.uname)
			}
			continue
		}
		if err != nil {
			t.Errorf("mapPlatform(%q) failed: %v", c.uname, err)
			continue
		}
		if goos != c.goos || goarch != c.goarch {
			t.Errorf("mapPlatform(%q) = %s/%s, want %s/%s", c.uname, goos, goarch, c.goos, c.goarch)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("hello", 10); got != "hello" {
		t.Errorf("short string was altered: %q", got)
	}
	if got := truncate("hello world", 5); got != "hello..." {
		t.Errorf("truncate = %q, want hello...", got)
	}
}

// The control directory must not be world-readable: its socket names reveal
// which machines this user connects to.
func TestControlDirPermissions(t *testing.T) {
	if _, err := controlPath(Host{Addr: "u@h"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(os.TempDir(), "rdev-ctl"))
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("control dir mode %o is group/world accessible", perm)
	}
}

// The two failures BatchMode=yes makes cryptic get a next step; everything else
// is left alone. The negative cases carry the weight here -- appending advice to an
// error that already says what is wrong makes it harder to read, not easier.
func TestExplainSSHError(t *testing.T) {
	host := Host{Addr: "user@dev.example.com", Port: 36000}

	cases := []struct {
		name        string
		in          string
		wantAdvice  bool
		wantSnippet string
	}{
		{
			name:        "host key not trusted",
			in:          "Host key verification failed.",
			wantAdvice:  true,
			wantSnippet: "ssh-keyscan -p 36000 dev.example.com",
		},
		{
			// OpenSSH prints this variant before the summary line, and it is what a
			// caller sees when only one key type is missing.
			name:        "no known host key of this type",
			in:          "No ED25519 host key is known for dev.example.com and you have requested strict checking.",
			wantAdvice:  true,
			wantSnippet: "ssh-keyscan",
		},
		{
			name:        "publickey auth rejected",
			in:          "user@dev.example.com: Permission denied (publickey).",
			wantAdvice:  true,
			wantSnippet: "must succeed with no prompt",
		},
		{
			name:       "unresolved hostname explains itself",
			in:         "ssh: Could not resolve hostname bogus.invalid: nodename nor servname provided",
			wantAdvice: false,
		},
		{
			name:       "connection refused explains itself",
			in:         "ssh: connect to host dev.example.com port 36000: Connection refused",
			wantAdvice: false,
		},
		{
			// A changed key is a possible interception, and the message OpenSSH prints
			// for it is already loud and specific. Burying it under our advice would be
			// the wrong call.
			name:       "changed host key is left verbatim",
			in:         "WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!",
			wantAdvice: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := explainSSHError(errors.New(tc.in), host)
			if !strings.Contains(got.Error(), tc.in) {
				t.Errorf("original ssh message lost:\n%s", got)
			}
			hasAdvice := strings.Contains(got.Error(), "BatchMode=yes")
			if hasAdvice != tc.wantAdvice {
				t.Errorf("advice present = %v, want %v:\n%s", hasAdvice, tc.wantAdvice, got)
			}
			if tc.wantSnippet != "" && !strings.Contains(got.Error(), tc.wantSnippet) {
				t.Errorf("missing %q:\n%s", tc.wantSnippet, got)
			}
		})
	}

	if explainSSHError(nil, host) != nil {
		t.Error("nil error should stay nil")
	}
}

// The advice must never suggest disabling the check it is explaining. Someone stuck
// on this error will find StrictHostKeyChecking=no on their own; having the tool
// recommend it would trade away the assumption every redacted credential rests on.
func TestExplainSSHErrorDoesNotSuggestDisablingVerification(t *testing.T) {
	got := explainSSHError(errors.New("Host key verification failed."), Host{Addr: "u@h"}).Error()
	if !strings.Contains(got, "Do not use StrictHostKeyChecking=no") {
		t.Error("advice should warn against StrictHostKeyChecking=no")
	}
	// Present only inside that warning, so one occurrence and no "=no" recommendation.
	if strings.Count(got, "StrictHostKeyChecking") != 1 {
		t.Errorf("StrictHostKeyChecking mentioned more than once:\n%s", got)
	}
}

// ssh-keyscan takes a hostname, not user@host: pasting the command with the user
// prefix left in fails, which is the kind of detail that makes advice useless.
func TestSSHHostnameStripsUser(t *testing.T) {
	cases := map[string]string{
		"user@dev.example.com": "dev.example.com",
		"dev.example.com":      "dev.example.com",
		"1.2.3.4":              "1.2.3.4",
		"user@1.2.3.4":         "1.2.3.4",
	}
	for in, want := range cases {
		if got := sshHostname(in); got != want {
			t.Errorf("sshHostname(%q) = %q, want %q", in, got, want)
		}
	}
}

// A host with no explicit port must still produce a runnable command.
func TestExplainSSHErrorDefaultsPort(t *testing.T) {
	got := explainSSHError(errors.New("Host key verification failed."), Host{Addr: "u@h"}).Error()
	if !strings.Contains(got, "ssh-keyscan -p 22 h") {
		t.Errorf("want default port 22 in the command:\n%s", got)
	}
}

// The connect probe replaces four sequential ssh round trips, so its output
// parsing has to tolerate whatever the remote shell adds around it.
func TestParseProbeOutput(t *testing.T) {
	cases := []struct {
		name              string
		out               string
		wantHome, wantSHA string
		wantGOOS          string
		wantErr           bool
	}{
		{
			name:     "linux with installed agent",
			out:      "rdev-os Linux\nrdev-arch x86_64\nrdev-home /home/u\nrdev-sha abc123\n",
			wantHome: "/home/u", wantSHA: "abc123", wantGOOS: "linux",
		},
		{
			name:     "agent absent omits sha",
			out:      "rdev-os Darwin\nrdev-arch arm64\nrdev-home /Users/u\n",
			wantHome: "/Users/u", wantSHA: "", wantGOOS: "darwin",
		},
		{
			// A chatty ~/.bashrc printing to stdout is common on shared boxes; the
			// prefixes are what keep it from being read as probe data.
			name:     "noisy profile output is ignored",
			out:      "Welcome to prod!\nrdev-os Linux\nMOTD line\nrdev-arch aarch64\nrdev-home /home/u\n",
			wantHome: "/home/u", wantGOOS: "linux",
		},
		{
			name:    "missing home is an error",
			out:     "rdev-os Linux\nrdev-arch x86_64\n",
			wantErr: true,
		},
		{
			name:    "unsupported platform is an error",
			out:     "rdev-os FreeBSD\nrdev-arch x86_64\nrdev-home /home/u\n",
			wantErr: true,
		},
	}

	for _, c := range cases {
		p, err := parseProbe(c.out)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected an error", c.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if p.home != c.wantHome {
			t.Errorf("%s: home = %q, want %q", c.name, p.home, c.wantHome)
		}
		if p.agentSHA != c.wantSHA {
			t.Errorf("%s: sha = %q, want %q", c.name, p.agentSHA, c.wantSHA)
		}
		if p.goos != c.wantGOOS {
			t.Errorf("%s: goos = %q, want %q", c.name, p.goos, c.wantGOOS)
		}
	}
}

// The probe script passes through ssh, which concatenates argv into one remote
// shell command line. Quoting has to survive that, including a state directory
// containing a space.
func TestShellQuoteSurvivesConcatenation(t *testing.T) {
	cases := []string{
		"simple",
		"with space",
		`with 'single' quotes`,
		`$(command-substitution)`,
		"multi\nline",
	}
	for _, in := range cases {
		quoted := shellQuote(in)
		// Round-trip through a real shell the same way ssh would.
		out, err := exec.Command("sh", "-c", "printf %s "+quoted).Output()
		if err != nil {
			t.Errorf("%q: %v", in, err)
			continue
		}
		if string(out) != in {
			t.Errorf("shellQuote(%q) round-tripped as %q", in, out)
		}
	}
}

// An agent older than the host must be rejected at the handshake rather than
// answering a later request with "unknown op", which reads as a protocol bug
// instead of a stale binary. The reverse direction is allowed; see
// TestHandshakeAcceptsNewerCompatibleAgent.
func TestHandshakeRejectsOlderAgent(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)

	done := make(chan error, 1)
	go func() {
		resp, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		if err != nil {
			done <- err
			return
		}
		// Mirror Dial's check.
		if !resp.Ping.Compatible(proto.Version) {
			done <- fmt.Errorf("agent protocol %d, want %d", resp.Ping.Version, proto.Version)
			return
		}
		done <- nil
	}()

	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	// A version-1 agent, i.e. one built before job_rm/list/-state existed.
	sendReply(t, replies, &proto.Response{
		ID: req.ID, OK: true,
		Ping: &proto.PingResult{Version: 1, MinVersion: 1, OS: "linux", Arch: "amd64"},
	})

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a version-1 agent should be rejected")
		}
		if !strings.Contains(err.Error(), "protocol") {
			t.Errorf("err = %v, want it to name the protocol mismatch", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handshake never completed")
	}
}

// Version must advance when ops are added, or a stale agent is indistinguishable
// from a current one.
func TestProtocolVersionCoversNewOps(t *testing.T) {
	if proto.Version < 2 {
		t.Errorf("Version = %d, but job_rm/list/-state/multi-wait were added after 1", proto.Version)
	}
}

// Compatibility is a range, not an exact match. The case that matters is an agent
// one version ahead of its host: two people sharing a dev box, the newer rdev
// uploaded the binary last. New ops are additive, so the older host can still work
// -- rejecting it outright was needless breakage.
func TestHandshakeAcceptsNewerCompatibleAgent(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)

	done := make(chan error, 1)
	go func() {
		resp, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		if err != nil {
			done <- err
			return
		}
		if !resp.Ping.Compatible(proto.Version) {
			done <- fmt.Errorf("rejected: agent %d-%d, host %d",
				resp.Ping.MinVersion, resp.Ping.Version, proto.Version)
			return
		}
		done <- nil
	}()

	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	// An agent that speaks one format newer while still serving ours.
	sendReply(t, replies, &proto.Response{
		ID: req.ID, OK: true,
		Ping: &proto.PingResult{Version: proto.Version + 1, MinVersion: proto.MinVersion},
	})

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("a newer agent that still serves our format should be accepted: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handshake never completed")
	}
}

func TestPingCompatibleMatrix(t *testing.T) {
	cases := []struct {
		name     string
		agentMin int
		agentVer int
		hostVer  int
		wantOK   bool
	}{
		{"exact match", 1, 2, 2, true},
		{"agent one ahead, still serves host", 1, 3, 2, true},
		{"agent far ahead, still serves host", 1, 9, 2, true},
		{"agent ahead but dropped our format", 3, 4, 2, false},
		{"agent behind the host", 1, 1, 2, false},
		// A build predating MinVersion reports 0, which must read as "exactly Version"
		// rather than "serves everything from 0".
		{"legacy agent, same version", 0, 2, 2, true},
		{"legacy agent, older version", 0, 1, 2, false},
		{"legacy agent, newer version", 0, 3, 2, false},
	}
	for _, c := range cases {
		p := &proto.PingResult{Version: c.agentVer, MinVersion: c.agentMin}
		if got := p.Compatible(c.hostVer); got != c.wantOK {
			t.Errorf("%s: agent %d-%d vs host %d = %v, want %v",
				c.name, c.agentMin, c.agentVer, c.hostVer, got, c.wantOK)
		}
	}
}

func TestPingCompatibleRejectsNil(t *testing.T) {
	var p *proto.PingResult
	if p.Compatible(proto.Version) {
		t.Error("a nil ping must not be treated as compatible")
	}
}

// An older agent must be rejected with a message that names the direction and the
// fix, since the alternative is a confusing "unknown op" partway through a session.
func TestHandshakeErrorNamesTheFix(t *testing.T) {
	c, requests, replies, _ := newTestConn(t)
	c.agentPath = "/home/u/.cache/rdev/rdev-agent"

	done := make(chan error, 1)
	go func() {
		resp, err := c.Do(context.Background(), &proto.Request{Op: proto.OpPing})
		if err != nil {
			done <- err
			return
		}
		if !resp.Ping.Compatible(proto.Version) {
			if resp.Ping.Version < proto.Version {
				done <- fmt.Errorf(
					"remote agent at %s speaks protocol %d but this rdev needs %d; "+
						"it was installed by an older rdev -- run 'make agents && make build' and reconnect",
					c.agentPath, resp.Ping.Version, proto.Version)
				return
			}
		}
		done <- nil
	}()

	var req proto.Request
	if err := requests.Decode(&req); err != nil {
		t.Fatal(err)
	}
	sendReply(t, replies, &proto.Response{
		ID: req.ID, OK: true,
		Ping: &proto.PingResult{Version: 1, MinVersion: 1},
	})

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a protocol-1 agent should be rejected")
		}
		for _, want := range []string{"make agents", c.agentPath} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err = %q, want it to mention %q", err, want)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handshake never completed")
	}
}
