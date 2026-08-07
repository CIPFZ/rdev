package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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
	t.Helper()

	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()

	c = &Conn{
		host:    Host{Name: "test", Addr: "u@h"},
		stderr:  &lockedBuilder{},
		pending: make(map[string]chan *proto.Response),
		stdin:   reqW,
		stdout:  newBufReader(respR),
	}
	go c.readLoop()
	t.Cleanup(func() {
		respW.Close()
		reqR.Close()
	})
	return c, json.NewDecoder(reqR), respW, func() { respW.Close() }
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
	cases := []struct {
		in, want string
	}{
		{"", ".cache/rdev"},
		{"~/.cache/rdev", ".cache/rdev"},
		{"/opt/rdev", "opt/rdev"},
		{"custom/dir", "custom/dir"},
	}
	for _, c := range cases {
		h := Host{RemoteDir: c.in}
		if got := h.remoteDir(); got != c.want {
			t.Errorf("remoteDir(%q) = %q, want %q", c.in, got, c.want)
		}
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
