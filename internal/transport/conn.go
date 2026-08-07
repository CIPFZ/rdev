// Package transport connects the rdev host to a remote rdev-agent.
//
// One Conn owns one ssh process running the agent in serve mode, plus an ssh
// ControlMaster socket shared by auxiliary commands (bootstrap, rsync). Requests
// and responses are newline-delimited JSON over the agent's stdin/stdout.
//
// The agent binary is uploaded on demand: Conn compares the local build's
// SHA-256 against what is installed under ~/.cache/rdev and re-uploads when they
// differ. Callers never manage that, so a fresh machine needs zero setup.
package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// Host describes how to reach a remote machine.
type Host struct {
	// Name is the local alias used in tool calls.
	Name string
	// Addr is the ssh destination, "user@host" or an ssh_config alias.
	Addr string
	// Port is the ssh port. 0 means the ssh default.
	Port int
	// RemoteDir holds agent state. Defaults to "~/.cache/rdev".
	RemoteDir string
	// GOOS and GOARCH select which agent build to upload. Detected on first
	// connect when empty.
	GOOS   string
	GOARCH string
}

// remoteDir returns the agent state directory, defaulting when unset.
func (h *Host) remoteDir() string {
	if h.RemoteDir == "" {
		return ".cache/rdev"
	}
	return strings.TrimPrefix(strings.TrimPrefix(h.RemoteDir, "~/"), "/")
}

// Conn is a live connection to one remote agent. Safe for concurrent use, and
// genuinely concurrent: requests carry an ID and are matched to replies by it, so
// a slow command does not delay unrelated ones on the same host.
type Conn struct {
	host Host

	// mu guards the fields below plus in-flight bookkeeping. It is never held
	// across a write or a round trip -- only long enough to register a pending
	// call, so reply delivery never waits on a slow send.
	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr *lockedBuilder
	// writeMu serializes writes to the agent's stdin. Separate from mu on purpose:
	// a write to a full pipe blocks until the agent drains it, and holding mu
	// through that would stop readLoop from delivering the very replies that let
	// the agent make progress.
	writeMu sync.Mutex

	seq    int
	closed bool
	// pending maps a request ID to the channel awaiting its reply. The reader
	// goroutine owns delivery; Do only registers and waits.
	pending map[string]chan *proto.Response
	// readErr records why the reader stopped, so a caller blocked on a reply
	// learns the real cause instead of a bare timeout.
	readErr error

	// ctlPath is the ssh ControlMaster socket, shared by aux commands so they
	// skip a fresh TCP+auth handshake.
	ctlPath string
	// agentPath is the absolute remote path of the installed agent binary.
	agentPath string
	// stateDir is the absolute remote directory holding job records. Passed to
	// the agent explicitly so both sides agree even for a custom RemoteDir.
	stateDir string
}

// lockedBuilder is a strings.Builder safe for concurrent append and read.
//
// The ssh process writes agent stderr from its own goroutine while callers read
// it to explain failures, so the buffer needs its own lock.
type lockedBuilder struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuilder) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuilder) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// AgentBinary supplies the locally built agent for a target platform. The MCP
// server embeds builds and looks them up by GOOS/GOARCH.
type AgentBinary struct {
	Data   []byte
	SHA256 string
}

// Dial establishes a connection, bootstrapping the agent when needed.
//
// lookup resolves an agent build for the remote platform. It is called only
// when an upload is required, so a warm connection costs one ssh round trip.
func Dial(ctx context.Context, host Host, lookup func(goos, goarch string) (*AgentBinary, error)) (*Conn, error) {
	if host.Addr == "" {
		return nil, errors.New("host addr required")
	}

	c := &Conn{
		host:    host,
		stderr:  &lockedBuilder{},
		pending: make(map[string]chan *proto.Response),
	}

	ctl, err := controlPath(host)
	if err != nil {
		return nil, err
	}
	c.ctlPath = ctl

	// One probe replaces four sequential round trips (connect, uname, $HOME,
	// sha256sum). On a jump host every trip is a full RTT, and they were strictly
	// ordered, so this is the bulk of warm-connect latency. It also opens the
	// ControlMaster as a side effect, which is what the separate connect step was
	// for.
	probe, err := c.probeRemote(ctx)
	if err != nil {
		return nil, err
	}
	c.host.GOOS, c.host.GOARCH = probe.goos, probe.goarch
	c.stateDir = probe.home + "/" + host.remoteDir()
	c.agentPath = probe.home + "/" + host.remoteDir() + "/rdev-agent"

	bin, err := lookup(probe.goos, probe.goarch)
	if err != nil {
		return nil, fmt.Errorf("no agent build for %s/%s: %w", probe.goos, probe.goarch, err)
	}

	if err := c.ensureAgent(ctx, bin, probe.agentSHA); err != nil {
		return nil, fmt.Errorf("bootstrap agent: %w", err)
	}
	if err := c.startAgent(ctx); err != nil {
		return nil, fmt.Errorf("start agent: %w", err)
	}

	// Handshake: confirms the binary runs and speaks a format we can use.
	resp, err := c.Do(ctx, &proto.Request{Op: proto.OpPing})
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("agent handshake: %w", err)
	}
	if !resp.Ping.Compatible(proto.Version) {
		c.Close()
		if resp.Ping == nil {
			return nil, errors.New("agent handshake returned no identity")
		}
		// Name the direction and the fix. An "agent protocol N, want M" alone leaves
		// the caller guessing which side to update, and the answer is almost always
		// the same: rebuild so the embedded agent matches.
		if resp.Ping.Version < proto.Version {
			return nil, fmt.Errorf(
				"remote agent at %s speaks protocol %d but this rdev needs %d; "+
					"it was installed by an older rdev -- run 'make agents && make build' and reconnect",
				c.agentPath, resp.Ping.Version, proto.Version)
		}
		return nil, fmt.Errorf(
			"remote agent at %s speaks protocol %d-%d and cannot serve this rdev's %d; "+
				"a newer rdev installed it -- update this rdev to match",
			c.agentPath, resp.Ping.MinVersion, resp.Ping.Version, proto.Version)
	}
	return c, nil
}

// sshBase returns the ssh args shared by every invocation, including the
// ControlMaster settings that make repeat connections nearly free.
func (c *Conn) sshBase() []string {
	args := []string{
		"-o", "BatchMode=yes",
		"-o", "ControlMaster=auto",
		"-o", "ControlPath=" + c.ctlPath,
		"-o", "ControlPersist=300",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=4",
	}
	if c.host.Port != 0 {
		args = append(args, "-p", fmt.Sprint(c.host.Port))
	}
	return args
}

// remoteProbe is everything Dial needs to know about the remote machine.
type remoteProbe struct {
	goos, goarch string
	home         string
	// agentSHA is the installed agent's checksum, empty when absent or when the
	// remote has no usable sha256 tool.
	agentSHA string
}

// probeRemote gathers the platform, home directory, and installed agent checksum
// in a single ssh invocation.
//
// The script is a fixed string with no interpolated values, so it introduces no
// quoting risk: the one variable part is the agent path, and it is built from
// $HOME on the remote side rather than substituted in. Output is line-oriented
// and prefixed so a chatty profile printing to stdout cannot be mistaken for
// probe data.
func (c *Conn) probeRemote(ctx context.Context) (*remoteProbe, error) {
	const script = `printf 'rdev-os %s\n' "$(uname -s)"
printf 'rdev-arch %s\n' "$(uname -m)"
printf 'rdev-home %s\n' "$HOME"
a="$HOME/` + agentRelPathPlaceholder + `"
if [ -f "$a" ]; then
  s=$(sha256sum "$a" 2>/dev/null || shasum -a 256 "$a" 2>/dev/null || true)
  printf 'rdev-sha %s\n' "${s%% *}"
fi`

	// The agent path is the only variable, and it is a fixed relative path from
	// the host's own config, not user input from a tool call.
	sh := strings.Replace(script, agentRelPathPlaceholder, c.host.remoteDir()+"/rdev-agent", 1)

	out, err := c.runSSH(ctx, "sh", "-c", shellQuote(sh))
	if err != nil {
		// A failure here is a real connectivity or auth problem; surface ssh's own
		// message, which explains it better than a wrapped error would.
		return nil, fmt.Errorf("connect and probe %s: %w", c.host.Addr, err)
	}
	return parseProbe(out)
}

// parseProbe reads the probe script's output.
//
// Kept separate from the ssh call so it can be tested against the shapes real
// machines produce -- notably a chatty ~/.bashrc writing to stdout, which is why
// every field is prefixed rather than positional.
func parseProbe(out string) (*remoteProbe, error) {
	p := &remoteProbe{}
	var rawOS, rawArch string
	for _, line := range strings.Split(out, "\n") {
		key, val, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			continue
		}
		switch key {
		case "rdev-os":
			rawOS = val
		case "rdev-arch":
			rawArch = val
		case "rdev-home":
			p.home = val
		case "rdev-sha":
			p.agentSHA = val
		}
	}

	if p.home == "" {
		return nil, fmt.Errorf("remote $HOME is empty (probe output %q)", truncate(out, 200))
	}
	goos, goarch, err := mapPlatform(rawOS + " " + rawArch)
	if err != nil {
		return nil, err
	}
	p.goos, p.goarch = goos, goarch
	return p, nil
}

// agentRelPathPlaceholder marks where the agent's relative path is substituted
// into the probe script.
const agentRelPathPlaceholder = "__RDEV_AGENT__"

// shellQuote wraps s in single quotes for safe passage through ssh's remote
// shell, which concatenates arguments into one command line.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// runSSH executes one auxiliary command over the shared connection. argv is
// passed as distinct ssh arguments; ssh still concatenates them into a remote
// shell command, so this is used only for fixed, argument-free probes.
func (c *Conn) runSSH(ctx context.Context, argv ...string) (string, error) {
	args := append(c.sshBase(), c.host.Addr)
	args = append(args, argv...)
	cmd := exec.CommandContext(ctx, "ssh", args...)
	var out, errBuf strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			msg = err.Error()
		}
		return out.String(), errors.New(msg)
	}
	return out.String(), nil
}

// mapPlatform turns `uname -s -m` output into GOOS/GOARCH.
//
// Kept separate from the ssh call so the mapping is testable: the spellings real
// machines report (x86_64 vs amd64, aarch64 vs arm64) are the whole difficulty.
func mapPlatform(unameOut string) (goos, goarch string, err error) {
	fields := strings.Fields(strings.TrimSpace(unameOut))
	if len(fields) < 2 {
		return "", "", fmt.Errorf("unexpected uname output %q", unameOut)
	}

	switch strings.ToLower(fields[0]) {
	case "linux":
		goos = "linux"
	case "darwin":
		goos = "darwin"
	default:
		return "", "", fmt.Errorf("unsupported remote OS %q", fields[0])
	}

	switch fields[1] {
	case "x86_64", "amd64":
		goarch = "amd64"
	case "aarch64", "arm64":
		goarch = "arm64"
	default:
		return "", "", fmt.Errorf("unsupported remote arch %q", fields[1])
	}
	return goos, goarch, nil
}

// newBufReader wraps a reader with the buffer size the agent stream uses.
func newBufReader(r io.Reader) *bufio.Reader { return bufio.NewReaderSize(r, 1<<20) }

// ensureAgent uploads the agent when the remote copy is missing or stale.
//
// Staleness is decided by content hash rather than mtime or version string, so
// a rebuilt binary is always picked up and an unchanged one is never re-sent.
//
// installedSHA comes from the connect probe, so the common case -- agent already
// current -- costs no ssh round trips at all here.
func (c *Conn) ensureAgent(ctx context.Context, bin *AgentBinary, installedSHA string) error {
	want := bin.SHA256
	if want == "" {
		sum := sha256.Sum256(bin.Data)
		want = hex.EncodeToString(sum[:])
	}
	if installedSHA != "" && installedSHA == want {
		return nil // already current
	}

	// Upload to a unique temp name, then chmod and rename in one round trip. The
	// rename is atomic within a directory, so a racing bootstrap (possibly from
	// another rdev process) sees either the old binary or the new one, never a
	// partial file, and never a file that is complete but not yet executable.
	tmp := fmt.Sprintf("%s.tmp.%d", c.agentPath, os.Getpid())
	if _, err := c.runSSH(ctx, "mkdir", "-p", c.stateDir+"/jobs"); err != nil {
		return fmt.Errorf("create remote dir: %w", err)
	}
	if err := c.upload(ctx, bin.Data, tmp); err != nil {
		return err
	}
	if _, err := c.runSSH(ctx, "sh", "-c", shellQuote(
		fmt.Sprintf("chmod 755 %s && mv -f %s %s", shellQuote(tmp), shellQuote(tmp), shellQuote(c.agentPath)),
	)); err != nil {
		return fmt.Errorf("install agent: %w", err)
	}
	return nil
}

// upload streams data to a remote path via `dd`, which avoids scp/sftp
// availability differences and reuses the ControlMaster connection.
func (c *Conn) upload(ctx context.Context, data []byte, remotePath string) error {
	args := append(c.sshBase(), c.host.Addr, "dd", "of="+remotePath, "status=none")
	cmd := exec.CommandContext(ctx, "ssh", args...)
	// bytes.NewReader, not strings.NewReader(string(data)): the latter copies the
	// whole embedded agent (~9 MB) for nothing.
	cmd.Stdin = bytes.NewReader(data)
	var errBuf strings.Builder
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("upload to %s: %s", remotePath, msg)
	}
	return nil
}

// startAgent launches the long-lived serve process over its own ssh session.
//
// The state directory is passed explicitly so the agent writes job records where
// this host installed it. Letting the agent default independently would silently
// split the two sides apart whenever RemoteDir is customized.
func (c *Conn) startAgent(ctx context.Context) error {
	args := append(c.sshBase(), c.host.Addr, c.agentPath, "-state", c.stateDir)
	cmd := exec.Command("ssh", args...) // no ctx: lifetime is managed by Close
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = c.stderr

	if err := cmd.Start(); err != nil {
		return err
	}
	c.cmd = cmd
	c.stdin = stdin
	c.stdout = bufio.NewReaderSize(stdout, 1<<20)
	// One goroutine owns the reader from here on, so callers never touch the pipe
	// and cannot consume each other's replies.
	go c.readLoop()
	return nil
}

// Do sends one request and returns its response.
//
// Concurrent calls are supported: each request carries a unique ID, a single
// reader goroutine routes replies back by that ID, and the connection mutex is
// held only long enough to register the pending call. A minutes-long job_wait
// therefore does not stall an exec on the same host.
func (c *Conn) Do(ctx context.Context, req *proto.Request) (*proto.Response, error) {
	c.mu.Lock()
	if c.closed {
		err := c.readErr
		c.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("connection closed: %w", err)
		}
		return nil, errors.New("connection closed")
	}
	c.seq++
	req.ID = fmt.Sprint(c.seq)
	ch := make(chan *proto.Response, 1)
	c.pending[req.ID] = ch
	stdin := c.stdin
	c.mu.Unlock()

	line, err := json.Marshal(req)
	if err != nil {
		c.abandon(req.ID)
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	// Write under writeMu, not mu: a full pipe blocks here until the agent reads,
	// and readLoop must stay free to deliver replies meanwhile.
	c.writeMu.Lock()
	_, writeErr := stdin.Write(append(line, '\n'))
	c.writeMu.Unlock()
	if writeErr != nil {
		c.abandon(req.ID)
		return nil, fmt.Errorf("write request (agent gone? stderr=%q): %w", c.stderrTail(), writeErr)
	}

	select {
	case <-ctx.Done():
		// Stop waiting, but leave the ID registered so the reader can discard the
		// reply when it arrives. Unlike the previous serial design, an abandoned
		// request no longer desynchronizes the stream, so the connection survives
		// and other in-flight calls are unaffected.
		c.abandon(req.ID)
		return nil, ctx.Err()
	case resp := <-ch:
		if resp == nil {
			c.mu.Lock()
			err := c.readErr
			c.mu.Unlock()
			if err == nil {
				err = errors.New("connection closed")
			}
			return nil, fmt.Errorf("read response (stderr=%q): %w", c.stderrTail(), err)
		}
		if !resp.OK {
			return resp, fmt.Errorf("remote: %s", resp.Err)
		}
		return resp, nil
	}
}

// abandon drops a pending entry whose caller gave up waiting.
func (c *Conn) abandon(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pending, id)
}

// readLoop routes agent replies to waiting callers until the stream ends.
//
// One goroutine owns the reader, which is what lets Do be concurrent: no caller
// ever reads the pipe, so no caller can consume another's reply.
func (c *Conn) readLoop() {
	for {
		raw, err := readLine(c.stdout)
		if err != nil {
			c.failAllPending(err)
			return
		}
		var resp proto.Response
		if err := json.Unmarshal(raw, &resp); err != nil {
			// A corrupt line means the stream framing is no longer trustworthy;
			// failing every waiter beats handing back mismatched replies.
			c.failAllPending(fmt.Errorf("decode response %q: %w", truncate(string(raw), 200), err))
			return
		}

		c.mu.Lock()
		ch, ok := c.pending[resp.ID]
		if ok {
			delete(c.pending, resp.ID)
		}
		c.mu.Unlock()
		if ok {
			ch <- &resp
			continue
		}
		// No waiter: the caller timed out and abandoned this ID. Dropping the
		// reply is correct and, unlike a serial stream, harmless.
	}
}

// failAllPending wakes every waiter after the reader stops.
func (c *Conn) failAllPending(err error) {
	c.mu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	pending := c.pending
	c.pending = make(map[string]chan *proto.Response)
	c.closed = true
	c.mu.Unlock()

	// A nil send tells the waiter to consult readErr for the cause.
	for _, ch := range pending {
		ch <- nil
	}
}

// readLine reads one NDJSON record of arbitrary length.
func readLine(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		buf = append(buf, chunk...)
		if !isPrefix {
			return buf, nil
		}
	}
}

// stderrTail returns recent agent stderr, which usually explains a transport
// failure ("permission denied", "no such file") better than the io error does.
func (c *Conn) stderrTail() string {
	s := c.stderr.String()
	if len(s) > 400 {
		s = s[len(s)-400:]
	}
	return strings.TrimSpace(s)
}

// AgentPath is the absolute remote path of the installed agent binary.
func (c *Conn) AgentPath() string { return c.agentPath }

// Host returns the connection's host descriptor.
func (c *Conn) Host() Host { return c.host }

// SSHArgs returns the base ssh options, so helpers like rsync can reuse the
// same multiplexed connection.
func (c *Conn) SSHArgs() []string { return c.sshBase() }

// Close terminates the agent session. The ControlMaster is left to expire on
// its own ControlPersist timer, keeping reconnects fast.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeLocked()
}

func (c *Conn) closeLocked() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.readErr == nil {
		c.readErr = errors.New("connection closed by caller")
	}
	// Wake anyone still waiting for a reply. Closing stdin usually makes readLoop
	// see EOF and do this itself, but a caller blocked on Do must not depend on
	// that race: an unwoken waiter would hang until its context expired.
	pending := c.pending
	c.pending = make(map[string]chan *proto.Response)
	for _, ch := range pending {
		select {
		case ch <- nil:
		default: // buffered channel already holds a reply
		}
	}
	if c.stdin != nil {
		c.stdin.Close() // EOF makes the agent exit its read loop cleanly
	}
	if c.cmd != nil && c.cmd.Process != nil {
		done := make(chan struct{})
		go func() { c.cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			c.cmd.Process.Kill()
			<-done
		}
	}
	return nil
}

// controlPath returns a per-host ControlMaster socket path.
//
// The name is hashed because ssh rejects control paths longer than a sockaddr_un
// (~104 bytes), which "user@long.host.name:port" can exceed.
func controlPath(h Host) (string, error) {
	dir := filepath.Join(os.TempDir(), "rdev-ctl")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	key := fmt.Sprintf("%s:%d", h.Addr, h.Port)
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(dir, hex.EncodeToString(sum[:])[:16]), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
