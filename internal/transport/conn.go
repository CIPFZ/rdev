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

	"github.com/tonynyyan/rdev/internal/proto"
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

// Conn is a live connection to one remote agent. Safe for concurrent use: a
// mutex serializes request/response pairs over the single agent pipe.
type Conn struct {
	host Host

	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr *strings.Builder

	seq    int
	closed bool

	// ctlPath is the ssh ControlMaster socket, shared by aux commands so they
	// skip a fresh TCP+auth handshake.
	ctlPath string
	// agentPath is the absolute remote path of the installed agent binary.
	agentPath string
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

	c := &Conn{host: host, stderr: &strings.Builder{}}

	ctl, err := controlPath(host)
	if err != nil {
		return nil, err
	}
	c.ctlPath = ctl

	// Open the ControlMaster first so bootstrap and the serve session share one
	// authenticated channel.
	if err := c.openMaster(ctx); err != nil {
		return nil, fmt.Errorf("ssh control master: %w", err)
	}

	goos, goarch, err := c.detectPlatform(ctx)
	if err != nil {
		return nil, fmt.Errorf("detect remote platform: %w", err)
	}
	c.host.GOOS, c.host.GOARCH = goos, goarch

	bin, err := lookup(goos, goarch)
	if err != nil {
		return nil, fmt.Errorf("no agent build for %s/%s: %w", goos, goarch, err)
	}

	if err := c.ensureAgent(ctx, bin); err != nil {
		return nil, fmt.Errorf("bootstrap agent: %w", err)
	}
	if err := c.startAgent(ctx); err != nil {
		return nil, fmt.Errorf("start agent: %w", err)
	}

	// Handshake: confirms the binary runs and speaks our protocol version.
	resp, err := c.Do(ctx, &proto.Request{Op: proto.OpPing})
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("agent handshake: %w", err)
	}
	if resp.Ping == nil || resp.Ping.Version != proto.Version {
		c.Close()
		got := 0
		if resp.Ping != nil {
			got = resp.Ping.Version
		}
		return nil, fmt.Errorf("agent protocol %d, want %d", got, proto.Version)
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

// openMaster primes the shared ssh connection. A failure here is a real
// connectivity or auth problem, so it surfaces the ssh stderr verbatim.
func (c *Conn) openMaster(ctx context.Context) error {
	args := append(c.sshBase(), c.host.Addr, "true")
	cmd := exec.CommandContext(ctx, "ssh", args...)
	var errBuf strings.Builder
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			msg = err.Error()
		}
		return errors.New(msg)
	}
	return nil
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

func (c *Conn) detectPlatform(ctx context.Context) (goos, goarch string, err error) {
	out, err := c.runSSH(ctx, "uname", "-s", "-m")
	if err != nil {
		return "", "", err
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) < 2 {
		return "", "", fmt.Errorf("unexpected uname output %q", out)
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

// ensureAgent uploads the agent when the remote copy is missing or stale.
//
// Staleness is decided by content hash rather than mtime or version string, so
// a rebuilt binary is always picked up and an unchanged one is never re-sent.
func (c *Conn) ensureAgent(ctx context.Context, bin *AgentBinary) error {
	dir := c.host.remoteDir()
	remote := dir + "/rdev-agent"

	// Resolve to an absolute path once: the agent needs it to re-exec itself in
	// supervisor mode, and "~" would not survive that.
	home, err := c.runSSH(ctx, "printf", "%s", "$HOME")
	if err != nil {
		return fmt.Errorf("resolve remote home: %w", err)
	}
	home = strings.TrimSpace(home)
	if home == "" {
		return errors.New("remote $HOME is empty")
	}
	c.agentPath = home + "/" + remote

	want := bin.SHA256
	if want == "" {
		sum := sha256.Sum256(bin.Data)
		want = hex.EncodeToString(sum[:])
	}

	// sha256sum is absent on some minimal images; treat any probe failure as
	// "needs upload" rather than erroring out.
	if out, err := c.runSSH(ctx, "sha256sum", c.agentPath); err == nil {
		if fields := strings.Fields(out); len(fields) > 0 && fields[0] == want {
			return nil // already current
		}
	}

	if _, err := c.runSSH(ctx, "mkdir", "-p", home+"/"+dir+"/jobs"); err != nil {
		return fmt.Errorf("create remote dir: %w", err)
	}

	// Upload to a unique temp name and rename into place, so a concurrent agent
	// is never executing a partially written file and two simultaneous
	// bootstraps (possibly from separate rdev processes) cannot clobber each
	// other's upload.
	tmp := fmt.Sprintf("%s.tmp.%d", c.agentPath, os.Getpid())
	if err := c.upload(ctx, bin.Data, tmp); err != nil {
		return err
	}
	if _, err := c.runSSH(ctx, "chmod", "755", tmp); err != nil {
		return fmt.Errorf("chmod agent: %w", err)
	}
	// mv within a directory is atomic, so a racing bootstrap either sees the old
	// binary or the new one, never a partial file.
	if _, err := c.runSSH(ctx, "mv", "-f", tmp, c.agentPath); err != nil {
		return fmt.Errorf("install agent: %w", err)
	}
	return nil
}

// upload streams data to a remote path via `dd`, which avoids scp/sftp
// availability differences and reuses the ControlMaster connection.
func (c *Conn) upload(ctx context.Context, data []byte, remotePath string) error {
	args := append(c.sshBase(), c.host.Addr, "dd", "of="+remotePath, "status=none")
	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdin = strings.NewReader(string(data))
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
func (c *Conn) startAgent(ctx context.Context) error {
	args := append(c.sshBase(), c.host.Addr, c.agentPath)
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
	return nil
}

// Do sends one request and returns its response.
//
// Calls are serialized: the agent pipe carries a single conversation, and
// interleaving writes would corrupt framing.
func (c *Conn) Do(ctx context.Context, req *proto.Request) (*proto.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errors.New("connection closed")
	}

	c.seq++
	req.ID = fmt.Sprint(c.seq)

	line, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	if _, err := c.stdin.Write(append(line, '\n')); err != nil {
		return nil, fmt.Errorf("write request (agent gone? stderr=%q): %w", c.stderrTail(), err)
	}

	type result struct {
		resp *proto.Response
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		raw, err := readLine(c.stdout)
		if err != nil {
			ch <- result{nil, fmt.Errorf("read response (stderr=%q): %w", c.stderrTail(), err)}
			return
		}
		var resp proto.Response
		if err := json.Unmarshal(raw, &resp); err != nil {
			ch <- result{nil, fmt.Errorf("decode response %q: %w", truncate(string(raw), 200), err)}
			return
		}
		ch <- result{&resp, nil}
	}()

	select {
	case <-ctx.Done():
		// The agent may still write this response later, desynchronizing the
		// stream. Close so the next call reconnects instead of reading a stale
		// reply as if it were fresh.
		c.closeLocked()
		return nil, ctx.Err()
	case r := <-ch:
		if r.err != nil {
			return nil, r.err
		}
		if !r.resp.OK {
			return r.resp, fmt.Errorf("remote: %s", r.resp.Err)
		}
		return r.resp, nil
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
