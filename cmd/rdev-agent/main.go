// Command rdev-agent runs on a remote machine and serves rdev requests over
// stdin/stdout using newline-delimited JSON.
//
// It is uploaded automatically by the rdev host and is not meant to be invoked
// by hand, though `rdev-agent -version` is handy when debugging a bootstrap.
//
// Design notes:
//
//   - Requests carry argv slices and are exec'd directly. No shell parses them,
//     so quoting bugs are structurally impossible.
//   - Jobs are detached with setsid and their output is written to files under
//     the state dir. The agent may die (ssh drop, host restart) without killing
//     or losing a running job; a later agent re-reads the same files.
//   - Every reply is a single line, so the host can frame responses by newline
//     without a length prefix.
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
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/CIPFZ/rdev/internal/buildinfo"
	"github.com/CIPFZ/rdev/internal/proto"
)

const (
	defaultReadLimit   = 1 << 20 // 1 MiB
	defaultMaxOutput   = 256 << 10
	maxRequestLineLen  = int(proto.AbsoluteRequestFrameBytes)
	hardExecTimeoutSec = 3600
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-version" || os.Args[1] == "--version") {
		// The build stamp goes on its own prefixed line, and the host parses it to
		// decide whether replacing this binary would be a downgrade. Prefixed
		// rather than positional for the same reason the connect probe is: a
		// chatty profile can write to stdout first.
		fmt.Printf("rdev-agent proto=%d-%d %s/%s\n",
			proto.MinVersion, proto.Version, runtime.GOOS, runtime.GOARCH)
		fmt.Println(buildinfo.StampLine())
		return
	}

	// Supervisor mode: rdev-agent -supervise <jobdir> -- <argv...>
	// Used by job_start so a detached job still gets its exit code recorded
	// after the serving agent dies with the ssh connection.
	if len(os.Args) > 3 && os.Args[1] == superviseFlag {
		jobDir := os.Args[2]
		rest := os.Args[3:]
		if rest[0] != "--" {
			fmt.Fprintln(os.Stderr, "rdev-agent -supervise: expected -- before argv")
			os.Exit(2)
		}
		runSupervisor(jobDir, rest[1:])
		return // unreachable: runSupervisor exits
	}

	// -state <dir> tells the agent where its job records live. The host passes
	// the same directory it installed the binary into, so a custom RemoteDir
	// cannot leave the two sides reading different paths. Absent, it falls back
	// to the default so an agent started by hand still works.
	stateArg := ""
	if len(os.Args) > 2 && os.Args[1] == stateFlag {
		stateArg = os.Args[2]
	}

	state, err := stateDir(stateArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rdev-agent: %v\n", err)
		os.Exit(1)
	}

	in := bufio.NewReaderSize(os.Stdin, 1<<20)
	// Replies are written from many goroutines, so the writer is guarded: a
	// response is one line, and interleaved writes would corrupt the framing the
	// host relies on.
	w := &respWriter{out: bufio.NewWriter(os.Stdout)}

	server := newAgentServer(context.Background(), state, w)

	for {
		line, err := readLine(in)
		if err != nil {
			break // EOF or transport error: the host closed the pipe.
		}
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var req proto.Request
		if err := json.Unmarshal(line, &req); err != nil {
			envelope := proto.NewError(proto.CodeInvalidFrame, "", proto.StateNotSent)
			w.write(&proto.Response{
				Type: proto.EventError, Terminal: true, OK: false,
				Err: envelope.Message, Error: envelope, Execution: envelope.ExecutionState,
			})
			continue
		}
		server.submit(req)
	}

	// EOF cancels every attached foreground operation and watcher. Detached jobs
	// have an explicit independent lifecycle and continue under their supervisor.
	server.close()
	w.flush()
}

// shutdownDrainTimeout bounds the post-disconnect drain. Long enough for an
// in-flight reply to be written, short enough that a dropped ssh session does not
// leave an agent lingering.
const shutdownDrainTimeout = 2 * time.Second

// maxConcurrentRequests bounds in-flight handlers. High enough that normal
// parallel tool calls never queue, low enough that a runaway caller cannot fork
// hundreds of processes on a machine someone else is using.
const maxConcurrentRequests = 16

// respWriter serializes replies onto the single stdout pipe.
type respWriter struct {
	mu             sync.Mutex
	controlWaiting atomic.Int32
	out            *bufio.Writer
}

func (w *respWriter) write(resp *proto.Response) bool {
	b, err := json.Marshal(resp)
	if err != nil {
		envelope := proto.NewError(proto.CodeInternalFailure, resp.OperationID, proto.StatePossiblyExecuted)
		b, _ = json.Marshal(errorResponse(&proto.Request{ID: resp.ID, OperationID: resp.OperationID}, envelope, max(resp.Seq, 1)))
	}
	if int64(len(b)) > proto.AbsoluteResponseFrameBytes {
		envelope := proto.NewError(proto.CodeFrameTooLarge, resp.OperationID, proto.StatePossiblyExecuted)
		truncation, _ := proto.NewTruncation(int64(len(b)), 0)
		envelope.Truncation = &truncation
		b, _ = json.Marshal(errorResponse(&proto.Request{ID: resp.ID, OperationID: resp.OperationID}, envelope, max(resp.Seq, 1)))
	}
	if resp.Type == proto.EventData {
		if w.controlWaiting.Load() != 0 || !w.mu.TryLock() {
			return false
		}
	} else {
		w.controlWaiting.Add(1)
		w.mu.Lock()
		w.controlWaiting.Add(-1)
	}
	defer w.mu.Unlock()
	if _, err := w.out.Write(b); err != nil {
		return false
	}
	if err := w.out.WriteByte('\n'); err != nil {
		return false
	}
	return w.out.Flush() == nil // flush per reply: the host blocks waiting for this line
}

func (w *respWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.out.Flush()
}

// handleSafely runs handle and converts a panic into an error reply.
//
// The agent serves every request for a host on one process, and a running job's
// only observer is that process. A nil map, a bad slice length, or any other
// latent panic in one handler would otherwise take down the serve loop, drop the
// host's pooled connections, and leave detached jobs unobservable. Turning it
// into a failed request keeps the blast radius at the one call that caused it.
func handleSafely(req *proto.Request, state string) (resp *proto.Response) {
	defer func() {
		if r := recover(); r != nil {
			// The stack goes to stderr, which the host captures and surfaces in
			// transport errors, so a panic stays diagnosable.
			fmt.Fprintf(os.Stderr, "rdev-agent: panic handling op %q: %v\n%s\n", req.Op, r, debug.Stack())
			resp = &proto.Response{
				ID:  req.ID,
				OK:  false,
				Err: fmt.Sprintf("agent panic handling %q: %v", req.Op, r),
			}
		}
	}()
	return handle(req, state)
}

// readLine reads one newline-terminated record, growing past bufio's buffer so
// large write_file payloads are not rejected.
func readLine(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		remaining := maxRequestLineLen - len(buf)
		if remaining < 1<<20 {
			b, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			if b == '\n' {
				return buf, nil
			}
			if remaining == 0 {
				return nil, errors.New("request too large")
			}
			buf = append(buf, b)
			continue
		}
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		if len(chunk) > remaining || len(buf) > maxRequestLineLen-len(chunk) {
			return nil, errors.New("request too large")
		}
		buf = append(buf, chunk...)
		if !isPrefix {
			return buf, nil
		}
	}
}

func handle(req *proto.Request, state string) *proto.Response {
	resp := &proto.Response{ID: req.ID}
	var err error

	switch req.Op {
	case proto.OpPing:
		resp.Ping = doPing()
	case proto.OpExec:
		if req.Exec == nil {
			err = errors.New("exec params required")
			break
		}
		resp.Exec, err = doExec(req.Exec)
	case proto.OpReadFile:
		if req.Read == nil {
			err = errors.New("read params required")
			break
		}
		resp.Read, err = doRead(req.Read)
	case proto.OpWriteFile:
		if req.Cat == nil {
			err = errors.New("write params required")
			break
		}
		resp.Cat, err = doWrite(req.Cat)
	case proto.OpList:
		if req.List == nil {
			err = errors.New("list params required")
			break
		}
		resp.List, err = doList(req.List)
	default:
		// Job ops are dispatched by doJob, which owns the list of names it
		// handles. Routing anything unrecognized there rather than duplicating
		// the set here means adding a job op cannot be half-wired.
		if !isJobOp(req.Op) {
			// Say what this most likely means. An unadorned "unknown op" reads as a
			// protocol bug, when in practice it means the host is newer than the
			// binary it is talking to.
			err = fmt.Errorf("unknown op %q (this agent speaks protocol %d; "+
				"if the host is newer, rebuild and re-upload it)", req.Op, proto.Version)
			break
		}
		if req.Job == nil {
			err = errors.New("job params required")
			break
		}
		resp.Job, err = doJob(req.Op, req.Job, state)
	}

	if err != nil {
		resp.OK = false
		resp.Err = err.Error()
		return resp
	}
	resp.OK = true
	return resp
}

func doPing() *proto.PingResult {
	home, _ := os.UserHomeDir()
	bin, _ := os.Executable()
	return &proto.PingResult{
		Version:    proto.Version,
		MinVersion: proto.MinVersion,
		Binary:     bin,
		Home:       home,
		OS:         runtime.GOOS,
		Arch:       runtime.GOARCH,
		PID:        os.Getpid(),
		Build:      buildinfo.Stamp(),
	}
}

// expandHome resolves a leading "~" against the user's home directory. The
// agent does this rather than the shell, since argv never reaches a shell.
func expandHome(p string) string {
	if p == "" {
		return ""
	}
	if p != "~" && !strings.HasPrefix(p, "~/") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	if p == "~" {
		return home
	}
	return filepath.Join(home, p[2:])
}

// buildCmd turns ExecParams into an *exec.Cmd.
//
// When LoginShell is set the command becomes:
//
//	bash -lc 'exec "$@"' rdev <argv...>
//
// The profile is sourced, then `exec "$@"` replaces the shell with the target
// process. Because argv arrives as positional parameters rather than embedded
// in the script text, the shell never re-parses it: a filename containing
// spaces, quotes, or `$(...)` is passed through byte-for-byte.
func buildCmd(p *proto.ExecParams) (*exec.Cmd, error) {
	if len(p.Argv) == 0 {
		return nil, errors.New("argv must not be empty")
	}

	var cmd *exec.Cmd
	if p.LoginShell {
		shell := os.Getenv("SHELL")
		if shell == "" {
			shell = "/bin/bash"
		}
		args := append([]string{"-lc", `exec "$@"`, "rdev"}, p.Argv...)
		cmd = exec.Command(shell, args...)
	} else {
		cmd = exec.Command(p.Argv[0], p.Argv[1:]...)
	}

	if p.Cwd != "" {
		dir := expandHome(p.Cwd)
		info, err := os.Stat(dir)
		if err != nil {
			return nil, fmt.Errorf("cwd %q: %w", p.Cwd, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("cwd %q is not a directory", p.Cwd)
		}
		cmd.Dir = dir
	}

	cmd.Env = os.Environ()
	for k, v := range p.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd, nil
}

// capWriter collects output up to a cap while still counting everything, so a
// truncated reply can report the true stream size.
type capWriter struct {
	buf   []byte
	total int64
	cap   int
	hook  func([]byte)
}

func (w *capWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > int64(^uint64(0)>>1)-w.total {
		w.total = int64(^uint64(0) >> 1)
	} else {
		w.total += int64(len(p))
	}
	if room := w.cap - len(w.buf); room > 0 {
		retained := p
		if len(p) <= room {
			w.buf = append(w.buf, p...)
		} else {
			retained = p[:room]
			w.buf = append(w.buf, retained...)
		}
		if w.hook != nil && len(retained) > 0 {
			w.hook(retained)
		}
	}
	return len(p), nil // always report full write: we intentionally drop excess
}

func (w *capWriter) truncated() bool { return w.total > int64(len(w.buf)) }

// text returns the captured output, trimmed so it never ends mid-character.
//
// The cap is a byte budget, so cutting at exactly that offset can split a
// multi-byte rune and yield a replacement character in the reply. Dropping the
// partial tail costs at most three bytes and keeps the output valid UTF-8.
func (w *capWriter) text() string {
	b := w.buf
	if w.truncated() {
		b = trimPartialRune(b)
	}
	return string(b)
}

func (w *capWriter) payload() (string, bool, int64) {
	raw := w.buf
	text := raw
	if w.truncated() {
		text = trimPartialRune(text)
	}
	if utf8.Valid(text) && !bytes.ContainsRune(text, 0) {
		return string(text), false, int64(len(text))
	}
	return base64.StdEncoding.EncodeToString(raw), true, int64(len(raw))
}

// trimPartialRune removes an incomplete UTF-8 sequence from the end of b.
func trimPartialRune(b []byte) []byte {
	// A rune is at most 4 bytes, so an incomplete tail is within the last 3.
	for i := len(b); i > 0 && i > len(b)-4; i-- {
		r, size := utf8.DecodeLastRune(b[:i])
		if r != utf8.RuneError || size > 1 {
			return b[:i]
		}
	}
	if len(b) >= 4 {
		return b[:len(b)-3]
	}
	return b
}

func doExec(p *proto.ExecParams) (*proto.ExecResult, error) {
	return doExecContext(context.Background(), p)
}

func doExecContext(ctx context.Context, p *proto.ExecParams) (*proto.ExecResult, error) {
	return doExecContextStream(ctx, p, nil, nil)
}

func doExecContextStream(ctx context.Context, p *proto.ExecParams, stdoutHook, stderrHook func([]byte)) (*proto.ExecResult, error) {
	cmd, err := buildCmd(p)
	if err != nil {
		return nil, err
	}

	limit := p.MaxOutputBytes
	if limit < 0 || int64(limit) > proto.AbsoluteOutputBytes {
		return nil, errors.New("max_output_bytes is outside the hard limit")
	}
	if limit == 0 {
		limit = defaultMaxOutput
	}
	if p.TimeoutSec < 0 || p.TimeoutSec > hardExecTimeoutSec {
		return nil, errors.New("timeout_sec is outside the hard limit")
	}
	if int64(len(p.Stdin)) > proto.AbsoluteRequestFrameBytes {
		return nil, errors.New("stdin exceeds the hard limit")
	}
	stdout := &capWriter{cap: limit, hook: stdoutHook}
	stderr := &capWriter{cap: limit, hook: stderrHook}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if p.Stdin != "" {
		cmd.Stdin = strings.NewReader(p.Stdin)
	}

	// Put the child in its own process group so a timeout kill reaches the
	// whole tree, not just the immediate child.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	start := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %q: %w", p.Argv[0], err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	var timedOut bool
	var canceled bool
	var timer <-chan time.Time
	if p.TimeoutSec > 0 {
		timer = time.After(time.Duration(p.TimeoutSec) * time.Second)
	}
	select {
	case err = <-done:
	case <-ctx.Done():
		canceled = true
		terminateProcessGroup(cmd.Process.Pid, done)
		err = ctx.Err()
	case <-timer:
		timedOut = true
		syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}

	stdoutText, stdoutB64, stdoutRetained := stdout.payload()
	stderrText, stderrB64, stderrRetained := stderr.payload()
	stdoutTruncation, _ := proto.NewTruncation(stdout.total, stdoutRetained)
	stderrTruncation, _ := proto.NewTruncation(stderr.total, stderrRetained)
	res := &proto.ExecResult{
		Stdout:           stdoutText,
		Stderr:           stderrText,
		StdoutB64:        stdoutB64,
		StderrB64:        stderrB64,
		StdoutBytes:      stdout.total,
		StderrBytes:      stderr.total,
		Truncated:        stdout.truncated() || stderr.truncated(),
		StdoutTruncation: stdoutTruncation,
		StderrTruncation: stderrTruncation,
		TimedOut:         timedOut,
		DurationMS:       time.Since(start).Milliseconds(),
	}
	if timedOut {
		res.ExitCode = -1
		return res, nil
	}
	if canceled {
		res.ExitCode = -1
		return res, err
	}
	// A non-zero exit is data, not a transport error: report it in ExitCode
	// and let the caller decide. Only failures to *run* the command error out.
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		res.ExitCode = ee.ExitCode()
	} else if err != nil {
		return nil, err
	}
	return res, nil
}

// terminateProcessGroup gives cooperative children a brief TERM window, then
// kills and reaps the exact process group owned by this request. No process-name
// matching is involved, so unrelated requests are unaffected.
func terminateProcessGroup(pgid int, done <-chan error) {
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	select {
	case <-done:
		return
	case <-time.After(250 * time.Millisecond):
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	<-done
}

func doRead(p *proto.ReadParams) (*proto.ReadResult, error) {
	if p.Offset < 0 {
		return nil, errors.New("read offset must not be negative")
	}
	path := expandHome(p.Path)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s is a directory", p.Path)
	}

	limit := p.Limit
	if limit < 0 || limit > proto.AbsoluteReadBytes {
		return nil, errors.New("read limit is outside the hard limit")
	}
	if limit == 0 {
		limit = defaultReadLimit
	}
	if p.Offset > 0 {
		if _, err := f.Seek(p.Offset, 0); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, limit)
	n, err := readFull(f, buf)
	if err != nil {
		return nil, err
	}
	data := buf[:n]

	original := info.Size() - p.Offset
	if original < 0 {
		original = 0
	}
	truncation, _ := proto.NewTruncation(original, int64(n))
	end, addErr := proto.CheckedAdd(p.Offset, int64(n))
	if addErr != nil {
		return nil, errors.New("read range overflows")
	}
	res := &proto.ReadResult{
		Size: info.Size(), EOF: end >= info.Size(),
		Truncation: truncation,
	}
	// Base64 anything that would not survive the JSON round-trip, so binary or
	// non-UTF-8 content arrives intact instead of being mangled into
	// replacement runes.
	if isJSONSafeText(data) {
		res.Content = string(data)
	} else {
		res.Content = base64.StdEncoding.EncodeToString(data)
		res.ContentB64 = true
	}
	return res, nil
}

func doWrite(p *proto.WriteParams) (*proto.WriteResult, error) {
	path := expandHome(p.Path)
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	data := []byte(p.Content)
	if p.ContentB64 {
		decoded, err := base64.StdEncoding.DecodeString(p.Content)
		if err != nil {
			return nil, fmt.Errorf("decode base64 content: %w", err)
		}
		data = decoded
	}

	mode := os.FileMode(p.Mode)
	if mode == 0 {
		mode = 0o644
	}
	flags := os.O_WRONLY | os.O_CREATE
	if p.Append {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	n, err := f.Write(data)
	if err != nil {
		return nil, err
	}
	// Re-apply the mode: O_CREATE only honors it when the file is new.
	if p.Mode != 0 {
		os.Chmod(path, mode)
	}
	return &proto.WriteResult{Path: path, BytesWritten: n}, nil
}

// readFull fills buf, stopping at EOF rather than treating it as an error.
//
// A short read is normal here: the file may have been truncated between Stat and
// Read, so the caller relies on the returned count instead of the buffer length.
func readFull(f *os.File, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := f.Read(buf[total:])
		total += n
		if errors.Is(err, io.EOF) || n == 0 {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// isJSONSafeText reports whether data can round-trip through JSON unchanged.
//
// Both checks matter. encoding/json replaces invalid UTF-8 with U+FFFD, so
// sending Latin-1 or a truncated multi-byte sequence as a string would silently
// corrupt the content -- the exact mangling base64 exists to avoid. NUL bytes
// survive JSON but signal a binary file, where a text reply is not what the
// caller wants anyway.
func isJSONSafeText(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	return !bytes.ContainsRune(b, 0)
}

// defaultListLimit bounds a listing so a directory with a million entries does
// not blow up the reply.
const defaultListLimit = 1000

// doList reads a directory into structured entries.
//
// This exists so callers stop running `ls -la` and parsing its output: the format
// varies by platform and locale, and filenames with spaces or newlines make the
// parse ambiguous. Entries here carry real types.
func doList(p *proto.ListParams) (*proto.ListResult, error) {
	path := expandHome(p.Path)
	if path == "" {
		path = "."
	}
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	limit := p.Limit
	if limit < 0 || limit > 10_000 {
		return nil, errors.New("list limit is outside the hard limit")
	}
	if limit == 0 {
		limit = defaultListLimit
	}

	res := &proto.ListResult{Path: path, Entries: make([]proto.DirEntry, 0, limit)}
	for {
		entries, readErr := dir.ReadDir(256)
		for _, e := range entries {
			res.Total++
			if len(res.Entries) >= limit {
				res.Truncated = true
				continue
			}
			de := proto.DirEntry{Name: e.Name(), IsDir: e.IsDir()}
			// Report the link itself rather than its target: resolving it may dangle
			// or cross a mount, and the caller can stat the target if it cares.
			de.Symlink = e.Type()&os.ModeSymlink != 0
			if info, infoErr := e.Info(); infoErr == nil {
				de.Size = info.Size()
				de.Mode = info.Mode().String()
				de.ModTime = info.ModTime().UTC().Format(time.RFC3339)
			}
			res.Entries = append(res.Entries, de)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return res, nil
}

// stateFlag is the argv[1] value that overrides the agent's state directory.
const stateFlag = "-state"

// stateDir resolves the directory holding job records, creating it as needed.
//
// dir may be empty (use the default), absolute, or "~"-prefixed: the host stores
// RemoteDir in whichever form the user wrote it, and the agent is the only side
// that can expand "~" correctly for the remote account.
func stateDir(dir string) (string, error) {
	if dir != "" {
		resolved := expandHome(dir)
		if !filepath.IsAbs(resolved) {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", err
			}
			resolved = filepath.Join(home, resolved)
		}
		if err := os.MkdirAll(filepath.Join(resolved, "jobs"), 0o755); err != nil {
			return "", err
		}
		return resolved, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	d := filepath.Join(home, ".cache", "rdev")
	if err := os.MkdirAll(filepath.Join(d, "jobs"), 0o755); err != nil {
		return "", err
	}
	return d, nil
}
