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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/tonynyyan/rdev/internal/proto"
)

const (
	defaultReadLimit  = 1 << 20 // 1 MiB
	defaultMaxOutput  = 1 << 20
	maxRequestLineLen = 64 << 20 // room for large write_file payloads
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-version" || os.Args[1] == "--version") {
		fmt.Printf("rdev-agent proto=%d %s/%s\n", proto.Version, runtime.GOOS, runtime.GOARCH)
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

	state, err := stateDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "rdev-agent: %v\n", err)
		os.Exit(1)
	}

	in := bufio.NewReaderSize(os.Stdin, 1<<20)
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	for {
		line, err := readLine(in)
		if err != nil {
			return // EOF or transport error: the host closed the pipe.
		}
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var req proto.Request
		if err := json.Unmarshal(line, &req); err != nil {
			writeResp(out, &proto.Response{OK: false, Err: "malformed request: " + err.Error()})
			continue
		}
		writeResp(out, handle(&req, state))
	}
}

// readLine reads one newline-terminated record, growing past bufio's buffer so
// large write_file payloads are not rejected.
func readLine(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return nil, err
		}
		buf = append(buf, chunk...)
		if len(buf) > maxRequestLineLen {
			return nil, errors.New("request too large")
		}
		if !isPrefix {
			return buf, nil
		}
	}
}

func writeResp(out *bufio.Writer, resp *proto.Response) {
	b, err := json.Marshal(resp)
	if err != nil {
		b, _ = json.Marshal(&proto.Response{ID: resp.ID, OK: false, Err: "marshal failed: " + err.Error()})
	}
	out.Write(b)
	out.WriteByte('\n')
	out.Flush() // flush per reply: the host blocks waiting for this line
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
	default:
		// Job ops are dispatched by doJob, which owns the list of names it
		// handles. Routing anything unrecognized there rather than duplicating
		// the set here means adding a job op cannot be half-wired.
		if !isJobOp(req.Op) {
			err = fmt.Errorf("unknown op %q", req.Op)
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
		Version: proto.Version,
		Binary:  bin,
		Home:    home,
		OS:      runtime.GOOS,
		Arch:    runtime.GOARCH,
		PID:     os.Getpid(),
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
}

func (w *capWriter) Write(p []byte) (int, error) {
	w.total += int64(len(p))
	if room := w.cap - len(w.buf); room > 0 {
		if len(p) <= room {
			w.buf = append(w.buf, p...)
		} else {
			w.buf = append(w.buf, p[:room]...)
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
	cmd, err := buildCmd(p)
	if err != nil {
		return nil, err
	}

	limit := p.MaxOutputBytes
	if limit <= 0 {
		limit = defaultMaxOutput
	}
	stdout := &capWriter{cap: limit}
	stderr := &capWriter{cap: limit}
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
	if p.TimeoutSec > 0 {
		select {
		case err = <-done:
		case <-time.After(time.Duration(p.TimeoutSec) * time.Second):
			timedOut = true
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done // reap
		}
	} else {
		err = <-done
	}

	res := &proto.ExecResult{
		Stdout:      stdout.text(),
		Stderr:      stderr.text(),
		StdoutBytes: stdout.total,
		StderrBytes: stderr.total,
		Truncated:   stdout.truncated() || stderr.truncated(),
		TimedOut:    timedOut,
		DurationMS:  time.Since(start).Milliseconds(),
	}
	if timedOut {
		res.ExitCode = -1
		return res, nil
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

func doRead(p *proto.ReadParams) (*proto.ReadResult, error) {
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
	if limit <= 0 {
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

	res := &proto.ReadResult{Size: info.Size(), EOF: p.Offset+int64(n) >= info.Size()}
	// Base64 anything that is not valid UTF-8 so binary content survives the
	// JSON round-trip instead of being mangled into replacement runes.
	if isPrintableUTF8(data) {
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

func readFull(f *os.File, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := f.Read(buf[total:])
		total += n
		if err != nil {
			if err.Error() == "EOF" {
				return total, nil
			}
			if n == 0 {
				return total, nil
			}
		}
		if n == 0 {
			return total, nil
		}
	}
	return total, nil
}

func isPrintableUTF8(b []byte) bool {
	for _, c := range b {
		if c == 0 {
			return false
		}
	}
	return true
}

func stateDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".cache", "rdev")
	if err := os.MkdirAll(filepath.Join(dir, "jobs"), 0o755); err != nil {
		return "", err
	}
	return dir, nil
}
