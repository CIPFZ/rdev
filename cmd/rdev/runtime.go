package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// Process-wide CLI switches. RDEV_DEBUG=1 or a leading -v traces each phase to
// stderr; RDEV_QUIET=1 suppresses advisory notices (never errors or results).
var (
	cliDebug = envTrue("RDEV_DEBUG")
	cliQuiet = envTrue("RDEV_QUIET")
)

func envTrue(name string) bool {
	switch strings.ToLower(os.Getenv(name)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// stripGlobalFlags removes leading process-wide flags so every subcommand keeps
// its own strict flag schema.
func stripGlobalFlags(args []string) []string {
	for len(args) > 1 {
		switch args[1] {
		case "-v", "--verbose", "-verbose":
			cliDebug = true
		default:
			return args
		}
		args = append(args[:1:1], args[2:]...)
	}
	return args
}

// notice prints an advisory line unless RDEV_QUIET is set.
func notice(format string, args ...any) {
	if cliQuiet {
		return
	}
	fmt.Fprintf(os.Stderr, "rdev: "+format+"\n", args...)
}

// phaseTracker remembers the latest lifecycle phase of this invocation, so a
// deadline or heartbeat can say where the command is, and echoes every phase
// with its elapsed time when debugging.
type phaseTracker struct {
	start time.Time
	mu    sync.Mutex
	phase string
	out   io.Writer
}

func newPhaseTracker() *phaseTracker {
	return &phaseTracker{start: time.Now(), phase: "starting", out: os.Stderr}
}

func (p *phaseTracker) trace(phase string) {
	p.mu.Lock()
	p.phase = phase
	p.mu.Unlock()
	if cliDebug {
		fmt.Fprintf(p.out, "rdev: debug +%.3fs %s\n", time.Since(p.start).Seconds(), phase)
	}
}

func (p *phaseTracker) current() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.phase
}

var phases = newPhaseTracker()

// clientDeadlineGrace is added to a remote timeout to form the local deadline.
// It covers ssh connect, a first-time agent upload and returning the result.
var clientDeadlineGrace = 45 * time.Second

// exitClientDeadline matches coreutils timeout(1).
const exitClientDeadline = 124

var errClientDeadline = errors.New("client deadline")

// clientDeadlineError reports that this process gave up locally. The remote
// timeout is enforced by the agent; this bounds everything around it.
type clientDeadlineError struct {
	budget time.Duration
	remote int
	phase  string
	cause  error
}

func (e *clientDeadlineError) Error() string {
	msg := fmt.Sprintf("client deadline exceeded after %s (remote timeout %ds + %s grace) during: %s; rerun with RDEV_DEBUG=1 for phase timings",
		e.budget, e.remote, clientDeadlineGrace, e.phase)
	if e.cause != nil && !errors.Is(e.cause, context.DeadlineExceeded) && !errors.Is(e.cause, errClientDeadline) {
		msg += "; last error: " + e.cause.Error()
	}
	return msg
}

// withClientDeadline bounds a whole invocation by the remote budget plus grace.
func withClientDeadline(ctx context.Context, remoteSec int) (context.Context, context.CancelFunc, func(error) error) {
	budget := time.Duration(remoteSec)*time.Second + clientDeadlineGrace
	dctx, cancel := context.WithTimeoutCause(ctx, budget, errClientDeadline)
	explain := func(err error) error {
		if err == nil || !errors.Is(context.Cause(dctx), errClientDeadline) {
			return err
		}
		return &clientDeadlineError{budget: budget, remote: remoteSec, phase: phases.current(), cause: err}
	}
	return dctx, cancel, explain
}

// startHeartbeat prints a progress line while a foreground command runs long,
// naming the current phase so a local stall is distinguishable from a slow
// remote command. The returned func stops it.
func startHeartbeat(first, every time.Duration) func() {
	if cliQuiet {
		return func() {}
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		timer := time.NewTimer(first)
		defer timer.Stop()
		for {
			select {
			case <-done:
				return
			case <-timer.C:
				phase, hint := phases.current(), ""
				if strings.HasPrefix(phase, "request sent") {
					hint = "; prefer `rdev job start` for long tasks"
				}
				notice("still running after %s (phase: %s)%s", time.Since(phases.start).Round(time.Second), phase, hint)
				timer.Reset(every)
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// stdinPolicy decides whether exec forwards local stdin.
//
// Forwarding needs EOF, and agent harnesses often hand a child an inherited
// pipe or socket that never closes. So only an explicit -stdin, a regular file
// or a pipe is read; sockets, terminals and devices are ignored unless asked.
type stdinPolicy int

const (
	stdinSkip stdinPolicy = iota
	stdinRead
	stdinReadPipe // read, but warn when the writer stays silent
)

func execStdinPolicy(force, never bool, mode os.FileMode, statErr error) (stdinPolicy, string) {
	switch {
	case never:
		return stdinSkip, "stdin ignored (-no-stdin)"
	case force:
		return stdinRead, "stdin forwarded (-stdin)"
	case statErr != nil:
		return stdinSkip, "stdin ignored (unavailable)"
	case mode.IsRegular():
		return stdinRead, "stdin forwarded (regular file)"
	case mode&os.ModeNamedPipe != 0:
		return stdinReadPipe, "stdin forwarded (pipe; waiting for EOF)"
	case mode&os.ModeSocket != 0:
		return stdinSkip, "stdin ignored (socket; pass -stdin to forward it)"
	default:
		return stdinSkip, "stdin ignored (terminal or device; pass -stdin to forward it)"
	}
}

// stdinQuietWarning is how long a pipe may stay silent before rdev says that it
// is still waiting for EOF.
const stdinQuietWarning = 2 * time.Second

// readStdinContext reads input to EOF, honouring ctx, and warns once if a pipe
// produces neither data nor EOF within stdinQuietWarning.
func readStdinContext(ctx context.Context, input io.Reader, warn bool) (string, error) {
	type result struct {
		body string
		err  error
	}
	progress := make(chan struct{}, 1)
	done := make(chan result, 1)
	go func() {
		body, err := readAllInput(&signalReader{r: input, progress: progress})
		done <- result{body, err}
	}()
	var quiet <-chan time.Time
	if warn {
		timer := time.NewTimer(stdinQuietWarning)
		defer timer.Stop()
		quiet = timer.C
	}
	for {
		select {
		case r := <-done:
			return r.body, r.err
		case <-progress:
			quiet = nil
		case <-quiet:
			notice("waiting for stdin EOF before sending the command; pass -no-stdin or </dev/null if no input is intended")
		case <-ctx.Done():
			return "", fmt.Errorf("read stdin: %w", context.Cause(ctx))
		}
	}
}

// signalReader reports the first byte read, so the quiet warning stays silent
// for a slow producer that is actually producing.
type signalReader struct {
	r        io.Reader
	progress chan struct{}
	once     sync.Once
}

func (s *signalReader) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if n > 0 || err != nil {
		s.once.Do(func() { s.progress <- struct{}{} })
	}
	return n, err
}

// waitBudget is the remote wait budget that job wait's local deadline extends.
func waitBudget(sec int) int {
	if sec == 0 {
		return proto.DefaultJobWaitSeconds
	}
	return sec
}

// jobSummary is the one-line human form of a job state, e.g.
// "job 2026...-7d3d38ca exited 0 after 6m1s".
func jobSummary(info *proto.JobInfo) string {
	if info == nil {
		return "job: no status returned"
	}
	line := fmt.Sprintf("job %s %s", info.ID, info.State)
	if info.State != proto.JobRunning {
		line = fmt.Sprintf("job %s %s, exit %d", info.ID, info.State, info.ExitCode)
	}
	started, err := time.Parse(time.RFC3339Nano, info.StartedAt)
	if err != nil {
		return line
	}
	ended := time.Now()
	if info.EndedAt != "" {
		if t, err := time.Parse(time.RFC3339Nano, info.EndedAt); err == nil {
			ended = t
		}
	}
	return fmt.Sprintf("%s after %s", line, ended.Sub(started).Round(time.Second))
}

// printWaitSummary writes job wait -q output: one summary line per job on
// stdout, so scripts get status without parsing JSON.
func printWaitSummary(info *proto.JobInfo, waited []*proto.WaitedJob) {
	if len(waited) == 0 {
		fmt.Println(jobSummary(info))
		return
	}
	for _, w := range waited {
		if w.Err != "" {
			fmt.Printf("job %s: %s\n", w.ID, w.Err)
			continue
		}
		fmt.Println(jobSummary(w.Info))
	}
}
