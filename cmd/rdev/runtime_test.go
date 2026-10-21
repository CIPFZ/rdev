package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestExecStdinPolicy(t *testing.T) {
	for _, test := range []struct {
		force, never bool
		mode         fs.FileMode
		statErr      error
		want         stdinPolicy
	}{
		{mode: 0, want: stdinRead},                                 // regular file
		{mode: fs.ModeNamedPipe, want: stdinReadPipe},              // shell pipe
		{mode: fs.ModeSocket, want: stdinSkip},                     // harness socket
		{mode: fs.ModeDevice | fs.ModeCharDevice, want: stdinSkip}, // terminal, /dev/null
		{mode: fs.ModeDir, want: stdinSkip},
		{statErr: errors.New("bad fd"), want: stdinSkip},
		{force: true, mode: fs.ModeSocket, want: stdinRead},
		{never: true, mode: 0, want: stdinSkip},
		{never: true, mode: fs.ModeNamedPipe, want: stdinSkip},
	} {
		got, why := execStdinPolicy(test.force, test.never, test.mode, test.statErr)
		if got != test.want || why == "" {
			t.Errorf("policy(force=%t never=%t mode=%v err=%v) = %v %q, want %v", test.force, test.never, test.mode, test.statErr, got, why, test.want)
		}
	}
}

func TestReadStdinContextStopsAtDeadline(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close() // held open: the writer never reaches EOF
	ctx, cancel := context.WithTimeoutCause(context.Background(), 200*time.Millisecond, errClientDeadline)
	defer cancel()
	started := time.Now()
	_, err = readStdinContext(ctx, r, true)
	if err == nil || !errors.Is(err, errClientDeadline) || !strings.Contains(err.Error(), "read stdin") {
		t.Fatalf("err = %v, want read stdin client deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("stdin read ignored its deadline for %s", elapsed)
	}
}

func TestReadStdinContextReadsToEOF(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	go func() {
		_, _ = w.Write([]byte("hello\n"))
		_ = w.Close()
	}()
	body, err := readStdinContext(t.Context(), r, true)
	if err != nil || body != "hello\n" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func TestClientDeadlineExplainsPhase(t *testing.T) {
	old := clientDeadlineGrace
	clientDeadlineGrace = 50 * time.Millisecond
	defer func() { clientDeadlineGrace = old }()
	ctx, cancel, explain := withClientDeadline(context.Background(), 0)
	defer cancel()
	phases.trace("ssh dial: probe")
	<-ctx.Done()
	var deadline *clientDeadlineError
	err := explain(ctx.Err())
	if !errors.As(err, &deadline) || !strings.Contains(err.Error(), "during: ssh dial: probe") {
		t.Fatalf("explain = %v", err)
	}
	// Once the deadline fired, whatever error the operation surfaced is
	// attributed to it so the exit code is stable.
	if other := explain(errors.New("unrelated")); !errors.As(other, &deadline) || !strings.Contains(other.Error(), "last error: unrelated") {
		t.Fatalf("expired context error not attributed: %v", other)
	}

	live, liveCancel, liveExplain := withClientDeadline(context.Background(), 60)
	defer liveCancel()
	_ = live
	plain := errors.New("plain")
	if got := liveExplain(plain); got != plain {
		t.Fatalf("live deadline rewrote %v to %v", plain, got)
	}
}

func TestStripGlobalFlags(t *testing.T) {
	oldDebug := cliDebug
	defer func() { cliDebug = oldDebug }()
	cliDebug = false
	got := stripGlobalFlags([]string{"rdev", "-v", "exec", "h", "-v", "--", "echo", "-v"})
	if strings.Join(got, " ") != "rdev exec h -v -- echo -v" || !cliDebug {
		t.Fatalf("got %q debug=%t", got, cliDebug)
	}
}

func TestJobSummary(t *testing.T) {
	got := jobSummary(&proto.JobInfo{ID: "j1", State: "exited", ExitCode: 0, StartedAt: "2026-10-22T10:00:00Z", EndedAt: "2026-10-22T10:06:01Z"})
	if got != "job j1 exited, exit 0 after 6m1s" {
		t.Fatalf("summary = %q", got)
	}
	if got := jobSummary(&proto.JobInfo{ID: "j2", State: proto.JobRunning, StartedAt: "bad"}); got != "job j2 running" {
		t.Fatalf("running summary = %q", got)
	}
}
