package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/CIPFZ/rdev/internal/proto"
)

// TestMain lets the test binary act as its own job supervisor.
//
// jobStart re-execs os.Executable() with -supervise, which in production is the
// agent binary. Under `go test` os.Executable() is the test binary, and a test
// binary never runs main(), so without this hook the supervisor would start no
// child and every job would look like it vanished.
func TestMain(m *testing.M) {
	if os.Getenv("RDEV_TEST_ORPHAN_SUPERVISOR") == "1" {
		// Start a supervisor and intentionally let this parent exit before the
		// metadata barrier is published. The supervisor must reclaim the
		// unpublished directory instead of leaving a partial job record.
		dir := os.Getenv("RDEV_TEST_ORPHAN_DIR")
		cmd := exec.Command(os.Args[0], superviseFlag, dir, "--", "sleep", "30")
		cmd.Env = make([]string, 0, len(os.Environ()))
		for _, env := range os.Environ() {
			if !strings.HasPrefix(env, "RDEV_TEST_ORPHAN_SUPERVISOR=") {
				cmd.Env = append(cmd.Env, env)
			}
		}
		cmd.Env = setEnvValue(cmd.Env, supervisorParentEnv, fmt.Sprint(os.Getpid()))
		if err := cmd.Start(); err != nil {
			os.Exit(2)
		}
		if err := os.WriteFile(os.Getenv("RDEV_TEST_ORPHAN_PID"), []byte(fmt.Sprint(cmd.Process.Pid)), 0o644); err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	}
	if len(os.Args) > 3 && os.Args[1] == superviseFlag && os.Args[3] == "--" {
		runSupervisor(os.Args[2], os.Args[4:])
		return // unreachable: runSupervisor exits
	}
	os.Exit(m.Run())
}

func TestSupervisorCrashBeforeMetadataCleansRecord(t *testing.T) {
	state := t.TempDir()
	jobs := filepath.Join(state, "jobs")
	if err := os.MkdirAll(jobs, 0o755); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(jobs, "unpublished")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	pidPath := filepath.Join(state, "supervisor.pid")
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		"RDEV_TEST_ORPHAN_SUPERVISOR=1",
		"RDEV_TEST_ORPHAN_DIR="+dir,
		"RDEV_TEST_ORPHAN_PID="+pidPath,
	)
	if err := cmd.Run(); err != nil {
		t.Fatalf("orphan-parent helper: %v", err)
	}
	pidBytes, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	var pid int
	if _, err := fmt.Sscan(string(pidBytes), &pid); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(dir); os.IsNotExist(err) && !processAlive(pid) {
			if _, err := os.Stat(lockPath(dir)); !os.IsNotExist(err) {
				t.Fatalf("unpublished job lock survived cleanup: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("supervisor %d or unpublished directory survived parent crash", pid)
}

func TestCapWriterTruncatesButCounts(t *testing.T) {
	w := &capWriter{cap: 10}
	w.Write([]byte("0123456789abcdef"))

	if got := string(w.buf); got != "0123456789" {
		t.Errorf("buf = %q, want first 10 bytes", got)
	}
	// The true size must survive truncation so a caller knows how much it did
	// not see.
	if w.total != 16 {
		t.Errorf("total = %d, want 16", w.total)
	}
	if !w.truncated() {
		t.Error("truncated() should report true")
	}
}

func TestCapWriterUnderCap(t *testing.T) {
	w := &capWriter{cap: 100}
	w.Write([]byte("short"))
	if w.truncated() {
		t.Error("truncated() should be false when under cap")
	}
}

// capWriter must report a full write even when dropping bytes: reporting a
// short write would make exec.Cmd fail with io.ErrShortWrite.
func TestCapWriterReportsFullWrite(t *testing.T) {
	w := &capWriter{cap: 2}
	n, err := w.Write([]byte("abcdef"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 6 {
		t.Errorf("Write() = %d, want 6 (full length)", n)
	}
}

func TestExecRunsArgvWithoutShellParsing(t *testing.T) {
	// The whole point of argv: metacharacters reach the program literally.
	tricky := `a b "c" $(echo hi) 'd' $HOME`
	res, err := doExec(&proto.ExecParams{Argv: []string{"echo", tricky}})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimRight(res.Stdout, "\n"); got != tricky {
		t.Errorf("stdout = %q, want the literal argument %q", got, tricky)
	}
}

func TestExecNonZeroExitIsData(t *testing.T) {
	res, err := doExec(&proto.ExecParams{Argv: []string{"sh", "-c", "exit 42"}})
	// A failing command is an answer, not a transport error.
	if err != nil {
		t.Fatalf("non-zero exit should not be an error: %v", err)
	}
	if res.ExitCode != 42 {
		t.Errorf("ExitCode = %d, want 42", res.ExitCode)
	}
}

func TestExecMissingBinaryErrors(t *testing.T) {
	// Failing to *start* a command is a real error, unlike a non-zero exit.
	if _, err := doExec(&proto.ExecParams{Argv: []string{"definitely-not-a-real-binary-xyz"}}); err == nil {
		t.Error("expected an error for a missing binary")
	}
}

func TestExecEmptyArgvRejected(t *testing.T) {
	if _, err := doExec(&proto.ExecParams{Argv: nil}); err == nil {
		t.Error("empty argv should be rejected")
	}
}

func TestExecTimeoutKills(t *testing.T) {
	res, err := doExec(&proto.ExecParams{
		Argv:       []string{"sleep", "10"},
		TimeoutSec: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut {
		t.Error("TimedOut should be true")
	}
	if res.DurationMS > 5000 {
		t.Errorf("DurationMS = %d, expected the kill to land near 1s", res.DurationMS)
	}
}

func TestExecEnvAndStdin(t *testing.T) {
	res, err := doExec(&proto.ExecParams{
		Argv: []string{"sh", "-c", "read line; echo \"$MYVAR:$line\""},
		Env:  map[string]string{"MYVAR": "hello"},
		// Stdin proves data can be passed without shell quoting.
		Stdin: "worldly\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(res.Stdout); got != "hello:worldly" {
		t.Errorf("stdout = %q, want hello:worldly", got)
	}
}

func TestExecCwd(t *testing.T) {
	dir := t.TempDir()
	res, err := doExec(&proto.ExecParams{Argv: []string{"pwd"}, Cwd: dir})
	if err != nil {
		t.Fatal(err)
	}
	// macOS reports /private/var for /var, so compare resolved paths.
	want, _ := filepath.EvalSymlinks(dir)
	got, _ := filepath.EvalSymlinks(strings.TrimSpace(res.Stdout))
	if got != want {
		t.Errorf("pwd = %q, want %q", got, want)
	}
}

func TestExecCwdMissingErrors(t *testing.T) {
	_, err := doExec(&proto.ExecParams{Argv: []string{"pwd"}, Cwd: "/no/such/dir/xyz"})
	if err == nil {
		t.Error("a missing cwd should error rather than silently using $HOME")
	}
}

func TestExecMaxOutputBytes(t *testing.T) {
	res, err := doExec(&proto.ExecParams{
		Argv:           []string{"sh", "-c", "printf 'x%.0s' $(seq 1 5000)"},
		MaxOutputBytes: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stdout) != 100 {
		t.Errorf("len(stdout) = %d, want 100", len(res.Stdout))
	}
	if !res.Truncated {
		t.Error("Truncated should be true")
	}
	if res.StdoutBytes != 5000 {
		t.Errorf("StdoutBytes = %d, want the true size 5000", res.StdoutBytes)
	}
}

func TestWriteThenReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deep", "file.txt")
	body := "line1\nline2 with \"quotes\"\n中文\n"

	// Parent directories are created, so callers do not need a separate mkdir.
	wres, err := doWrite(&proto.WriteParams{Path: path, Content: body})
	if err != nil {
		t.Fatal(err)
	}
	if wres.BytesWritten != len(body) {
		t.Errorf("BytesWritten = %d, want %d", wres.BytesWritten, len(body))
	}

	rres, err := doRead(&proto.ReadParams{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if rres.Content != body {
		t.Errorf("Content = %q, want %q", rres.Content, body)
	}
	if !rres.EOF {
		t.Error("EOF should be true for a full read")
	}
}

func TestWriteMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "script.sh")
	if _, err := doWrite(&proto.WriteParams{Path: path, Content: "#!/bin/sh\n", Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Errorf("mode = %o, want 755", info.Mode().Perm())
	}
}

func TestWriteAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	doWrite(&proto.WriteParams{Path: path, Content: "first\n"})
	doWrite(&proto.WriteParams{Path: path, Content: "second\n", Append: true})

	b, _ := os.ReadFile(path)
	if string(b) != "first\nsecond\n" {
		t.Errorf("content = %q, want both lines", string(b))
	}
}

func TestReadOffsetAndLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	os.WriteFile(path, []byte("0123456789"), 0o644)

	res, err := doRead(&proto.ReadParams{Path: path, Offset: 3, Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if res.Content != "3456" {
		t.Errorf("Content = %q, want 3456", res.Content)
	}
	if res.EOF {
		t.Error("EOF should be false when the slice stops short of the end")
	}
	if res.Size != 10 {
		t.Errorf("Size = %d, want the full file size 10", res.Size)
	}
}

func TestReadBinaryIsBase64(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bin")
	// A NUL byte would not survive a JSON string round-trip intact.
	os.WriteFile(path, []byte{0x00, 0x01, 0xff}, 0o644)

	res, err := doRead(&proto.ReadParams{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ContentB64 {
		t.Error("binary content should be base64-encoded")
	}
}

func TestReadDirectoryErrors(t *testing.T) {
	if _, err := doRead(&proto.ReadParams{Path: t.TempDir()}); err == nil {
		t.Error("reading a directory should error")
	}
}

func TestExpandHome(t *testing.T) {
	home, _ := os.UserHomeDir()
	tests := []struct{ in, want string }{
		{"~", home},
		{"~/nexus", filepath.Join(home, "nexus")},
		{"/abs/path", "/abs/path"},
		{"relative", "relative"},
		{"", ""},
		// A leading "~" only expands as a path segment, never mid-string.
		{"~user/x", "~user/x"},
	}
	for _, tt := range tests {
		if got := expandHome(tt.in); got != tt.want {
			t.Errorf("expandHome(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestJobLifecycle(t *testing.T) {
	state := t.TempDir()
	if err := os.MkdirAll(filepath.Join(state, "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Point the supervisor at the test binary; os.Executable() resolves to the
	// compiled test, which re-enters main() and handles -supervise.
	res, err := jobStart(&proto.JobParams{
		Label: "unit",
		Spec:  &proto.ExecParams{Argv: []string{"sh", "-c", "echo hello; exit 3"}},
	}, state)
	if err != nil {
		t.Fatal(err)
	}
	id := res.Info.ID
	if res.Info.State != proto.JobRunning {
		t.Errorf("initial state = %q, want running", res.Info.State)
	}

	// Poll rather than sleeping a fixed amount, so the test is not flaky on a
	// loaded machine.
	var info *proto.JobInfo
	for range 100 {
		info, err = jobStatus(id, state)
		if err != nil {
			t.Fatal(err)
		}
		if info.State != proto.JobRunning {
			break
		}
		waitABit()
	}
	if info.State != proto.JobExited {
		t.Fatalf("final state = %q, want exited", info.State)
	}
	if info.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", info.ExitCode)
	}

	logs, err := jobLogs(&proto.JobParams{ID: id, Stream: "stdout"}, state)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.Logs, "hello") {
		t.Errorf("logs = %q, want to contain hello", logs.Logs)
	}

	list, err := jobList(&proto.JobParams{}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.List) != 1 || list.List[0].ID != id {
		t.Errorf("jobList returned %d jobs, want 1 matching %s", len(list.List), id)
	}
}

func TestJobLogsGrepAndTail(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "j1")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "j1", Argv: []string{"x"}, PID: 1})
	os.WriteFile(filepath.Join(dir, "stdout"),
		[]byte("alpha\nbeta\nalpha2\ngamma\nalpha3\n"), 0o644)

	// Grep runs on the remote side so a huge log never crosses the wire.
	res, err := jobLogs(&proto.JobParams{ID: "j1", Grep: "alpha", TailLines: 2}, state)
	if err != nil {
		t.Fatal(err)
	}
	if res.Matched != 3 {
		t.Errorf("Matched = %d, want 3 (count before tail)", res.Matched)
	}
	lines := strings.Split(res.Logs, "\n")
	if len(lines) != 2 || lines[0] != "alpha2" || lines[1] != "alpha3" {
		t.Errorf("Logs = %q, want the last 2 matching lines", res.Logs)
	}
}

func TestJobLogsRejectsBadStream(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "j1")
	os.MkdirAll(dir, 0o755)
	if _, err := jobLogs(&proto.JobParams{ID: "j1", Stream: "bogus"}, state); err == nil {
		t.Error("an invalid stream name should error")
	}
}

func TestJobStatusUnknownID(t *testing.T) {
	if _, err := jobStatus("nope", t.TempDir()); err == nil {
		t.Error("an unknown job id should error")
	}
}

func TestJobIDsAreSortableAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for range 5 {
		id := newJobID()
		if seen[id] {
			t.Fatalf("duplicate job id %q", id)
		}
		seen[id] = true
	}
}

func TestBuildCmdLoginShellUsesPositionalArgs(t *testing.T) {
	cmd, err := buildCmd(&proto.ExecParams{
		Argv:       []string{"echo", "$(danger)"},
		LoginShell: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The trampoline must pass argv positionally: embedding it in the script
	// text would let the shell re-parse it.
	joined := strings.Join(cmd.Args, " ")
	if !strings.Contains(joined, `exec "$@"`) {
		t.Errorf("login shell should use an exec \"$@\" trampoline, got %v", cmd.Args)
	}
	last := cmd.Args[len(cmd.Args)-1]
	if last != "$(danger)" {
		t.Errorf("last arg = %q, want the literal $(danger)", last)
	}
}

// waitABit is the poll interval used by job tests. Kept short so tests stay
// fast, and called in a bounded loop so a hung job fails rather than hangs.
func waitABit() {
	time.Sleep(20 * time.Millisecond)
}

// A byte cap can land inside a multi-byte rune, which would put a replacement
// character in the reply. The tail must be dropped instead.
func TestCapWriterTrimsPartialRune(t *testing.T) {
	// "中" is 3 bytes; a cap of 4 splits the second character.
	w := &capWriter{cap: 4}
	w.Write([]byte("中中中"))

	got := w.text()
	if !utf8.ValidString(got) {
		t.Errorf("text() = %q, which is not valid UTF-8", got)
	}
	if got != "中" {
		t.Errorf("text() = %q, want just the first full rune", got)
	}
}

func TestCapWriterKeepsWholeRunes(t *testing.T) {
	// A cap on an exact rune boundary must not lose a character.
	w := &capWriter{cap: 6}
	w.Write([]byte("中中中"))
	if got := w.text(); got != "中中" {
		t.Errorf("text() = %q, want 中中", got)
	}
}

func TestCapWriterUntruncatedTextIsExact(t *testing.T) {
	w := &capWriter{cap: 100}
	w.Write([]byte("中文 ok"))
	if got := w.text(); got != "中文 ok" {
		t.Errorf("text() = %q, want the input unchanged", got)
	}
}

func TestCapWriterASCIITruncationUnaffected(t *testing.T) {
	w := &capWriter{cap: 5}
	w.Write([]byte("abcdefgh"))
	if got := w.text(); got != "abcde" {
		t.Errorf("text() = %q, want abcde", got)
	}
}

// A SIGKILLed supervisor leaves its child orphaned to init. Reporting that as
// "unknown" would hide work that is still running, so the child pid is probed.
func TestOrphanedChildReportsRunning(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "orphan")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	// A supervisor pid that is certainly dead, plus a live child.
	live := exec.Command("sleep", "30")
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	defer live.Process.Kill()

	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
		ID: "orphan", Argv: []string{"sleep", "30"}, PID: 999999,
	})
	writeJSON(filepath.Join(dir, "child.json"), map[string]any{"child_pid": live.Process.Pid})

	info, err := jobStatus("orphan", state)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != proto.JobRunning {
		t.Errorf("State = %q, want running (the orphaned child is alive)", info.State)
	}
	if !info.Orphaned {
		t.Error("Orphaned should be true so the caller knows no exit code is coming")
	}
	if info.ChildPID != live.Process.Pid {
		t.Errorf("ChildPID = %d, want %d", info.ChildPID, live.Process.Pid)
	}
}

func TestDeadJobWithNoChildIsUnknown(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "gone")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "gone", Argv: []string{"x"}, PID: 999999})

	info, err := jobStatus("gone", state)
	if err != nil {
		t.Fatal(err)
	}
	if info.State != proto.JobUnknown {
		t.Errorf("State = %q, want unknown when nothing is alive and no status was recorded", info.State)
	}
}

func TestJobStopKillsOrphanedChild(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "orphan2")
	os.MkdirAll(dir, 0o755)

	live := exec.Command("sleep", "30")
	if err := live.Start(); err != nil {
		t.Fatal(err)
	}
	pid := live.Process.Pid
	go live.Wait() // reap so the probe below sees a real death, not a zombie

	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "orphan2", Argv: []string{"sleep"}, PID: 999999})
	writeJSON(filepath.Join(dir, "child.json"), map[string]any{"child_pid": pid})

	// Stopping must reach the child even though the supervisor is long gone,
	// otherwise a caller can see the leak but not clean it up.
	if _, err := jobStop(&proto.JobParams{ID: "orphan2", Signal: "KILL"}, state); err != nil {
		t.Fatalf("jobStop should reach the orphaned child: %v", err)
	}

	for range 100 {
		if !processAlive(pid) {
			return
		}
		waitABit()
	}
	t.Errorf("child pid %d survived jobStop", pid)
}

func TestJobWaitBlocksUntilExit(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	res, err := jobStart(&proto.JobParams{
		Label: "wait-exit",
		Spec:  &proto.ExecParams{Argv: []string{"sh", "-c", "echo one; echo two; sleep 1; exit 6"}},
	}, state)
	if err != nil {
		t.Fatal(err)
	}
	id := res.Info.ID

	// One wait call replaces a caller-side polling loop.
	got, err := jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 30, TailOnExit: 2}, state)
	if err != nil {
		t.Fatal(err)
	}
	if got.TimedOut {
		t.Fatal("wait should not have timed out")
	}
	if got.Info.State != proto.JobExited {
		t.Errorf("State = %q, want exited", got.Info.State)
	}
	if got.Info.ExitCode != 6 {
		t.Errorf("ExitCode = %d, want 6", got.Info.ExitCode)
	}
	// tail_on_exit saves a follow-up logs call.
	if !strings.Contains(got.Logs, "two") {
		t.Errorf("Logs = %q, want the trailing output", got.Logs)
	}
	if got.WaitedMS <= 0 {
		t.Error("WaitedMS should be recorded")
	}
}

func TestJobWaitTimesOutWithoutAffectingJob(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	res, err := jobStart(&proto.JobParams{
		Spec: &proto.ExecParams{Argv: []string{"sleep", "30"}},
	}, state)
	if err != nil {
		t.Fatal(err)
	}
	id := res.Info.ID
	defer jobStop(&proto.JobParams{ID: id, Signal: "KILL"}, state)

	got, err := jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 1}, state)
	if err != nil {
		t.Fatal(err)
	}
	if !got.TimedOut {
		t.Error("TimedOut should be true when the budget expires")
	}
	// A timeout must leave the job alone: the caller may wait again.
	if got.Info.State != proto.JobRunning {
		t.Errorf("State = %q, want running (a timeout must not disturb the job)", got.Info.State)
	}
	if got.WaitedMS > 5000 {
		t.Errorf("WaitedMS = %d, expected the wait to end near 1s", got.WaitedMS)
	}
}

func TestJobWaitReturnsImmediatelyForFinishedJob(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	res, _ := jobStart(&proto.JobParams{
		Spec: &proto.ExecParams{Argv: []string{"true"}},
	}, state)
	id := res.Info.ID

	// Let it finish, then confirm waiting on a done job does not block.
	jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 30}, state)

	got, err := jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 30}, state)
	if err != nil {
		t.Fatal(err)
	}
	if got.TimedOut {
		t.Error("waiting on an already-finished job should return at once")
	}
	if got.WaitedMS > 1000 {
		t.Errorf("WaitedMS = %d, want a near-instant return", got.WaitedMS)
	}
}

func TestJobWaitRejectsBudgetAboveHardLimit(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	// Callers may lower budgets but cannot silently raise them past the absolute
	// ceiling. Validation happens before job lookup, so no detached child is
	// needed (and the test cannot race TempDir cleanup with its supervisor).
	if _, err := jobWait(&proto.JobParams{ID: "unused", WaitTimeoutSec: 999999}, state); err == nil {
		t.Fatal("wait budget above the hard limit was accepted")
	}
}

func TestJobWaitUnknownID(t *testing.T) {
	if _, err := jobWait(&proto.JobParams{ID: "nope"}, t.TempDir()); err == nil {
		t.Error("an unknown job id should error")
	}
}

func TestReadTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	os.WriteFile(path, []byte("a\nb\nc\nd\ne\n"), 0o644)

	got, err := readTail(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got != "d\ne" {
		t.Errorf("readTail() = %q, want %q", got, "d\ne")
	}
}

func TestReadTailFewerLinesThanRequested(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	os.WriteFile(path, []byte("only\n"), 0o644)

	got, err := readTail(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got != "only" {
		t.Errorf("readTail() = %q, want %q", got, "only")
	}
}

// Guards against the dispatcher and doJob drifting apart: job_wait was once
// implemented in doJob but never routed to it, so it reported "unknown op".
func TestEveryJobOpIsRouted(t *testing.T) {
	ops := []string{
		proto.OpJobStart, proto.OpJobList, proto.OpJobStatus,
		proto.OpJobLogs, proto.OpJobStop, proto.OpJobWait, proto.OpJobRm,
	}
	for _, op := range ops {
		if !isJobOp(op) {
			t.Errorf("op %q is not routed to doJob", op)
		}
	}
}

func TestHandleRejectsUnknownOp(t *testing.T) {
	resp := handle(&proto.Request{Op: "not_a_real_op"}, t.TempDir())
	if resp.OK {
		t.Error("an unknown op should not succeed")
	}
	if !strings.Contains(resp.Err, "unknown op") {
		t.Errorf("Err = %q, want it to mention an unknown op", resp.Err)
	}
}

// Exercises the dispatcher rather than doJob directly, which is the layer that
// was broken.
func TestHandleRoutesJobWait(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	resp := handle(&proto.Request{
		Op:  proto.OpJobWait,
		Job: &proto.JobParams{ID: "missing-job"},
	}, state)

	// A missing job is the expected failure here; "unknown op" would mean the
	// dispatcher never reached doJob.
	if strings.Contains(resp.Err, "unknown op") {
		t.Errorf("job_wait was not routed: %q", resp.Err)
	}
}

// A caller polling with next_offset can hold an offset from before the log was
// rotated or truncated. That used to compute a negative slice length and panic,
// taking the whole agent -- and every job's only observer -- down with it.
func TestJobLogsStaleOffsetPastEOF(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "j1")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "j1", Argv: []string{"x"}, PID: 1})
	os.WriteFile(filepath.Join(dir, "stdout"), []byte("hello\n"), 0o644)

	res, err := jobLogs(&proto.JobParams{ID: "j1", SinceOffset: 9999}, state)
	if err != nil {
		t.Fatalf("a stale offset should read as empty, not fail: %v", err)
	}
	if res.Logs != "" {
		t.Errorf("Logs = %q, want empty: there is nothing past EOF", res.Logs)
	}
	// The clamped offset lets the next poll resume from a real position.
	if res.NextOffset != 6 {
		t.Errorf("NextOffset = %d, want 6 (clamped to the file size)", res.NextOffset)
	}
}

func TestJobLogsNegativeOffsetIsInvalidRequest(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "j1")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "j1", Argv: []string{"x"}, PID: 1})
	os.WriteFile(filepath.Join(dir, "stdout"), []byte("a\nb\n"), 0o644)

	_, err := jobLogs(&proto.JobParams{ID: "j1", SinceOffset: -5}, state)
	var typed *agentError
	if !errors.As(err, &typed) || typed.kind != agentInvalid {
		t.Fatalf("negative offset error = %v, want typed invalid request", err)
	}
}

// Non-UTF-8 bytes with no NUL used to be sent as a JSON string, where
// encoding/json silently rewrites them to U+FFFD. base64 is what keeps the
// content intact.
func TestReadInvalidUTF8IsBase64(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "latin1.txt")
	// Latin-1 "éè" plus 0xFF: invalid UTF-8, but contains no NUL byte.
	os.WriteFile(path, []byte{0xE9, 0xE8, 0xFF, 'a', 'b'}, 0o644)

	res, err := doRead(&proto.ReadParams{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ContentB64 {
		t.Fatalf("ContentB64 = false for invalid UTF-8; content %q would be corrupted by JSON", res.Content)
	}
	decoded, err := base64.StdEncoding.DecodeString(res.Content)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}
	if !bytes.Equal(decoded, []byte{0xE9, 0xE8, 0xFF, 'a', 'b'}) {
		t.Errorf("decoded = %x, want the original bytes byte-for-byte", decoded)
	}
}

func TestReadValidUTF8StaysText(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "utf8.txt")
	os.WriteFile(path, []byte("中文 ok"), 0o644)

	res, err := doRead(&proto.ReadParams{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if res.ContentB64 {
		t.Error("valid UTF-8 should stay readable text, not be base64-encoded")
	}
	if res.Content != "中文 ok" {
		t.Errorf("Content = %q, want the original text", res.Content)
	}
}

// A read whose limit lands mid-rune yields invalid UTF-8, which must be
// base64'd rather than corrupted. This is a different path from capWriter's
// trimming: doRead reports exact bytes at an offset, so it cannot drop the tail.
func TestReadPartialRuneIsBase64(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cut.txt")
	os.WriteFile(path, []byte("中文"), 0o644)

	// 4 bytes cuts the second 3-byte rune in half.
	res, err := doRead(&proto.ReadParams{Path: path, Limit: 4})
	if err != nil {
		t.Fatal(err)
	}
	if !res.ContentB64 {
		t.Errorf("a read cut mid-rune should be base64, got text %q", res.Content)
	}
}

// One bad request must not take down the serve loop: a running job's only
// observer is the agent process.
func TestHandleSafelyContainsPanic(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "j1")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "j1", Argv: []string{"x"}, PID: 1})
	os.WriteFile(filepath.Join(dir, "stdout"), []byte("x\n"), 0o644)

	// Now fixed, so this exercises the guard without relying on a live panic.
	resp := handleSafely(&proto.Request{
		Op:  proto.OpJobLogs,
		Job: &proto.JobParams{ID: "j1", SinceOffset: 1 << 40},
	}, state)
	if resp == nil {
		t.Fatal("handleSafely returned nil")
	}
	if !resp.OK {
		t.Errorf("a clamped offset should succeed, got Err = %q", resp.Err)
	}
}

func TestIsJSONSafeText(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want bool
	}{
		{"ascii", []byte("hello"), true},
		{"utf8", []byte("中文"), true},
		{"empty", nil, true},
		{"nul byte", []byte{'a', 0, 'b'}, false},
		{"latin1", []byte{0xE9, 0xE8}, false},
		{"partial rune", []byte("中")[:2], false},
	}
	for _, c := range cases {
		if got := isJSONSafeText(c.in); got != c.want {
			t.Errorf("%s: isJSONSafeText(%x) = %v, want %v", c.name, c.in, got, c.want)
		}
	}
}

// The host passes -state so a custom RemoteDir cannot leave the two sides
// reading different job directories.

func TestStateDirCustomAndDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	got, err := stateDir("")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(home, ".cache", "rdev"); got != want {
		t.Errorf("default = %q, want %q", got, want)
	}

	got, err = stateDir("~/custom/rdev")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(home, "custom", "rdev"); got != want {
		t.Errorf("tilde = %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(got, "jobs")); err != nil {
		t.Errorf("jobs dir not created: %v", err)
	}

	abs := filepath.Join(t.TempDir(), "abs")
	got, err = stateDir(abs)
	if err != nil {
		t.Fatal(err)
	}
	if got != abs {
		t.Errorf("abs = %q, want %q", got, abs)
	}

	// A relative dir is what hosts.json holds when written as ".cache/rdev".
	got, err = stateDir("rel/dir")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(home, "rel", "dir"); got != want {
		t.Errorf("relative = %q, want %q", got, want)
	}
}

// Job logs are unbounded, so without a reclaim path the state dir grows for the
// life of the machine.
func TestJobRmByID(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "j1")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
		ID: "j1", Argv: []string{"x"}, PID: 999999,
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	})
	writeJSON(filepath.Join(dir, "status.json"), map[string]any{
		"exit_code": 0, "ended_at": time.Now().UTC().Format(time.RFC3339),
	})
	os.WriteFile(filepath.Join(dir, "stdout"), []byte("0123456789"), 0o644)

	res, err := jobRm(&proto.JobParams{ID: "j1"}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 1 || res.Removed[0] != "j1" {
		t.Errorf("Removed = %v, want [j1]", res.Removed)
	}
	if res.FreedBytes < 10 {
		t.Errorf("FreedBytes = %d, want at least the 10-byte log", res.FreedBytes)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Error("job directory should be gone")
	}
}

// Removing a live job's records would leave the process running with no way to
// observe or stop it, which is worse than the disk usage.
func TestJobRmSkipsRunningJob(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "live")
	os.MkdirAll(dir, 0o755)
	// os.Getpid() is certainly alive, so this job reads as running.
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
		ID: "live", Argv: []string{"sleep"}, PID: os.Getpid(),
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	})

	res, err := jobRm(&proto.JobParams{ID: "live"}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 0 {
		t.Errorf("Removed = %v, want nothing: the job is running", res.Removed)
	}
	if len(res.Skipped) != 1 || res.Skipped[0] != "live" {
		t.Errorf("Skipped = %v, want [live]", res.Skipped)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Error("a running job's records must survive")
	}
}

func TestJobRmKeepLast(t *testing.T) {
	state := t.TempDir()
	// Distinct StartedAt values so "newest" is well defined.
	for i, id := range []string{"old1", "old2", "new1", "new2"} {
		dir := filepath.Join(state, "jobs", id)
		os.MkdirAll(dir, 0o755)
		writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
			ID: id, Argv: []string{"x"}, PID: 999999,
			StartedAt: time.Date(2020, 1, 1+i, 0, 0, 0, 0, time.UTC).Format(time.RFC3339),
		})
		writeJSON(filepath.Join(dir, "status.json"), map[string]any{"exit_code": 0})
	}

	res, err := jobRm(&proto.JobParams{KeepLast: 2}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 2 {
		t.Fatalf("Removed = %v, want the 2 oldest", res.Removed)
	}
	for _, id := range res.Removed {
		if id == "new1" || id == "new2" {
			t.Errorf("removed %s, which is among the newest 2", id)
		}
	}
	for _, keep := range []string{"new1", "new2"} {
		if _, err := os.Stat(filepath.Join(state, "jobs", keep)); err != nil {
			t.Errorf("%s should have been kept", keep)
		}
	}
}

// Both filters must agree, so the combination is conservative rather than
// surprising: a job inside the keep window stays even when it is old.
func TestJobRmFiltersAreConjunctive(t *testing.T) {
	state := t.TempDir()
	ancient := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	for _, id := range []string{"a", "b", "c"} {
		dir := filepath.Join(state, "jobs", id)
		os.MkdirAll(dir, 0o755)
		writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
			ID: id, Argv: []string{"x"}, PID: 999999, StartedAt: ancient,
		})
		writeJSON(filepath.Join(dir, "status.json"), map[string]any{
			"exit_code": 0, "ended_at": ancient,
		})
	}

	// All three are ancient, but keep_last=3 protects every one of them.
	res, err := jobRm(&proto.JobParams{KeepLast: 3, OlderThanSec: 60}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 0 {
		t.Errorf("Removed = %v, want nothing: keep_last covers all of them", res.Removed)
	}
}

func TestJobRmOlderThanKeepsRecent(t *testing.T) {
	state := t.TempDir()
	now := time.Now().UTC()
	mk := func(id string, ended time.Time) {
		dir := filepath.Join(state, "jobs", id)
		os.MkdirAll(dir, 0o755)
		writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
			ID: id, Argv: []string{"x"}, PID: 999999,
			StartedAt: ended.Format(time.RFC3339),
		})
		writeJSON(filepath.Join(dir, "status.json"), map[string]any{
			"exit_code": 0, "ended_at": ended.Format(time.RFC3339),
		})
	}
	mk("stale", now.Add(-2*time.Hour))
	mk("recent", now.Add(-1*time.Minute))

	res, err := jobRm(&proto.JobParams{OlderThanSec: 3600}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 1 || res.Removed[0] != "stale" {
		t.Errorf("Removed = %v, want [stale] only", res.Removed)
	}
}

func TestJobRmRequiresAFilter(t *testing.T) {
	if _, err := jobRm(&proto.JobParams{}, t.TempDir()); err == nil {
		t.Error("job_rm with no id and no filters should error rather than wipe everything")
	}
}

// A missing EndedAt (supervisor died without recording status) falls back to
// StartedAt rather than being treated as age zero and kept forever.
func TestJobRmFallsBackToStartedAt(t *testing.T) {
	state := t.TempDir()
	dir := filepath.Join(state, "jobs", "noend")
	os.MkdirAll(dir, 0o755)
	writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
		ID: "noend", Argv: []string{"x"}, PID: 999999,
		StartedAt: time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339),
	})

	res, err := jobRm(&proto.JobParams{OlderThanSec: 3600}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 1 {
		t.Errorf("Removed = %v, want the job aged by StartedAt", res.Removed)
	}
}

func TestListReturnsStructuredEntries(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file.txt"), []byte("hello"), 0o644)
	os.MkdirAll(filepath.Join(dir, "subdir"), 0o755)
	os.Symlink(filepath.Join(dir, "file.txt"), filepath.Join(dir, "link"))
	// A name that would make `ls` output ambiguous to parse.
	os.WriteFile(filepath.Join(dir, "od d na me.txt"), []byte("x"), 0o644)

	res, err := doList(&proto.ListParams{Path: dir})
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 4 {
		t.Errorf("Total = %d, want 4", res.Total)
	}

	byName := map[string]proto.DirEntry{}
	for _, e := range res.Entries {
		byName[e.Name] = e
	}
	if e := byName["file.txt"]; e.Size != 5 || e.IsDir {
		t.Errorf("file.txt = %+v, want size 5 and not a dir", e)
	}
	if e := byName["subdir"]; !e.IsDir {
		t.Errorf("subdir = %+v, want IsDir", e)
	}
	if e := byName["link"]; !e.Symlink {
		t.Errorf("link = %+v, want Symlink", e)
	}
	if _, ok := byName["od d na me.txt"]; !ok {
		t.Error("a name with spaces should survive as one entry")
	}
	if e := byName["file.txt"]; e.ModTime == "" || e.Mode == "" {
		t.Errorf("file.txt missing ModTime/Mode: %+v", e)
	}
}

func TestListLimitReportsTruncation(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644)
	}
	res, err := doList(&proto.ListParams{Path: dir, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 2 {
		t.Errorf("got %d entries, want 2", len(res.Entries))
	}
	if !res.Truncated {
		t.Error("Truncated should be set when the limit cut the listing")
	}
	// Total reports the real count so the caller knows what it did not see.
	if res.Total != 5 {
		t.Errorf("Total = %d, want 5", res.Total)
	}
}

func TestListMissingDirErrors(t *testing.T) {
	if _, err := doList(&proto.ListParams{Path: filepath.Join(t.TempDir(), "nope")}); err == nil {
		t.Error("listing a missing directory should error")
	}
}

func TestListRoutedThroughHandle(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "f"), []byte("x"), 0o644)

	resp := handle(&proto.Request{Op: proto.OpList, List: &proto.ListParams{Path: dir}}, t.TempDir())
	if !resp.OK {
		t.Fatalf("list not routed: %q", resp.Err)
	}
	if resp.List == nil || len(resp.List.Entries) != 1 {
		t.Errorf("List = %+v, want 1 entry", resp.List)
	}
}

func TestJobRmRoutedThroughHandle(t *testing.T) {
	resp := handle(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: "missing"}}, t.TempDir())
	if strings.Contains(resp.Err, "unknown op") {
		t.Errorf("job_rm was not routed: %q", resp.Err)
	}
}

// Replies are written from many goroutines onto one pipe, so the writer must
// serialize them: interleaved partial lines would corrupt the framing the host
// relies on to match replies to requests.
func TestRespWriterSerializesConcurrentReplies(t *testing.T) {
	var buf lockedBuffer
	w := newRespWriter(&buf, nil)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w.write(&proto.Response{
				ID: fmt.Sprint(i), OK: true,
				// A payload long enough that an unguarded write would interleave.
				Exec: &proto.ExecResult{Stdout: strings.Repeat("x", 4096)},
			})
		}(i)
	}
	wg.Wait()
	w.flush()

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != n {
		t.Fatalf("got %d lines, want %d: replies were interleaved", len(lines), n)
	}
	seen := map[string]bool{}
	for _, line := range lines {
		var resp proto.Response
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			t.Fatalf("corrupt line: %v", err)
		}
		if seen[resp.ID] {
			t.Errorf("duplicate reply for ID %s", resp.ID)
		}
		seen[resp.ID] = true
	}
}

// lockedBuffer is a concurrency-safe sink for the writer test.
type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Handlers run concurrently, and they share the job state directory, so parallel
// job_start calls must not collide on IDs or clobber each other's records.
func TestConcurrentJobStartsAreIsolated(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	const n = 8
	var wg sync.WaitGroup
	ids := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp := handleSafely(&proto.Request{
				Op: proto.OpJobStart,
				Job: &proto.JobParams{
					Spec:  &proto.ExecParams{Argv: []string{"sh", "-c", fmt.Sprintf("echo job-%d", i)}},
					Label: fmt.Sprint(i),
				},
			}, state)
			if !resp.OK {
				t.Errorf("job %d failed: %s", i, resp.Err)
				return
			}
			ids <- resp.Job.Info.ID
		}(i)
	}
	wg.Wait()
	close(ids)

	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate job ID %q: concurrent starts collided", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Errorf("got %d unique jobs, want %d", len(seen), n)
	}

	// Wait for the supervisors to finish writing before the test's TempDir
	// cleanup runs; a job still creating status.json would fail the removal.
	for id := range seen {
		jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 10}, state)
	}
}

// The post-disconnect drain must be bounded. job_wait can be budgeted for an
// hour, so waiting for every handler would keep an agent alive long after its
// host went away -- one lingering process per dropped ssh session.
//
// Abandoning those handlers is safe: the pipe is closed so replies have nowhere
// to go, and jobs are detached with setsid, so no work depends on this process.
func TestShutdownDrainIsBounded(t *testing.T) {
	if shutdownDrainTimeout > 5*time.Second {
		t.Errorf("shutdownDrainTimeout = %v, too long: a dropped connection would strand an agent", shutdownDrainTimeout)
	}
	if shutdownDrainTimeout < 500*time.Millisecond {
		t.Errorf("shutdownDrainTimeout = %v, too short for an almost-finished handler to flush", shutdownDrainTimeout)
	}
	// A single wait budget can far exceed the drain, which is the case that makes
	// the bound necessary rather than cosmetic.
	if time.Duration(maxWaitSec)*time.Second <= shutdownDrainTimeout {
		t.Error("maxWaitSec no longer exceeds the drain timeout; the bound may be pointless")
	}
}

// The fast path must return exactly what the scanning path would.
func TestJobLogsFastPathMatchesScan(t *testing.T) {
	cases := []struct {
		name string
		body string
		tail int
	}{
		{"trailing newline", "a\nb\nc\n", 2},
		{"no trailing newline", "a\nb\nc", 2},
		{"single line", "only\n", 5},
		{"empty file", "", 3},
		{"tail exceeds lines", "x\ny\n", 100},
		{"blank lines", "a\n\n\nb\n", 4},
		{"crlf", "a\r\nb\r\n", 2},
		{"tail 1", "1\n2\n3\n4\n", 1},
		{"utf8", "中文\nline2\n", 2},
	}
	for _, c := range cases {
		state := t.TempDir()
		dir := filepath.Join(state, "jobs", "j")
		os.MkdirAll(dir, 0o755)
		writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{ID: "j", Argv: []string{"x"}, PID: 1})
		os.WriteFile(filepath.Join(dir, "stdout"), []byte(c.body), 0o644)

		fast, err := jobLogs(&proto.JobParams{ID: "j", TailLines: c.tail}, state)
		if err != nil {
			t.Fatalf("%s: fast: %v", c.name, err)
		}
		// Compare against the previous implementation, kept below as an oracle.
		want := refTail(c.body, c.tail)
		if fast.Logs != want {
			t.Errorf("%s: fast path = %q, reference = %q", c.name, fast.Logs, want)
		}
		if fast.NextOffset != int64(len(c.body)) {
			t.Errorf("%s: NextOffset = %d, want %d", c.name, fast.NextOffset, len(c.body))
		}
	}
}

// refTail is the old implementation, used as the oracle.
func refTail(body string, n int) string {
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// A line longer than one read chunk must still be tailed correctly.
func TestReadTailHugeLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	huge := strings.Repeat("H", 300<<10) // 300 KB, spans several 64 KB chunks
	os.WriteFile(path, []byte("first\n"+huge+"\nlast\n"), 0o644)

	got, err := readTail(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(got, "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if lines[1] != "last" {
		t.Errorf("last line = %q, want last", lines[1])
	}
	if len(lines[0]) != len(huge) {
		t.Errorf("huge line truncated: got %d bytes, want %d", len(lines[0]), len(huge))
	}
}

// The scan cap must not corrupt output when the tail exceeds it.
func TestReadTailBeyondScanCap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	var sb strings.Builder
	for i := 0; i < 100; i++ {
		sb.WriteString(strings.Repeat(fmt.Sprintf("%d", i%10), 40<<10))
		sb.WriteByte('\n')
	}
	os.WriteFile(path, []byte(sb.String()), 0o644)

	got, err := readTail(path, 50)
	if err != nil {
		t.Fatal(err)
	}
	// Whatever we get back must be whole lines from the end, never a partial one.
	lines := strings.Split(got, "\n")
	last := lines[len(lines)-1]
	if len(last) != 40<<10 {
		t.Errorf("last line is %d bytes, want a whole 40KB line", len(last))
	}
	t.Logf("returned %d lines under a capped scan", len(lines))
}

// Waiting on N jobs used to cost N serial blocking calls, each re-sending the
// same context. One call now covers a batch under a shared deadline.
func TestJobWaitManyReturnsAllOutcomes(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	// Two jobs with different exit codes, so results cannot be confused.
	var ids []string
	for _, code := range []int{0, 3} {
		resp := handleSafely(&proto.Request{
			Op: proto.OpJobStart,
			Job: &proto.JobParams{
				Spec: &proto.ExecParams{Argv: []string{"sh", "-c", fmt.Sprintf("exit %d", code)}},
			},
		}, state)
		if !resp.OK {
			t.Fatalf("job_start: %s", resp.Err)
		}
		ids = append(ids, resp.Job.Info.ID)
	}

	res, err := jobWait(&proto.JobParams{IDs: ids, WaitTimeoutSec: 20}, state)
	if err != nil {
		t.Fatal(err)
	}
	if res.TimedOut {
		t.Error("TimedOut set although both jobs finish immediately")
	}
	if len(res.Waited) != 2 {
		t.Fatalf("got %d results, want one per requested id", len(res.Waited))
	}

	byID := map[string]*proto.WaitedJob{}
	for _, w := range res.Waited {
		byID[w.ID] = w
	}
	for i, want := range []int{0, 3} {
		w := byID[ids[i]]
		if w == nil {
			t.Fatalf("no result for %s", ids[i])
		}
		if w.Err != "" {
			t.Errorf("%s: unexpected err %q", ids[i], w.Err)
			continue
		}
		if w.Info.State != proto.JobExited {
			t.Errorf("%s: state = %q, want exited", ids[i], w.Info.State)
		}
		if w.Info.ExitCode != want {
			t.Errorf("%s: exit = %d, want %d", ids[i], w.Info.ExitCode, want)
		}
	}
}

// One unknown id must not fail the whole call: the other jobs still have useful
// answers, and a batch assembled from several places can easily carry a stale id.
func TestJobWaitManyReportsBadIDPerJob(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	resp := handleSafely(&proto.Request{
		Op:  proto.OpJobStart,
		Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"true"}}},
	}, state)
	good := resp.Job.Info.ID

	res, err := jobWait(&proto.JobParams{IDs: []string{good, "no-such-job"}, WaitTimeoutSec: 20}, state)
	if err != nil {
		t.Fatalf("a bad id should be reported per-job, not fail the call: %v", err)
	}
	if len(res.Waited) != 2 {
		t.Fatalf("got %d results, want 2", len(res.Waited))
	}
	for _, w := range res.Waited {
		switch w.ID {
		case good:
			if w.Err != "" {
				t.Errorf("good job reported err %q", w.Err)
			}
		case "no-such-job":
			if w.Err == "" {
				t.Error("unknown id should carry an err")
			}
		}
	}
}

// wait_any lets a caller react to the first finisher -- usually the first failure
// in a batch -- without waiting out the slowest job.
func TestJobWaitAnyReturnsBeforeSlowJob(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)

	start := func(argv ...string) string {
		resp := handleSafely(&proto.Request{
			Op:  proto.OpJobStart,
			Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: argv}},
		}, state)
		if !resp.OK {
			t.Fatalf("job_start: %s", resp.Err)
		}
		return resp.Job.Info.ID
	}
	fast := start("true")
	slow := start("sleep", "30")

	begin := time.Now()
	res, err := jobWait(&proto.JobParams{
		IDs: []string{slow, fast}, WaitAny: true, WaitTimeoutSec: 20,
	}, state)
	elapsed := time.Since(begin)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("waited %v; wait_any should return on the first finisher", elapsed)
	}
	if res.TimedOut {
		t.Error("TimedOut set although one job finished")
	}

	// Every requested job is still reported, the unfinished one as running.
	if len(res.Waited) != 2 {
		t.Fatalf("got %d results, want 2", len(res.Waited))
	}
	var sawExited, sawRunning bool
	for _, w := range res.Waited {
		switch w.Info.State {
		case proto.JobExited:
			sawExited = true
		case proto.JobRunning:
			sawRunning = true
		}
	}
	if !sawExited || !sawRunning {
		t.Errorf("want one exited and one running, got %+v, %+v", res.Waited[0].Info, res.Waited[1].Info)
	}

	jobStop(&proto.JobParams{ID: slow, Signal: "KILL"}, state)
}

// A duplicated id would otherwise be polled twice per round for no benefit.
func TestJobWaitManyDeduplicates(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)
	resp := handleSafely(&proto.Request{
		Op:  proto.OpJobStart,
		Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"true"}}},
	}, state)
	id := resp.Job.Info.ID

	res, err := jobWait(&proto.JobParams{IDs: []string{id, id, "", id}, WaitTimeoutSec: 20}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Waited) != 1 {
		t.Errorf("got %d results, want 1 after dedup", len(res.Waited))
	}
}

func TestJobWaitManyRejectsNoUsableIDs(t *testing.T) {
	if _, err := jobWait(&proto.JobParams{IDs: []string{"", ""}}, t.TempDir()); err == nil {
		t.Error("a list of empty ids should error rather than wait on nothing")
	}
}

// A still-running job at deadline is reported as running, with TimedOut set, and
// is left untouched so the caller can wait again.
func TestJobWaitManyTimesOutWithoutAffectingJobs(t *testing.T) {
	state := t.TempDir()
	os.MkdirAll(filepath.Join(state, "jobs"), 0o755)
	resp := handleSafely(&proto.Request{
		Op:  proto.OpJobStart,
		Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sleep", "30"}}},
	}, state)
	id := resp.Job.Info.ID

	res, err := jobWait(&proto.JobParams{IDs: []string{id}, WaitTimeoutSec: 1}, state)
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut {
		t.Error("TimedOut should be set when the budget expires")
	}
	if res.Waited[0].Info.State != proto.JobRunning {
		t.Errorf("state = %q, want running: the wait must not disturb the job", res.Waited[0].Info.State)
	}

	jobStop(&proto.JobParams{ID: id, Signal: "KILL"}, state)
}

// The limit bounds the returned records after the metadata-defined global order
// is known. Total still reports every directory seen.
func TestJobListLimitAndTotals(t *testing.T) {
	state := t.TempDir()
	root := filepath.Join(state, "jobs")
	os.MkdirAll(root, 0o755)
	for i := 0; i < 12; i++ {
		dir := filepath.Join(root, fmt.Sprintf("2026010%d-000000-%04x", i%10, i))
		os.MkdirAll(dir, 0o755)
		writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
			ID: filepath.Base(dir), Argv: []string{"x"}, PID: 999999,
			StartedAt: time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC).Format(time.RFC3339),
		})
	}

	res, err := jobList(&proto.JobParams{Limit: 5}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.List) != 5 {
		t.Errorf("got %d jobs, want 5", len(res.List))
	}
	if res.Total != 12 {
		t.Errorf("Total = %d, want 12 so a caller knows what it did not see", res.Total)
	}
	if !res.Truncated {
		t.Error("Truncated should be set when the limit hid jobs")
	}
	// Newest first.
	for i := 1; i < len(res.List); i++ {
		if res.List[i-1].StartedAt < res.List[i].StartedAt {
			t.Errorf("results are not newest-first at %d", i)
		}
	}
}

// Limit validation is an operation invariant, not a property of the current
// filesystem state. Drive the real handler across the jobs directory's full
// lifecycle so a missing directory cannot turn an invalid limit into success.
func TestJobListLimitValidatedBeforeJobsDirectoryAccess(t *testing.T) {
	state := t.TempDir()
	jobsRoot := filepath.Join(state, "jobs")
	type stateStep struct {
		name  string
		apply func() error
	}
	steps := []stateStep{
		{name: "empty_state", apply: func() error { return nil }},
		{name: "jobs_directory_exists", apply: func() error {
			return os.MkdirAll(jobsRoot, 0o755)
		}},
		{name: "jobs_directory_removed_again", apply: func() error {
			return os.RemoveAll(jobsRoot)
		}},
	}

	var wantLimitEnvelope string
	for _, step := range steps {
		if err := step.apply(); err != nil {
			t.Fatalf("prepare %s: %v", step.name, err)
		}
		t.Run(step.name, func(t *testing.T) {
			for _, limit := range []int{-1, 0, 1, 1000, 1001} {
				resp := handleSafely(&proto.Request{
					ID: "job-list-boundary", OperationID: "job-list-boundary-op",
					Op: proto.OpJobList, Job: &proto.JobParams{Limit: limit},
				}, state)
				if limit >= 0 && limit <= 1000 {
					if !resp.OK || resp.Error != nil || resp.Job == nil {
						t.Errorf("limit %d response = %+v, want successful empty list", limit, resp)
						continue
					}
					if len(resp.Job.List) != 0 || resp.Job.Total != 0 || resp.Job.Truncated {
						t.Errorf("limit %d job result = %+v, want empty and untruncated", limit, resp.Job)
					}
					continue
				}

				if resp.OK || resp.Error == nil || resp.Error.Code != proto.CodeLimitExceeded {
					t.Errorf("limit %d response = %+v, want structured %s", limit, resp, proto.CodeLimitExceeded)
					continue
				}
				if resp.Execution != proto.StateNotSent || resp.Job != nil {
					t.Errorf("limit %d response execution/job = %s/%+v, want not_sent/nil", limit, resp.Execution, resp.Job)
				}
				envelope, err := json.Marshal(resp.Error)
				if err != nil {
					t.Fatal(err)
				}
				if wantLimitEnvelope == "" {
					wantLimitEnvelope = string(envelope)
				} else if string(envelope) != wantLimitEnvelope {
					t.Errorf("limit %d envelope = %s, want identical %s", limit, envelope, wantLimitEnvelope)
				}
			}
		})
	}
}

// Directory names are only an operational convenience; StartedAt plus ID is the
// shared strict order for listing and keep_last. This fixture has 101 valid jobs
// in one timestamp-prefixed name bucket, with the lexically smallest ID carrying
// the newest nanosecond timestamp. Selecting 100 names before reading metadata
// drops that actual newest job. It also exercises legacy whole-second records,
// equal-nanosecond ID ties, an old outlier, a damaged record, and every limit
// boundary in one deterministic fixture.
func TestJobListLimitUsesGlobalMetadataOrderAndMatchesSweep(t *testing.T) {
	state := t.TempDir()
	root := filepath.Join(state, "jobs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}

	const validJobs = 101
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	ids := make([]string, validJobs)
	for i := range ids {
		id := fmt.Sprintf("20260828-120000-%08x", i)
		ids[i] = id
		startedAt := base.Format(time.RFC3339) // legacy whole-second record
		switch i {
		case 0:
			startedAt = base.Add(2 * time.Nanosecond).Format(time.RFC3339Nano)
		case 99, 100:
			startedAt = base.Add(time.Nanosecond).Format(time.RFC3339Nano)
		case 50:
			startedAt = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
		}
		dir := jobDir(state, id)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
			ID: id, Argv: []string{"true"}, PID: 999999, StartedAt: startedAt,
		}); err != nil {
			t.Fatal(err)
		}
		if err := writeJSON(filepath.Join(dir, "status.json"), map[string]any{
			"exit_code": 0, "ended_at": startedAt,
		}); err != nil {
			t.Fatal(err)
		}
	}

	badDir := filepath.Join(root, "000-corrupt")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "meta.json"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}

	list100, err := jobList(&proto.JobParams{Limit: 100}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(list100.List) != 100 || list100.Total != validJobs+1 || !list100.Truncated {
		t.Fatalf("limit 100 result: len=%d Total=%d Truncated=%v, want 100/%d/true",
			len(list100.List), list100.Total, list100.Truncated, validJobs+1)
	}
	if got := list100.List[0].ID; got != ids[0] {
		t.Fatalf("newest ID = %s, want lexically smallest but metadata-newest %s", got, ids[0])
	}
	if got := []string{list100.List[1].ID, list100.List[2].ID}; got[0] != ids[100] || got[1] != ids[99] {
		t.Fatalf("equal-nanosecond order = %v, want descending IDs [%s %s]", got, ids[100], ids[99])
	}
	listed := make(map[string]bool, len(list100.List))
	for _, info := range list100.List {
		listed[info.ID] = true
	}
	if listed[ids[50]] {
		t.Fatalf("old outlier %s displaced a newer record", ids[50])
	}

	defaultList, err := jobList(&proto.JobParams{Limit: 0}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(defaultList.List) != 100 {
		t.Fatalf("default limit returned %d jobs, want 100", len(defaultList.List))
	}
	for i := range defaultList.List {
		if defaultList.List[i].ID != list100.List[i].ID {
			t.Fatalf("default limit differs from explicit 100 at %d: %s != %s",
				i, defaultList.List[i].ID, list100.List[i].ID)
		}
	}
	large, err := jobList(&proto.JobParams{Limit: 1000}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(large.List) != validJobs || large.Total != validJobs+1 || large.Truncated {
		t.Fatalf("limit 1000 result: len=%d Total=%d Truncated=%v, want %d/%d/false",
			len(large.List), large.Total, large.Truncated, validJobs, validJobs+1)
	}
	for _, invalid := range []int{-1, 1001} {
		if _, err := jobList(&proto.JobParams{Limit: invalid}, state); err == nil {
			t.Errorf("limit %d unexpectedly succeeded", invalid)
		}
	}

	swept, err := jobRm(&proto.JobParams{KeepLast: 100}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(swept.Removed) != 1 || swept.Removed[0] != ids[50] {
		t.Fatalf("keep_last removed %v, want only old outlier %s", swept.Removed, ids[50])
	}
	if _, err := os.Stat(badDir); err != nil {
		t.Fatalf("damaged metadata directory should remain untouched: %v", err)
	}

	after, err := jobList(&proto.JobParams{Limit: 100}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.List) != 100 || after.Total != 101 || !after.Truncated {
		t.Fatalf("post-sweep result: len=%d Total=%d Truncated=%v, want 100/101/true",
			len(after.List), after.Total, after.Truncated)
	}
	for i, info := range after.List {
		if info.ID != list100.List[i].ID {
			t.Fatalf("list/sweep retained sets differ at %d: %s != %s", i, info.ID, list100.List[i].ID)
		}
	}
}

func TestJobListUnlimitedReportsNoTruncation(t *testing.T) {
	state := t.TempDir()
	root := filepath.Join(state, "jobs")
	os.MkdirAll(root, 0o755)
	for i := 0; i < 3; i++ {
		dir := filepath.Join(root, fmt.Sprintf("20260101-00000%d-aaaa", i))
		os.MkdirAll(dir, 0o755)
		writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
			ID: filepath.Base(dir), Argv: []string{"x"}, PID: 999999,
			StartedAt: "2026-01-01T00:00:00Z",
		})
	}
	res, err := jobList(&proto.JobParams{}, state)
	if err != nil {
		t.Fatal(err)
	}
	if res.Truncated {
		t.Error("Truncated set although everything fit under the default limit")
	}
	if res.Total != 3 || len(res.List) != 3 {
		t.Errorf("Total = %d, len = %d, want 3 and 3", res.Total, len(res.List))
	}
}

// A timed-out command must still return what it printed before the kill.
//
// This is what makes a bounded exec usable for watching progress: the output up to
// the deadline is the answer, so a caller does not have to switch to a job just to
// see whether anything happened. Discarding it would make a timeout indistinguishable
// from a command that produced nothing.
func TestExecTimeoutPreservesPartialOutput(t *testing.T) {
	res, err := doExec(&proto.ExecParams{
		Argv:       []string{"sh", "-c", "echo early; echo second >&2; sleep 30"},
		TimeoutSec: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut {
		t.Fatal("TimedOut should be set")
	}
	if !strings.Contains(res.Stdout, "early") {
		t.Errorf("stdout = %q, want the pre-kill output retained", res.Stdout)
	}
	if !strings.Contains(res.Stderr, "second") {
		t.Errorf("stderr = %q, want the pre-kill output retained", res.Stderr)
	}
	// ExitCode is meaningless for a killed process; TimedOut is the signal.
	if res.ExitCode != -1 {
		t.Errorf("ExitCode = %d, want -1 to mark it as unset", res.ExitCode)
	}
}

// Truncation accounting must stay correct when a timeout also fires, or a caller
// cannot tell how much output it did not see.
func TestExecTimeoutStillReportsTrueOutputSize(t *testing.T) {
	res, err := doExec(&proto.ExecParams{
		Argv:           []string{"sh", "-c", "printf 'x%.0s' $(seq 1 5000); sleep 30"},
		TimeoutSec:     1,
		MaxOutputBytes: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut || !res.Truncated {
		t.Errorf("TimedOut = %v, Truncated = %v, want both", res.TimedOut, res.Truncated)
	}
	if len(res.Stdout) != 100 {
		t.Errorf("len(stdout) = %d, want the 100-byte cap", len(res.Stdout))
	}
	if res.StdoutBytes != 5000 {
		t.Errorf("StdoutBytes = %d, want the true size 5000", res.StdoutBytes)
	}
}

// The timeout kill goes to the process group, so a command that backgrounds work
// does not leave orphans running on the remote after the call returns.
func TestExecTimeoutKillsGrandchildren(t *testing.T) {
	// The marker rides along as a shell variable so pgrep can find this test's
	// process without matching an unrelated sleep.
	const marker = "rdev_test_orphan_marker"
	res, err := doExec(&proto.ExecParams{
		// The inner sleep is a grandchild: killing only the direct child would
		// leave it running.
		Argv:       []string{"sh", "-c", marker + "=1 sh -c 'sleep 45' & echo started; wait"},
		TimeoutSec: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut {
		t.Fatal("TimedOut should be set")
	}

	// Give the kill a moment to be reaped, then confirm nothing survived.
	time.Sleep(300 * time.Millisecond)
	check, err := doExec(&proto.ExecParams{
		Argv: []string{"sh", "-c", "pgrep -f " + marker + " | wc -l"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(check.Stdout); got != "0" {
		t.Errorf("%s survivors = %s, want 0: the group kill missed a grandchild", marker, got)
	}
}
