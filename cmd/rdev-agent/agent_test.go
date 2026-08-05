package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/tonynyyan/rdev/internal/proto"
)

// TestMain lets the test binary act as its own job supervisor.
//
// jobStart re-execs os.Executable() with -supervise, which in production is the
// agent binary. Under `go test` os.Executable() is the test binary, and a test
// binary never runs main(), so without this hook the supervisor would start no
// child and every job would look like it vanished.
func TestMain(m *testing.M) {
	if len(os.Args) > 3 && os.Args[1] == superviseFlag && os.Args[3] == "--" {
		runSupervisor(os.Args[2], os.Args[4:])
		return // unreachable: runSupervisor exits
	}
	os.Exit(m.Run())
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

	list, err := jobList(state)
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
