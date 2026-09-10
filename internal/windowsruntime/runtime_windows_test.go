// Native Windows integration tests execute the actual remote agent over its
// protocol. They do not claim that a Windows-local controller is supported.
package windowsruntime

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
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/state"
	"github.com/CIPFZ/rdev/internal/synctree"
	"github.com/CIPFZ/rdev/internal/winutil"
)

func privateRoot(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	if err := winutil.EnsurePrivateDir(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

type peer struct {
	t      *testing.T
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    chan proto.Response
	errors chan error
	seq    int
	closed bool
}

func startPeer(t *testing.T, root string) *peer {
	t.Helper()
	binary := os.Getenv("RDEV_WINDOWS_AGENT")
	if binary == "" {
		t.Fatal("RDEV_WINDOWS_AGENT must name the built Windows agent")
	}
	cmd := exec.Command(binary, "-state", root)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	p := &peer{t: t, cmd: cmd, in: in, out: make(chan proto.Response, 128), errors: make(chan error, 1)}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.close)
	go func() {
		scan := bufio.NewScanner(out)
		scan.Buffer(make([]byte, 64<<10), 8<<20)
		for scan.Scan() {
			var r proto.Response
			if err := json.Unmarshal(scan.Bytes(), &r); err != nil {
				p.errors <- err
				return
			}
			p.out <- r
		}
		err := scan.Err()
		if err == nil {
			err = io.EOF
		}
		p.errors <- err
	}()
	r := p.call(proto.Request{Op: proto.OpPing, Hello: &proto.HelloParams{MinVersion: proto.Version, MaxVersion: proto.Version, Features: proto.SupportedFeatures()}})
	if !r.OK || r.Ping == nil || r.Ping.OS != "windows" {
		t.Fatalf("Windows hello: %+v", r)
	}
	return p
}
func (p *peer) close() {
	if p.closed {
		return
	}
	p.closed = true
	p.in.Close()
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
}
func (p *peer) send(r proto.Request) string {
	p.t.Helper()
	p.seq++
	r.ID = strconv.Itoa(p.seq)
	if r.ClientID == "" {
		r.ClientID = "windows-runtime"
	}
	if r.OperationID == "" {
		r.OperationID = "operation-" + r.ID
	}
	if err := json.NewEncoder(p.in).Encode(r); err != nil {
		p.t.Fatal(err)
	}
	return r.ID
}
func (p *peer) receive(id string, hello bool) proto.Response {
	p.t.Helper()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case r := <-p.out:
			if r.ID == id && (hello || r.Terminal) {
				return r
			}
		case err := <-p.errors:
			p.t.Fatal(err)
		case <-timer.C:
			p.t.Fatal("agent response timed out")
		}
	}
}
func (p *peer) call(r proto.Request) proto.Response {
	id := p.send(r)
	return p.receive(id, r.Hello != nil)
}
func helper(args ...string) *proto.ExecParams {
	return &proto.ExecParams{Argv: append([]string{os.Args[0], "-test.run=^TestWindowsHelper$", "--"}, args...), Env: map[string]string{"RDEV_WINDOWS_HELPER": "1"}}
}

func TestWindowsHelper(t *testing.T) {
	if os.Getenv("RDEV_WINDOWS_HELPER") != "1" {
		return
	}
	i := 0
	for i < len(os.Args) && os.Args[i] != "--" {
		i++
	}
	args := os.Args[i+1:]
	switch args[0] {
	case "echo":
		b, _ := io.ReadAll(os.Stdin)
		cwd, _ := os.Getwd()
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"args": args[1:], "stdin": string(b), "cwd": cwd, "env": os.Getenv("RDEV_TEST_VALUE")})
		os.Exit(0)
	case "later":
		time.Sleep(500 * time.Millisecond)
		fmt.Print("survived disconnect 中文")
		os.Exit(0)
	case "exit":
		os.Exit(23)
	case "sleep":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	case "tree":
		c := exec.Command(os.Args[0], "-test.run=^TestWindowsHelper$", "--", "sleep")
		c.Env = os.Environ()
		if c.Start() != nil {
			os.Exit(2)
		}
		_ = os.WriteFile(args[1], []byte(strconv.Itoa(c.Process.Pid)), 0600)
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
	os.Exit(2)
}

func TestWindowsExecFilesAndExit(t *testing.T) {
	root := privateRoot(t)
	p := startPeer(t, root)
	args := []string{"", "中文 with space", `C:\ends\`, `"quoted"`, `$(&%not-run%)`}
	spec := helper(append([]string{"echo"}, args...)...)
	spec.Cwd = root
	spec.Stdin = "stdin 中文"
	spec.Env["RDEV_TEST_VALUE"] = "value 中文"
	r := p.call(proto.Request{Op: proto.OpExec, Exec: spec})
	if !r.OK || r.Exec == nil || r.Exec.ExitCode != 0 {
		t.Fatalf("exec: %+v", r)
	}
	var echo struct {
		Args            []string
		Stdin, Cwd, Env string
	}
	if err := json.Unmarshal([]byte(r.Exec.Stdout), &echo); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(echo.Args, args) || echo.Stdin != spec.Stdin || echo.Env != "value 中文" || !strings.EqualFold(echo.Cwd, root) {
		t.Fatalf("argv/environment round trip: %+v", echo)
	}
	r = p.call(proto.Request{Op: proto.OpExec, Exec: helper("exit")})
	if !r.OK || r.Exec == nil || r.Exec.ExitCode != 23 {
		t.Fatalf("exit: %+v", r)
	}
	path := filepath.Join(root, "中文 file.bin")
	data := []byte{0, 255, 1, 2, 10}
	r = p.call(proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: path, Content: base64.StdEncoding.EncodeToString(data), ContentB64: true}})
	if !r.OK {
		t.Fatalf("write: %+v", r)
	}
	r = p.call(proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: path}})
	if !r.OK || r.Read == nil {
		t.Fatalf("read: %+v", r)
	}
	got, err := base64.StdEncoding.DecodeString(r.Read.Content)
	if err != nil || !r.Read.ContentB64 || !bytes.Equal(got, data) {
		t.Fatalf("binary read: %+v", r)
	}
	for _, path := range []string{`\\.\pipe\rdev-invalid`, filepath.Join(root, "data:stream"), filepath.Join(root, "NUL")} {
		r = p.call(proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: path}})
		if r.OK {
			t.Fatalf("unsafe path accepted: %s", path)
		}
	}
}

func TestWindowsTimeoutAndDisconnectContainTrees(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(fmt.Sprint(disconnect), func(t *testing.T) {
			root := privateRoot(t)
			p := startPeer(t, root)
			marker := filepath.Join(root, "child.pid")
			spec := helper("tree", marker)
			spec.TimeoutSec = 1
			id := p.send(proto.Request{Op: proto.OpExec, Exec: spec})
			var pid int
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if b, e := os.ReadFile(marker); e == nil {
					pid, _ = strconv.Atoi(string(b))
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if pid == 0 {
				t.Fatal("child not started")
			}
			if disconnect {
				p.close()
			} else {
				r := p.receive(id, false)
				if !r.OK || r.Exec == nil || !r.Exec.TimedOut {
					t.Fatalf("timeout: %+v", r)
				}
			}
			deadline = time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				if _, e := winutil.ProcessIdentity(pid); e != nil {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatal("request descendant survived")
		})
	}
}

func TestWindowsDetachedJobRestartAndDurableIdentity(t *testing.T) {
	root := privateRoot(t)
	p := startPeer(t, root)
	req := proto.Request{Op: proto.OpJobStart, OperationID: "durable-windows-start", Job: &proto.JobParams{Spec: helper("later"), DurableStart: true}}
	r := p.call(req)
	if !r.OK || r.Job == nil || r.Job.Info == nil {
		t.Fatalf("start: %+v", r)
	}
	id := r.Job.Info.ID
	p.close()
	p = startPeer(t, root)
	req.Replay = true
	r = p.call(req)
	if !r.OK || r.Job == nil || r.Job.Info == nil || r.Job.Info.ID != id {
		t.Fatalf("durable replay: %+v", r)
	}
	r = p.call(proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: id, WaitTimeoutSec: 5, TailLines: 20}})
	if !r.OK || r.Job == nil || r.Job.Info == nil || r.Job.Info.State != proto.JobExited {
		t.Fatalf("restarted wait: %+v", r)
	}
	r = p.call(proto.Request{Op: proto.OpJobLogs, Job: &proto.JobParams{ID: id, TailLines: 20}})
	if !r.OK {
		t.Fatalf("logs: %+v", r)
	}
	b, _ := json.Marshal(r)
	if !bytes.Contains(b, []byte("survived disconnect")) {
		t.Fatalf("logs missing: %s", b)
	}
}

func TestWindowsJobStopAndMigrationFence(t *testing.T) {
	root := privateRoot(t)
	p := startPeer(t, root)
	r := p.call(proto.Request{Op: proto.OpJobStart, Job: &proto.JobParams{Spec: helper("sleep")}})
	if !r.OK || r.Job == nil || r.Job.Info == nil {
		t.Fatalf("job start: %+v", r)
	}
	id := r.Job.Info.ID
	if _, err := state.Migrate(root, false); !errors.Is(err, state.ErrMigrationLocked) {
		t.Fatalf("running supervisor did not fence migration: %v", err)
	}
	r = p.call(proto.Request{Op: proto.OpJobStop, Job: &proto.JobParams{ID: id, Signal: "TERM"}})
	if !r.OK {
		t.Fatalf("stop: %+v", r)
	}
	r = p.call(proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: id, WaitTimeoutSec: 5}})
	if !r.OK || r.Job == nil || r.Job.Info == nil || r.Job.Info.State != proto.JobKilled {
		t.Fatalf("stopped state: %+v", r)
	}
}

func TestWindowsStagedSyncAndOutcome(t *testing.T) {
	ctx := context.Background()
	root := privateRoot(t)
	source := filepath.Join(root, "source")
	if err := winutil.EnsurePrivateDir(source); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "中文.txt"), []byte("snapshot bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := synctree.NewStore(filepath.Join(root, "store"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := synctree.NewID()
	stage, err := store.Capture(ctx, "owner", id, source, "preserve")
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(root, "destination")
	snapshot, err := synctree.Inspect(ctx, destination, synctree.StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := synctree.Build(stage.Manifest, snapshot, false, "overwrite")
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.Execute(ctx, "owner", id, destination, "operation", plan)
	if err != nil {
		t.Fatal(err)
	}
	if out.State == "" {
		t.Fatal("missing durable outcome")
	}
	b, err := os.ReadFile(filepath.Join(destination, "中文.txt"))
	if err != nil || string(b) != "snapshot bytes" {
		t.Fatalf("destination: %q %v", b, err)
	}
	if _, err = store.Execute(ctx, "owner", id, destination, "operation", plan); !errors.Is(err, synctree.ErrRecorded) {
		t.Fatalf("sync replay: %v", err)
	}
}
