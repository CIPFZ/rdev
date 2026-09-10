//go:build !windows

package transport

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/proto"
)

// This opt-in gate is intentionally separate from native pipe tests: only a
// real Windows OpenSSH endpoint can certify shell bootstrap and SSH disconnects.
func TestWindowsRemoteSSH(t *testing.T) {
	target := os.Getenv("RDEV_WINDOWS_SSH")
	if target == "" {
		t.Skip("RDEV_WINDOWS_SSH not set: real Windows SSH runtime unverified")
	}
	binPath := os.Getenv("RDEV_WINDOWS_AGENT")
	if binPath == "" {
		t.Fatal("RDEV_WINDOWS_AGENT must name the Windows agent bytes")
	}
	data, err := os.ReadFile(binPath)
	if err != nil {
		t.Fatal(err)
	}
	suffix, err := randomStageSuffix()
	if err != nil {
		t.Fatal(err)
	}
	host := Host{Name: "phase9-runtime", Addr: target, RemoteDir: ".cache/rdev-phase9-" + suffix}
	lookup := func(goos, arch string) (*AgentBinary, error) {
		if goos != "windows" || arch != "amd64" {
			t.Fatalf("wrong remote platform: %s/%s", goos, arch)
		}
		return &AgentBinary{Data: data, SHA256: artifact.Hash(data), Authorize: func(context.Context, Host) (artifact.Decision, error) {
			return artifact.Decision{Digest: artifact.Hash(data), Unsigned: true, Channel: "dev", Version: "0.1.0-dev.0"}, nil
		}}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := Dial(ctx, host, lookup)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		c.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		_, err := c.runSSH(cleanup, powershellCommand(windowsPrivateScript+`Check-Private $p.root; for($i=0;$i -lt 20;$i++){try{[IO.Directory]::Delete($p.root,$true);exit 0}catch{Start-Sleep -Milliseconds 200}};throw 'owned test namespace cleanup failed'`, map[string]string{"root": c.stateDir})...)
		if err != nil {
			t.Errorf("Windows SSH fixture cleanup: %v", err)
		}
	}()
	seq := 0
	call := func(r *proto.Request) *proto.Response {
		t.Helper()
		seq++
		r.ID = ""
		r.ClientID = "phase9-ssh"
		if r.OperationID == "" {
			r.OperationID = "ssh-test-" + strings.Repeat("a", seq)
		}
		resp, err := c.Do(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		if !resp.OK {
			raw, _ := json.Marshal(resp.Error)
			t.Fatalf("remote %s failed: %s", r.Op, raw)
		}
		return resp
	}
	args := []string{"powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", "[Console]::OutputEncoding=[Text.Encoding]::UTF8; [Console]::Write('phase9 中文')"}
	r := call(&proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: args}})
	if r.Exec == nil || r.Exec.ExitCode != 0 || !strings.Contains(r.Exec.Stdout, "phase9") {
		t.Fatalf("remote exec: %+v", r)
	}
	file := c.stateDir + `\roundtrip.bin`
	call(&proto.Request{Op: proto.OpWriteFile, Cat: &proto.WriteParams{Path: file, Content: "AAECA/8=", ContentB64: true}})
	r = call(&proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: file}})
	if r.Read == nil || r.Read.Content != "AAECA/8=" {
		t.Fatalf("remote file: %+v", r)
	}
	jobSpec := &proto.ExecParams{Argv: []string{"powershell.exe", "-NoProfile", "-NonInteractive", "-Command", "Start-Sleep -Seconds 2; [Console]::Write('survived SSH disconnect')"}}
	r = call(&proto.Request{Op: proto.OpJobStart, OperationID: "durable-ssh-job", Job: &proto.JobParams{Spec: jobSpec, DurableStart: true}})
	if r.Job == nil || r.Job.Info == nil {
		t.Fatal("missing job identity")
	}
	id := r.Job.Info.ID
	c.Close()
	c, err = Dial(ctx, host, lookup)
	if err != nil {
		t.Fatal(err)
	}
	r = call(&proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: id, WaitTimeoutSec: 10, TailOnExit: 10}})
	if r.Job == nil || r.Job.Info == nil || r.Job.Info.State != proto.JobExited || r.Job.Info.ExitCode != 0 || !strings.Contains(r.Job.Logs, "survived SSH") {
		t.Fatalf("job after SSH reconnect: %+v", r)
	}
}
