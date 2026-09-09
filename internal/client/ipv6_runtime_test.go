package client

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/transport"
)

// This opt-in harness generates its own temporary SSH credentials and serves
// only ::1. It never reads or copies existing private keys or external hosts.
func TestLocalIPv6SSHAndSync(t *testing.T) {
	if os.Getenv("RDEV_RUN_IPV6") != "1" {
		t.Skip("set RDEV_RUN_IPV6=1 for isolated local IPv6/sshd runtime")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("isolated sshd harness requires Linux")
	}
	sshd, err := exec.LookPath("sshd")
	if err != nil {
		t.Fatal(err)
	}
	ssh, err := exec.LookPath("ssh")
	if err != nil {
		t.Fatal(err)
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	dir := t.TempDir()
	for _, name := range []string{"host", "identity"} {
		cmd := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", filepath.Join(dir, name))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("generate isolated %s key: %v %s", name, err, out)
		}
	}
	serverConfig := fmt.Sprintf("Port %d\nListenAddress ::1\nHostKey %s\nPidFile %s\nAuthorizedKeysFile %s\nStrictModes no\nPubkeyAuthentication yes\nPasswordAuthentication no\nKbdInteractiveAuthentication no\nUsePAM no\nPermitRootLogin yes\nAllowUsers %s\n", port, filepath.Join(dir, "host"), filepath.Join(dir, "sshd.pid"), filepath.Join(dir, "identity.pub"), current.Username)
	configPath := filepath.Join(dir, "sshd_config")
	if err := os.WriteFile(configPath, []byte(serverConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	server := exec.Command(sshd, "-D", "-f", configPath, "-E", filepath.Join(dir, "sshd.log"))
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Process.Kill(); _ = server.Wait() })
	ready := false
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); {
		conn, err := net.DialTimeout("tcp6", fmt.Sprintf("[::1]:%d", port), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			ready = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !ready {
		logs, _ := os.ReadFile(filepath.Join(dir, "sshd.log"))
		t.Fatalf("isolated sshd did not listen: %s", logs)
	}
	clientConfig := fmt.Sprintf("Host *\n IdentityFile %s\n IdentitiesOnly yes\n UserKnownHostsFile %s\n StrictHostKeyChecking accept-new\n ConnectTimeout 5\n", filepath.Join(dir, "identity"), filepath.Join(dir, "known_hosts"))
	sshConfig := filepath.Join(dir, "ssh_config")
	if err := os.WriteFile(sshConfig, []byte(clientConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	wrapDir := filepath.Join(dir, "tools")
	if err := os.Mkdir(wrapDir, 0o700); err != nil {
		t.Fatal(err)
	}
	wrapper := "#!/bin/sh\nexec \"$RDEV_IPV6_REAL_SSH\" -F \"$RDEV_IPV6_CONFIG\" \"$@\"\n"
	if err := os.WriteFile(filepath.Join(wrapDir, "ssh"), []byte(wrapper), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", wrapDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("RDEV_IPV6_REAL_SSH", ssh)
	t.Setenv("RDEV_IPV6_CONFIG", sshConfig)
	agentPath := filepath.Join("..", "..", "cmd", "rdev", "agents", "rdev-agent-linux-"+runtime.GOARCH)
	agent, err := os.ReadFile(agentPath)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(agent))
	c := New(func(goos, goarch string) (*transport.AgentBinary, error) {
		if goos != "linux" || goarch != runtime.GOARCH {
			return nil, fmt.Errorf("unexpected local platform")
		}
		return &transport.AgentBinary{Data: agent, SHA256: digest}, nil
	})
	namespace := fmt.Sprintf(".cache/rdev-phase6-ipv6-%d-%d", os.Getpid(), port)
	stateRoot := filepath.Join(current.HomeDir, namespace)
	if _, err := os.Lstat(stateRoot); !os.IsNotExist(err) {
		t.Fatalf("isolated namespace already exists: %v", err)
	}
	t.Cleanup(func() {
		c.mu.Lock()
		pooled, connected := c.conns["ipv6"]
		c.mu.Unlock()
		c.Close()
		if connected {
			args := append(pooled.conn.SSHArgs(), "-O", "exit", pooled.conn.Host().Addr)
			_ = exec.Command(ssh, append([]string{"-F", sshConfig}, args...)...).Run()
		}
		_ = os.RemoveAll(stateRoot)
	})
	if err := c.Hosts.Add(transport.Host{Name: "ipv6", Addr: current.Username + "@[::1]:" + strconv.Itoa(port), RemoteDir: namespace}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.Ping(ctx, "ipv6"); err != nil {
		t.Fatal(err)
	}
	res, err := c.Exec(ctx, ExecOptions{Host: "ipv6", Argv: []string{"printf", "ipv6-ok"}})
	if err != nil || res.Stdout != "ipv6-ok" {
		t.Fatalf("IPv6 exec=%+v %v", res, err)
	}
	local := filepath.Join(dir, "-leading 雪.txt")
	if err := os.WriteFile(local, []byte("ipv6-sync"), 0o600); err != nil {
		t.Fatal(err)
	}
	remote := filepath.Join(stateRoot, "copy.txt")
	result, err := c.Sync(ctx, SyncOptions{Host: "ipv6", Local: local, Remote: remote, ConflictPolicy: "overwrite"})
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("IPv6 rsync=%+v %v", result, err)
	}
	data, err := os.ReadFile(remote)
	if err != nil || string(data) != "ipv6-sync" {
		t.Fatalf("IPv6 transferred bytes=%q %v", data, err)
	}
	if strings.Contains(result.Command, "@[::1]:") == false {
		t.Fatalf("rsync command omitted bracketed IPv6: %s", result.Command)
	}
	t.Log("isolated ::1 sshd: normalized IPv6 user/port bootstrapped agent, exec and rsync with spaces/Unicode succeeded")
}
