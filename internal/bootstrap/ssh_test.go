package bootstrap

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

// This fixture speaks real SSH on loopback and runs the actual mutation shell
// command with a disposable HOME. It does not use any real account keys,
// passwords, known_hosts or SSH agent. OpenSSH/container acceptance is separate.
type sshFixture struct {
	listener         net.Listener
	home             string
	wg               sync.WaitGroup
	commands         atomic.Int32
	keepKey          bool
	dropAfterCommand bool
	swapHostKey      bool
	config           Config
}

func testSigner(t *testing.T) (ssh.Signer, []byte) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(private, "test")
	if err != nil {
		t.Fatal(err)
	}
	encoded := pem.EncodeToMemory(block)
	signer, err := ssh.ParsePrivateKey(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return signer, encoded
}

func newSSHFixture(t *testing.T, keep, drop, swap bool) *sshFixture {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fixture executes POSIX remote scripts")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverKey, _ := testSigner(t)
	alternateKey, _ := testSigner(t)
	clientKey, private := testSigner(t)
	f := &sshFixture{listener: listener, home: t.TempDir(), keepKey: keep, dropAfterCommand: drop, swapHostKey: swap}
	f.config = Config{Address: listener.Addr().String(), User: "fixture", Password: "synthetic-fixture-only", PublicKey: ssh.MarshalAuthorizedKey(clientKey.PublicKey()), PrivateKey: private, HostKeyCallback: ssh.FixedHostKey(serverKey.PublicKey())}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				cfg := &ssh.ServerConfig{
					PasswordCallback: func(meta ssh.ConnMetadata, password []byte) (*ssh.Permissions, error) {
						if meta.User() == f.config.User && string(password) == f.config.Password {
							return nil, nil
						}
						return nil, errors.New("rejected")
					},
					PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
						if meta.User() != f.config.User {
							return nil, errors.New("rejected")
						}
						if f.keepKey && bytes.Equal(key.Marshal(), clientKey.PublicKey().Marshal()) {
							return nil, nil
						}
						data, _ := os.ReadFile(filepath.Join(f.home, ".ssh", "authorized_keys"))
						for len(data) > 0 {
							pub, _, _, rest, err := ssh.ParseAuthorizedKey(data)
							if err != nil {
								break
							}
							if bytes.Equal(pub.Marshal(), key.Marshal()) {
								return nil, nil
							}
							data = rest
						}
						return nil, errors.New("rejected")
					},
				}
				if f.swapHostKey && f.commands.Load() > 0 {
					cfg.AddHostKey(alternateKey)
				} else {
					cfg.AddHostKey(serverKey)
				}
				server, channels, requests, err := ssh.NewServerConn(conn, cfg)
				if err != nil {
					return
				}
				defer server.Close()
				go ssh.DiscardRequests(requests)
				for incoming := range channels {
					if incoming.ChannelType() != "session" {
						_ = incoming.Reject(ssh.UnknownChannelType, "session only")
						continue
					}
					channel, requests, err := incoming.Accept()
					if err != nil {
						return
					}
					for request := range requests {
						if request.Type != "exec" {
							_ = request.Reply(false, nil)
							continue
						}
						var msg struct{ Command string }
						if ssh.Unmarshal(request.Payload, &msg) != nil {
							_ = request.Reply(false, nil)
							break
						}
						_ = request.Reply(true, nil)
						command := exec.Command("sh", "-c", msg.Command)
						command.Dir = f.home
						for _, value := range os.Environ() {
							if !strings.HasPrefix(value, "HOME=") {
								command.Env = append(command.Env, value)
							}
						}
						command.Env = append(command.Env, "HOME="+f.home)
						command.Stdout, command.Stderr = channel, channel.Stderr()
						status := uint32(0)
						if err := command.Run(); err != nil {
							status = 1
						}
						f.commands.Add(1)
						if f.dropAfterCommand {
							_ = listener.Close()
						}
						_, _ = channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{status}))
						break
					}
					_ = channel.Close()
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); f.wg.Wait() })
	return f
}

func (f *sshFixture) seed(t *testing.T) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(f.home, ".ssh"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.home, ".ssh", "authorized_keys"), f.config.PublicKey, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestRevokeSSHVerification(t *testing.T) {
	for _, tt := range []struct {
		name             string
		keep, drop, swap bool
		want             string
	}{
		{name: "explicit publickey rejection succeeds"},
		{name: "other auth source still accepts", keep: true, want: "still authenticates"},
		{name: "network loss is inconclusive", drop: true, want: "inconclusive"},
		{name: "changed host key is inconclusive", swap: true, want: "inconclusive"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := newSSHFixture(t, tt.keep, tt.drop, tt.swap)
			f.seed(t)
			// Zero Timeout is intentional: this must use the default, not an
			// already-expired context that prevents every CLI revoke.
			err := Revoke(t.Context(), f.config)
			if tt.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("want %q, got %v", tt.want, err)
			}
			if f.commands.Load() != 1 {
				t.Fatalf("mutation commands = %d", f.commands.Load())
			}
		})
	}
}

func TestBootstrapSSHPasswordThenDedicatedKey(t *testing.T) {
	f := newSSHFixture(t, false, false, false)
	if err := Run(t.Context(), f.config); err != nil {
		t.Fatal(err)
	}
	if f.commands.Load() != 2 {
		t.Fatalf("want install then verification command, got %d", f.commands.Load())
	}
	if err := Revoke(t.Context(), f.config); err != nil {
		t.Fatal(err)
	}
}

func TestSSHHandshakeHonorsContext(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithCancel(t.Context())
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithTimeout(t.Context(), 150*time.Millisecond)
			}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				client, err := dialConfig(ctx, listener.Addr().String(), &ssh.ClientConfig{User: "fixture"})
				if client != nil {
					client.Close()
				}
				result <- err
			}()
			conn, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if mode == "cancel" {
				cancel()
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("silent peer unexpectedly connected")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("SSH handshake ignored context")
			}
		})
	}
}
