//go:build linux

package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/transport"
)

func syncProcessState(pid int) (state, start string) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", ""
	}
	line := string(raw)
	end := strings.LastIndex(line, ")")
	if end < 0 {
		return "", ""
	}
	fields := strings.Fields(line[end+1:])
	if len(fields) < 20 {
		return "", ""
	}
	return fields[0], fields[19]
}

func TestRealRsyncCancellationStopsSSHDescendants(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("real rsync required")
	}
	for _, preflight := range []bool{false, true} {
		t.Run(fmt.Sprint("delete-preflight-", preflight), func(t *testing.T) {
			dir := t.TempDir()
			wrapper := "#!/bin/sh\nprintf '%s' \"$$\" > \"$RDEV_RSYNC_TEST_DIR/ssh.pid\"\nsleep 60 &\nprintf '%s' \"$!\" > \"$RDEV_RSYNC_TEST_DIR/child.pid\"\nwait\n"
			if err := os.WriteFile(filepath.Join(dir, "ssh"), []byte(wrapper), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("RDEV_RSYNC_TEST_DIR", dir)
			c := newTestClient()
			host := transport.Host{Name: "dev", Addr: "unused"}
			if err := c.Hosts.Add(host); err != nil {
				t.Fatal(err)
			}
			base := &fakeRemoteConn{host: host}
			c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) { return base, nil }
			defer c.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := c.Sync(ctx, SyncOptions{Host: "dev", Direction: "push", Local: t.TempDir(), Remote: "dst", DryRun: !preflight, Delete: preflight, ConfirmDelete: preflight})
				done <- err
			}()
			pids := map[int]string{}
			t.Cleanup(func() {
				for pid, identity := range pids {
					if _, current := syncProcessState(pid); current != "" && current == identity {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
				}
			})
			deadline := time.Now().Add(5 * time.Second)
			for _, name := range []string{"ssh.pid", "child.pid"} {
				for {
					data, err := os.ReadFile(filepath.Join(dir, name))
					pid, parseErr := strconv.Atoi(string(data))
					if err == nil && parseErr == nil && pid > 0 {
						_, identity := syncProcessState(pid)
						if identity != "" {
							pids[pid] = identity
							break
						}
					}
					if time.Now().After(deadline) {
						t.Fatal("rsync did not spawn its SSH descendant")
					}
					time.Sleep(time.Millisecond)
				}
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("real rsync cancellation lost context identity: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("rsync descendant kept the output pipe and operation alive")
			}
			// killpg delivers SIGKILL to every member, but wait on the rsync
			// leader is not waitpid on its grandchildren. Their /proc state may
			// briefly remain runnable after descriptors have already closed.
			stoppedBy := time.Now().Add(time.Second)
			for pid, identity := range pids {
				for {
					state, current := syncProcessState(pid)
					if current != identity || state == "" || state == "Z" || state == "X" {
						break
					}
					if time.Now().After(stoppedBy) {
						t.Fatalf("operation descendant %d survived cancellation", pid)
					}
					time.Sleep(time.Millisecond)
				}
			}
			if base.closed {
				t.Fatal("rsync cancellation closed the shared base connection")
			}
			if _, err := c.Ping(t.Context(), "dev"); err != nil {
				t.Fatal(err)
			}
		})
	}
}
