//go:build unix

package main

import (
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// An inherited stdin that never closes must not stop exec from reaching the
// connection phase: a socket is ignored, and -no-stdin ignores a pipe.
func TestCLIExecDoesNotBlockOnOpenStdin(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	sock, peer := os.NewFile(uintptr(fds[0]), "stdin-sock"), os.NewFile(uintptr(fds[1]), "peer")
	defer sock.Close()
	defer peer.Close()
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipeR.Close()
	defer pipeW.Close()

	for _, test := range []struct {
		name  string
		stdin *os.File
		flags []string
	}{
		{"socket", sock, nil},
		{"pipe -no-stdin", pipeR, []string{"-no-stdin"}},
	} {
		args := append([]string{"-test.run=^TestCLIContractProcessHelper$", "--", "-v", "exec", "127.0.0.1:1", "-timeout", "1"}, test.flags...)
		args = append(args, "--", "true")
		cmd := exec.Command(binary, args...)
		cmd.Dir = t.TempDir()
		cmd.Env = append(os.Environ(), "RDEV_CONTRACT_HELPER=1", "HOME="+t.TempDir(), "RDEV_BROKER_SOCKET=")
		cmd.Stdin = test.stdin
		started := time.Now()
		out, err := cmd.CombinedOutput()
		if elapsed := time.Since(started); elapsed > 30*time.Second {
			t.Fatalf("%s: exec took %s", test.name, elapsed)
		}
		text := string(out)
		if err == nil || !strings.Contains(text, "stdin ignored") || strings.Contains(text, "stdin read:") || !strings.Contains(text, "connecting to") {
			t.Fatalf("%s: err=%v output=%s", test.name, err, text)
		}
	}
}
