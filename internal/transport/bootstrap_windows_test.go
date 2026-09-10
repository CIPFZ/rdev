package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/winutil"
)

// Exercise Windows PowerShell 5.1 through the default OpenSSH command shell.
// This supplements, but does not replace, the real SSH lifecycle gate.
func runWindowsBootstrap(t *testing.T, args []string, input io.Reader) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "cmd.exe", "/d", "/s", "/c", strings.Join(args, " "))
	cmd.Stdin = input
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("PowerShell bootstrap: %v\n%s", err, &stderr)
	}
	return out
}

func TestWindowsTransportHelper(t *testing.T) {
	for i, arg := range os.Args {
		if arg != "--rdev-bootstrap-helper" {
			continue
		}
		if err := json.NewEncoder(os.Stdout).Encode(os.Args[i+1:]); err != nil {
			os.Exit(2)
		}
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	}
}

func TestWindowsBootstrapNativeArgvAndBinaryPipes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "中文 space '& $(literal)")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	self, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(root, "helper.exe")
	if err = os.WriteFile(exe, self, 0600); err != nil {
		t.Fatal(err)
	}
	argv := []string{"", "中文 space", `C:\ends\`, `a\"b`, `$(&%not-run%)`}
	args := append([]string{"-test.run=^TestWindowsTransportHelper$", "--rdev-bootstrap-helper"}, argv...)
	payload := bytes.Repeat([]byte{0, 255, 128, 13, 10, 'M', 'Z'}, 10000)
	out := runWindowsBootstrap(t, windowsLaunch(exe, args...), bytes.NewReader(payload))
	header, suffix, ok := bytes.Cut(out, []byte{'\n'})
	var got []string
	if !ok || json.Unmarshal(header, &got) != nil || !reflect.DeepEqual(got, argv) || !bytes.Equal(suffix, payload) {
		t.Fatal("PowerShell launch changed argv or binary streams")
	}
}

func TestWindowsBootstrapVersionedInstall(t *testing.T) {
	exe := os.Getenv("RDEV_WINDOWS_AGENT")
	if exe == "" {
		t.Fatal("RDEV_WINDOWS_AGENT must name the built Windows agent")
	}
	bin, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "中文 space '& $(literal)")
	digest := artifact.Hash(bin)
	decision, err := json.Marshal(artifact.Decision{Digest: digest, Unsigned: true, Channel: "dev", Version: "0.1.0-dev.0"})
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]string{"root": root, "digest": digest, "args": windowsArgs([]string{"-install-candidate", root, string(decision), ""})}
	args, input, err := windowsInstallCommand(data, bin)
	if err != nil {
		t.Fatal(err)
	}
	runWindowsBootstrap(t, args, input)
	active := windowsAgentPath(root, digest)
	if err = winutil.CheckPrivate(active); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(active)
	if err != nil || !bytes.Equal(actual, bin) {
		t.Fatal("bootstrap did not publish exact agent bytes")
	}
	// Verify the same launch path used on reconnection, and that the upload
	// reservation was released after the helper exited.
	if out := runWindowsBootstrap(t, windowsLaunch(active, "-version"), nil); !bytes.Contains(out, []byte("rdev-agent")) {
		t.Fatalf("installed agent version: %s", out)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".rdev-upload-slot-") {
			t.Fatal("completed bootstrap retained an upload reservation")
		}
	}
}
