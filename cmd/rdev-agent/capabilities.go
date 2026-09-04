package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const capabilityProbeVersion = "1"

func executionProfile() proto.ExecutionProfile {
	p := proto.ExecutionProfile{Shell: os.Getenv("SHELL"), Path: os.Getenv("PATH"), Home: "", Locale: os.Getenv("LC_ALL"), Cwd: currentWorkingDir(), Umask: processUmask()}
	if p.Shell == "" {
		p.Shell = os.Getenv("COMSPEC")
	}
	if p.Locale == "" {
		p.Locale = os.Getenv("LANG")
	}
	if p.Home, _ = os.UserHomeDir(); p.Home == "" {
		p.Home = os.Getenv("HOME")
	}
	return withProfileDigest(p)
}

// processUmask reads the kernel's representation when procfs exposes it. The
// usual syscall.Umask(0) approach is not safe here: changing the process umask
// even briefly races with concurrent job creation in the agent. An empty value
// means that the platform does not provide a race-free read-only query.
func processUmask() string {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return ""
	}
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		if !bytes.HasPrefix(line, []byte("Umask:")) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(string(line), "Umask:"))
		n, err := strconv.ParseUint(value, 8, 32)
		if err != nil || n > 0777 {
			return ""
		}
		return fmt.Sprintf("%04o", n)
	}
	return ""
}

func currentWorkingDir() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return cwd
}

func withProfileDigest(p proto.ExecutionProfile) proto.ExecutionProfile {
	p.Digest = ""
	b, _ := json.Marshal(p)
	s := sha256.Sum256(b)
	p.Digest = hex.EncodeToString(s[:])
	return p
}

func probeCapabilities(refresh bool) *proto.CapabilityResult {
	_ = refresh // capability probes are cheap and intentionally uncached for now.
	now := time.Now().UTC()
	result := &proto.CapabilityResult{ProbeVersion: capabilityProbeVersion, ProbedAt: now.Format(time.RFC3339Nano), OS: runtime.GOOS, Arch: runtime.GOARCH, Rlimit: rlimitAvailable()}
	// Linux cgroup v2 is the only currently advertised hard-control backend.
	if runtime.GOOS == "linux" {
		result.Cgroup = cgroupV2Available()
	}
	result.Resources = proto.ResourceEnvelope{WallTimeoutSec: hardExecTimeoutSec}
	result.Effective = result.Resources
	p := executionProfile()
	result.Profile = &p
	return result
}

func cgroupV2Available() bool {
	b, err := os.ReadFile("/sys/fs/cgroup/cgroup.controllers")
	return err == nil && len(bytes.TrimSpace(b)) > 0
}

func rlimitAvailable() bool {
	var limit syscall.Rlimit
	return syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit) == nil
}
