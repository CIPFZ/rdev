package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

const capabilityProbeVersion = "1"

func executionProfile() proto.ExecutionProfile {
	p := proto.ExecutionProfile{Shell: os.Getenv("SHELL"), Path: os.Getenv("PATH"), Home: "", Locale: os.Getenv("LC_ALL"), Cwd: currentWorkingDir()}
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
		if _, err := os.Stat("/sys/fs/cgroup/cgroup.controllers"); err == nil {
			result.Cgroup = true
		}
	}
	result.Resources = proto.ResourceEnvelope{WallTimeoutSec: hardExecTimeoutSec}
	result.Effective = result.Resources
	p := executionProfile()
	result.Profile = &p
	return result
}

func rlimitAvailable() bool {
	var limit syscall.Rlimit
	return syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit) == nil
}
