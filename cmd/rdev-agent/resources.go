package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/CIPFZ/rdev/internal/proto"
)

// Resource admission is deliberately conservative: a non-zero budget is
// accepted only when this build can enforce it for the complete process tree.
func effectiveEnvelope(requested *proto.ResourceEnvelope) (proto.ResourceEnvelope, error) {
	var out proto.ResourceEnvelope
	if requested == nil {
		return out, nil
	}
	if requested.CPUQuotaMillis != 0 || requested.MemoryBytes != 0 || requested.PIDs != 0 {
		return out, fmt.Errorf("requested cpu, memory, or pid limits are unsupported on this agent")
	}
	if requested.FDs < 0 || requested.FDs > 0 {
		if requested.FDs <= 0 {
			return out, fmt.Errorf("fd limit must be positive")
		}
		var lim syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil || uint64(requested.FDs) > lim.Max {
			return out, fmt.Errorf("requested fd limit exceeds enforceable hard limit")
		}
		out.FDs = requested.FDs
	}
	if requested.WallTimeoutSec < 0 || requested.WallTimeoutSec > hardExecTimeoutSec {
		return out, fmt.Errorf("wall timeout is outside the hard limit")
	}
	out.WallTimeoutSec = requested.WallTimeoutSec
	if requested.JobCount < 0 || requested.JobCount > 0 {
		if requested.JobCount <= 0 {
			return out, fmt.Errorf("job count must be positive")
		}
		// Job-count is enforced against the agent's state root at admission.
		out.JobCount = requested.JobCount
	}
	return out, nil
}

func activeJobCount(state string) int {
	entries, err := os.ReadDir(filepath.Join(state, "jobs"))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		m, err := readMeta(filepath.Join(state, "jobs", e.Name()))
		if err == nil && jobAlive(m, filepath.Join(state, "jobs", e.Name())) {
			n++
		}
	}
	return n
}

func enforceJobEnvelope(p *proto.JobParams, state string) (proto.ResourceEnvelope, error) {
	effective, err := effectiveEnvelope(p.Resources)
	if err != nil {
		return effective, err
	}
	if effective.JobCount > 0 && activeJobCount(state) >= effective.JobCount {
		return effective, errors.New("job count envelope exceeded")
	}
	return effective, nil
}
