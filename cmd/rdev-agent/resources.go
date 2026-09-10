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
	out := proto.ResourceEnvelope{WallTimeoutSec: proto.DefaultJobWallTimeoutSeconds}
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
		if err := validateFDLimit(requested.FDs); err != nil {
			return out, err
		}
		out.FDs = requested.FDs
	}
	wall, err := proto.ResolveTimeout(requested.WallTimeoutSec, proto.DefaultJobWallTimeoutSeconds)
	if err != nil {
		return out, err
	}
	out.WallTimeoutSec = wall
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
		dir := filepath.Join(state, "jobs", e.Name())
		m, err := readMeta(dir)
		// A newly reserved directory has not published meta.json yet. Count it
		// conservatively while admission is serialized, otherwise two concurrent
		// starters can both pass the job-count check before either supervisor is
		// visible.
		if err == nil && jobAlive(m, dir) {
			n++
		} else if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOENT) {
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
