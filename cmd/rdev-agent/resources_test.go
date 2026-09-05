package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestEffectiveEnvelopeRejectsUnsupportedControls(t *testing.T) {
	for _, field := range []string{"cpu", "memory", "pids"} {
		r := &proto.ResourceEnvelope{}
		switch field {
		case "cpu":
			r.CPUQuotaMillis = 1
		case "memory":
			r.MemoryBytes = 1
		case "pids":
			r.PIDs = 1
		}
		if _, err := effectiveEnvelope(r); err == nil {
			t.Fatalf("%s limit unexpectedly accepted", field)
		}
	}
}

func TestJobWallTimeoutKillsDescendantGroup(t *testing.T) {
	state := t.TempDir()
	if err := os.MkdirAll(filepath.Join(state, "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	res, err := jobStart(&proto.JobParams{
		Spec:      &proto.ExecParams{Argv: []string{"sh", "-c", "sleep 30"}},
		Resources: &proto.ResourceEnvelope{WallTimeoutSec: 1},
	}, state)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		info, statusErr := jobStatus(res.Info.ID, state)
		if statusErr != nil {
			t.Fatal(statusErr)
		}
		if info.State != proto.JobRunning {
			if info.ResourceLimit != "wall_timeout" {
				t.Fatalf("resource limit = %q, want wall_timeout (info=%+v)", info.ResourceLimit, info)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("wall timeout did not terminate the job")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestEffectiveEnvelopeBoundsWallAndFD(t *testing.T) {
	if _, err := effectiveEnvelope(&proto.ResourceEnvelope{WallTimeoutSec: hardExecTimeoutSec + 1}); err == nil {
		t.Fatal("wall limit above hard cap accepted")
	}
	if got, err := effectiveEnvelope(&proto.ResourceEnvelope{}); err != nil || got != (proto.ResourceEnvelope{}) {
		t.Fatalf("zero envelope: %+v %v", got, err)
	}
}
