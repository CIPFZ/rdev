package main

import (
	"testing"

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

func TestEffectiveEnvelopeBoundsWallAndFD(t *testing.T) {
	if _, err := effectiveEnvelope(&proto.ResourceEnvelope{WallTimeoutSec: hardExecTimeoutSec + 1}); err == nil {
		t.Fatal("wall limit above hard cap accepted")
	}
	if got, err := effectiveEnvelope(&proto.ResourceEnvelope{}); err != nil || got != (proto.ResourceEnvelope{}) {
		t.Fatalf("zero envelope: %+v %v", got, err)
	}
}
