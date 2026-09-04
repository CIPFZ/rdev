package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestExecutionProfileDigestIsStableAndExcludesDigest(t *testing.T) {
	t.Setenv("SHELL", "/bin/sh")
	t.Setenv("PATH", "/usr/bin:/bin")
	t.Setenv("LC_ALL", "C.UTF-8")
	one := executionProfile()
	two := executionProfile()
	if one.Digest == "" || one.Digest != two.Digest {
		t.Fatalf("profile digest is not stable: %q vs %q", one.Digest, two.Digest)
	}
	if len(one.Digest) != 64 || strings.Trim(one.Digest, "0123456789abcdef") != "" {
		t.Fatalf("digest is not lowercase sha256: %q", one.Digest)
	}
	withDifferentDigest := one
	withDifferentDigest.Digest = "different"
	if got := withProfileDigest(withDifferentDigest).Digest; got != one.Digest {
		t.Fatalf("digest changed when only the prior digest changed: %q", got)
	}
}

func TestCapabilityProbeReportsProfileAndSafeMetadata(t *testing.T) {
	result := probeCapabilities(true)
	if result == nil || result.ProbeVersion == "" || result.ProbedAt == "" || result.OS == "" || result.Arch == "" {
		t.Fatalf("incomplete capability result: %+v", result)
	}
	if result.Profile == nil || result.Profile.Digest == "" || result.Profile.Path == "" {
		t.Fatalf("missing execution profile: %+v", result.Profile)
	}
	if result.Resources.WallTimeoutSec <= 0 || result.Effective.WallTimeoutSec != result.Resources.WallTimeoutSec {
		t.Fatalf("effective policy is not reported: resources=%+v effective=%+v", result.Resources, result.Effective)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), os.Getenv("RDEV_TEST_SECRET")) && os.Getenv("RDEV_TEST_SECRET") != "" {
		t.Fatal("capability result unexpectedly contains test secret")
	}
}

func TestCapabilityOperationIsReadOnly(t *testing.T) {
	descriptor, ok := proto.LookupOperation(proto.OpCapabilityProbe)
	if !ok || descriptor.Class != proto.ClassReadOnly || descriptor.Retry != proto.RetrySafe {
		t.Fatalf("capability operation descriptor=%+v, ok=%v", descriptor, ok)
	}
}
