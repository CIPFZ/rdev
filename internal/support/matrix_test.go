package support

import (
	"github.com/CIPFZ/rdev/internal/proto"
	"testing"
)

func TestSupportSnapshotSeparatesRuntimeAndBuildClaims(t *testing.T) {
	matrix := Snapshot()
	if matrix.SchemaVersion != SchemaVersion {
		t.Fatalf("schema = %d", matrix.SchemaVersion)
	}
	for _, platforms := range [][]Platform{matrix.Local, matrix.Remote} {
		for _, platform := range platforms {
			if platform.Tier == "tier1" && platform.Status == "cross-build only" {
				t.Errorf("build-only platform promoted to tier1: %+v", platform)
			}
		}
	}
	if len(matrix.RequiredSSHFeatures) == 0 || len(matrix.NonGoals) == 0 {
		t.Fatal("support contract omitted requirements or non-goals")
	}
}

func TestJobStartDiscoveryRequiresExecutionFeatures(t *testing.T) {
	for _, mode := range []string{"standalone", "broker"} {
		for _, missing := range append([]proto.Feature{""}, proto.SupportedFeatures()...) {
			d := Discover(mode)
			features := []proto.Feature{}
			for _, f := range proto.SupportedFeatures() {
				if f != missing {
					features = append(features, f)
				}
			}
			d.SetRuntime(&proto.CapabilityResult{ProbeVersion: "1", OS: "linux", Arch: "amd64", Features: features})
			op, _ := proto.LookupOperation(proto.OpJobStart)
			want := "supported"
			for _, required := range op.RequiredFeatures {
				if missing == required {
					want = "unsupported"
				}
			}
			if mode == "broker" && missing == proto.FeatureDurableJobStart {
				want = "unsupported"
			}
			seen := false
			for _, control := range d.Runtime.Controls {
				if control.Name == "job_start" {
					seen = true
					if control.Status != want {
						t.Fatalf("%s missing %s got %s want %s", mode, missing, control.Status, want)
					}
				}
			}
			if !seen {
				t.Fatal("job capability undiscoverable")
			}
		}
	}
}
