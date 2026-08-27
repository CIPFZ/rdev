package support

import "testing"

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
