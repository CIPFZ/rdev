package release

import (
	artifactcontract "github.com/CIPFZ/rdev/internal/artifact"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSignedAuditCannotSelectReplacementManifest(t *testing.T) {
	dir := t.TempDir()
	original := []byte(`{"schema_version":1}`)
	expected := artifactcontract.File{Name: "manifest.json", SHA256: artifactcontract.Hash(original), Size: int64(len(original))}
	replacement := []byte(`{"schema_version":2}`)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), replacement, 0600); err != nil {
		t.Fatal(err)
	}
	err := verifyExpected(dir, expected)
	if err == nil || !strings.Contains(err.Error(), "digest/size mismatch") {
		t.Fatalf("signed audit selected a replacement manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), original, 0600); err != nil {
		t.Fatal(err)
	}
	// Returning to A after a failed B audit cannot turn it into successful evidence.
	if err := verifyExpected(dir, expected); err == nil {
		t.Fatal("incomplete signed audit accepted")
	}
}
