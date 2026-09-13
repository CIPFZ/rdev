package artifact

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDiagnosePolicyDistinguishesMissingAndValid(t *testing.T) {
	dir, err := os.MkdirTemp(".", ".policy-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "release-policy.json")
	t.Setenv("RDEV_RELEASE_POLICY", path)
	d := DiagnosePolicy(time.Now())
	if d.Exists || d.Valid || d.Error != "policy is missing" {
		t.Fatalf("missing diagnosis = %+v", d)
	}
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"valid_until":"2099-01-01T00:00:00Z","bundle_dir":"/tmp","channels":["dev"],"allow_unsigned_dev":true,"allow_test_roots":false,"roots":[],"hosts":{},"rollback_digests":{}}`), 0600); err != nil {
		t.Fatal(err)
	}
	d = DiagnosePolicy(time.Now())
	if !d.Exists || !d.Valid || d.Error != "" {
		t.Fatalf("valid diagnosis = %+v", d)
	}
}

func TestDiagnosePolicyRejectsPublicFile(t *testing.T) {
	dir, err := os.MkdirTemp(".", ".policy-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "release-policy.json")
	t.Setenv("RDEV_RELEASE_POLICY", path)
	if err := os.WriteFile(path, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	d := DiagnosePolicy(time.Now())
	if d.Valid || !d.Exists || d.Error == "" {
		t.Fatalf("public policy diagnosis = %+v", d)
	}
}
