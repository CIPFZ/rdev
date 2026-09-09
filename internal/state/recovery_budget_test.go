package state

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryBudgetPreservesEvidenceAndRejectsBeforeMutation(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < RecoveryMaxRuns; i++ {
		if err := os.MkdirAll(filepath.Join(root, "backup", fmt.Sprint(i)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := reserveRecovery(root, "backup", nil); err == nil {
		t.Fatal("unbounded recovery runs")
	}
	entries, err := os.ReadDir(filepath.Join(root, "backup"))
	if err != nil || len(entries) != RecoveryMaxRuns {
		t.Fatalf("recovery evidence removed: %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "quarantine"), 0700); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(root, "quarantine", "large"))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(RecoveryMaxBytes); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err = os.WriteFile(filepath.Join(root, "meta.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = reserveRecovery(root, "quarantine", []string{"meta.json"}); err == nil {
		t.Fatal("byte budget exceeded")
	}
	if b, err := os.ReadFile(filepath.Join(root, "meta.json")); err != nil || string(b) != "{}" {
		t.Fatal("source changed before budget rejection")
	}
}
