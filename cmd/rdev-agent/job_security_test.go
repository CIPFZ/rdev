package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestJobIDRejectsPathBearingValues(t *testing.T) {
	state := t.TempDir()
	for _, id := range []string{"../escape", "..", "/tmp/x", `a\\b`, "a\x00b", "a/b"} {
		if _, err := jobStatus(id, state); err == nil {
			t.Errorf("jobStatus accepted malicious id %q", id)
		}
	}
	outside := filepath.Join(state, "escape")
	if _, err := os.Stat(outside); !os.IsNotExist(err) {
		t.Fatalf("malicious id created or reached %s", outside)
	}
}

func TestJobStartUsesPrivateRecordsAndIdentity(t *testing.T) {
	state := t.TempDir()
	res, err := jobStart(&proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sleep", "2"}}}, state)
	if err != nil {
		t.Fatal(err)
	}
	defer jobStop(&proto.JobParams{ID: res.Info.ID, Signal: "KILL"}, state)
	dir := jobDir(state, res.Info.ID)
	for _, tc := range []struct {
		path string
		mode os.FileMode
	}{
		{filepath.Join(state, "jobs"), 0o700}, {dir, 0o700},
		{filepath.Join(dir, "meta.json"), 0o600}, {filepath.Join(dir, "stdout"), 0o600},
		{filepath.Join(dir, "stderr"), 0o600},
	} {
		st, statErr := os.Stat(tc.path)
		if statErr != nil {
			t.Fatalf("stat %s: %v", tc.path, statErr)
		}
		if got := st.Mode().Perm(); got != tc.mode {
			t.Errorf("%s mode %o, want %o", tc.path, got, tc.mode)
		}
	}
	if res.Info.StartedAt == "" {
		t.Error("missing StartedAt")
	}
	meta, err := readMeta(dir)
	if err != nil {
		t.Fatal(err)
	}
	if meta.ProcessIdentity == "" {
		t.Error("missing process identity")
	}
}

func TestJobStopRefusesIdentityMismatch(t *testing.T) {
	state := t.TempDir()
	res, err := jobStart(&proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sleep", "3"}}}, state)
	if err != nil {
		t.Fatal(err)
	}
	dir := jobDir(state, res.Info.ID)
	meta, err := readMeta(dir)
	if err != nil {
		t.Fatal(err)
	}
	meta.ProcessIdentity = "definitely-stale"
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		t.Fatal(err)
	}
	if _, err := jobStop(&proto.JobParams{ID: res.Info.ID, Signal: "KILL"}, state); err == nil {
		t.Fatal("jobStop accepted a stale process identity")
	}
	if !processAlive(meta.PID) {
		t.Fatal("identity mismatch stop killed the live process")
	}
	// Restore the real token so cleanup can safely stop the process.
	meta.ProcessIdentity, _ = processIdentity(meta.PID)
	_ = writeJSON(filepath.Join(dir, "meta.json"), meta)
	_, _ = jobStop(&proto.JobParams{ID: res.Info.ID, Signal: "KILL"}, state)
}
