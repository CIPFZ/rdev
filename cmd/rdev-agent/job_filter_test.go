package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestJobListScopePrecedesPaginationAndTotals(t *testing.T) {
	state := t.TempDir()
	for i, id := range []string{"owner-old", "owner-new", "other-newest"} {
		dir := filepath.Join(state, "jobs", id)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
			ID: id, PID: 999999, Argv: []string{"x"},
			StartedAt: time.Date(2026, 1, 1, 0, i, 0, 0, time.UTC).Format(time.RFC3339),
		}); err != nil {
			t.Fatal(err)
		}
	}
	res, err := jobList(&proto.JobParams{FilterIDs: true, IDs: []string{"owner-old", "owner-new"}, Limit: 1}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.List) != 1 || res.List[0].ID != "owner-new" || res.Total != 2 || !res.Truncated {
		t.Fatalf("scoped page = %+v", res)
	}
	res, err = jobList(&proto.JobParams{FilterIDs: true}, state)
	if err != nil || len(res.List) != 0 || res.Total != 0 || res.Truncated {
		t.Fatal("empty scope exposed jobs")
	}
	if _, err := jobList(&proto.JobParams{FilterIDs: true, IDs: []string{"../outside"}}, state); err == nil {
		t.Fatal("invalid scoped ID accepted")
	}
}
