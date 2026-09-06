package broker

import (
	"path/filepath"
	"testing"
)

func TestJobRegistrySurvivesServiceBoundary(t *testing.T) {
	r := NewJobRegistry()
	r.Put(JobRef{ID: "j", Owner: "c\x00p", Host: "h"})
	if j, ok := r.Get("j"); !ok || j.Owner != "c\x00p" {
		t.Fatal(j, ok)
	}
	if len(r.Snapshot()) != 1 {
		t.Fatal("snapshot")
	}
}

func TestJobRegistrySaveLoad(t *testing.T) {
	p := filepath.Join(t.TempDir(), "jobs.json")
	r := NewJobRegistry()
	r.Put(JobRef{ID: "j", Owner: "o", Host: "h"})
	if err := r.Save(p); err != nil {
		t.Fatal(err)
	}
	out := NewJobRegistry()
	if err := out.Load(p); err != nil {
		t.Fatal(err)
	}
	if _, ok := out.Get("j"); !ok {
		t.Fatal("job not restored")
	}
}

func TestJobRegistryLoadReplacesStaleStateOnRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "jobs.json")
	persisted := NewJobRegistry()
	persisted.Put(JobRef{ID: "live", Owner: "o", Host: "h"})
	if err := persisted.Save(p); err != nil {
		t.Fatal(err)
	}
	restarted := NewJobRegistry()
	restarted.Put(JobRef{ID: "stale", Owner: "o", Host: "h"})
	if err := restarted.Load(p); err != nil {
		t.Fatal(err)
	}
	if _, ok := restarted.Get("stale"); ok {
		t.Fatal("stale job survived restart load")
	}
	if _, ok := restarted.Get("live"); !ok {
		t.Fatal("persisted job missing after restart load")
	}
}
