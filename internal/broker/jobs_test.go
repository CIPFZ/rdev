package broker

import "testing"

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
