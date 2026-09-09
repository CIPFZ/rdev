package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	statepkg "github.com/CIPFZ/rdev/internal/state"
)

func TestFutureManifestRejectsStateWritersBeforeEffects(t *testing.T) {
	for _, manifest := range []string{`{"schema_version":99}`, `{"schema_version":1,"schema_version":99}`, `{"schema_version":1,"unknown":true}`, `{`} {
		t.Run(manifest, func(t *testing.T) {
			root := privateTempDir(t)
			if err := os.Mkdir(filepath.Join(root, "jobs"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "manifest.json"), []byte(manifest), 0600); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(privateTempDir(t), "business-marker")
			requests := []*proto.Request{
				{ID: "start", Op: proto.OpJobStart, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", `printf once > "$1"`, "state-test", marker}}}},
				{ID: "stop", Op: proto.OpJobStop, Job: &proto.JobParams{ID: "future", Signal: "TERM"}},
				{ID: "rm", Op: proto.OpJobRm, Job: &proto.JobParams{ID: "future"}},
				{ID: "gc", Op: proto.OpStorageGC, Storage: &proto.StorageParams{}},
				{ID: "sync", Op: proto.OpSyncStage, ClientID: "client_test", ProjectID: "project", Sync: &proto.SyncParams{Action: "begin"}},
			}
			for _, req := range requests {
				response := handleContext(context.Background(), req, root, defaultWaitHub)
				if response.OK || response.Error == nil || response.Execution != proto.StateNotSent {
					t.Fatalf("%s failed open: %+v", req.Op, response)
				}
			}
			if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("rejected writer ran business command")
			}
			jobs, err := os.ReadDir(filepath.Join(root, "jobs"))
			if err != nil || len(jobs) != 0 {
				t.Fatal("rejected start created job state")
			}
			if _, err := os.Lstat(filepath.Join(root, ".sync")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("rejected sync created stage")
			}
			if got, _ := os.ReadFile(filepath.Join(root, "manifest.json")); string(got) != manifest {
				t.Fatal("manifest changed")
			}
			inspect := handle(&proto.Request{Op: proto.OpStateInspect, State: &proto.StateParams{}}, root)
			if !inspect.OK || inspect.State == nil || len(inspect.State.Findings) == 0 {
				t.Fatal("incompatible state lost inspection path")
			}
		})
	}
}

func TestFutureJobMetadataCannotBeStoppedOrRemoved(t *testing.T) {
	for _, record := range []string{`{"schema_version":99,"id":"future","pid":0}`, `{"schema_version":0,"id":"future","pid":0}`, `{"schema_version":99,"schema_version":1,"id":"future","pid":0}`} {
		root := privateTempDir(t)
		dir := filepath.Join(root, "jobs", "future")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(record), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := jobStop(&proto.JobParams{ID: "future", Signal: "TERM"}, root); err == nil {
			t.Fatal("future job stop accepted")
		}
		if _, err := jobRm(&proto.JobParams{ID: "future"}, root); err == nil {
			t.Fatal("future job removed")
		}
		if _, err := readMeta(dir); err == nil {
			t.Fatal("future record read as current")
		}
		for _, op := range []string{proto.OpJobStop, proto.OpJobRm} {
			response := handle(&proto.Request{Op: op, Job: &proto.JobParams{ID: "future", Signal: "TERM"}}, root)
			if response.OK || response.Execution != proto.StateNotSent {
				t.Fatalf("%s schema rejection lost no-effect semantics: %+v", op, response)
			}
		}
		if got, _ := os.ReadFile(filepath.Join(dir, "meta.json")); string(got) != record {
			t.Fatal("future metadata changed")
		}
	}
}

func TestDetachedSupervisorFencesMigrationWithoutKillingJob(t *testing.T) {
	root := privateTempDir(t)
	started, err := jobStart(&proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sleep", "30"}}}, root)
	if err != nil {
		t.Fatal(err)
	}
	id := started.Info.ID
	t.Cleanup(func() { _, _ = jobStop(&proto.JobParams{ID: id, Signal: "KILL"}, root) })
	// jobStart already returned and released both parent descriptors. The
	// child's inherited description alone must still fence migration.
	if _, err := statepkg.Migrate(root, false); !errors.Is(err, statepkg.ErrMigrationLocked) {
		t.Fatalf("active supervisor did not fence migration: %v", err)
	}
	if _, err := statepkg.Repair(root, false); !errors.Is(err, statepkg.ErrMigrationLocked) {
		t.Fatalf("active supervisor did not fence repair: %v", err)
	}
	status, err := jobStatus(id, root)
	if err != nil || status.State != proto.JobRunning {
		t.Fatalf("migration killed or lost job: %+v %v", status, err)
	}
	if _, err := jobStop(&proto.JobParams{ID: id, Signal: "TERM", GraceSec: 1}, root); err != nil {
		t.Fatal(err)
	}
	if _, err := jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 5}, root); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err = statepkg.Migrate(root, false)
		if err == nil {
			break
		}
		if !errors.Is(err, statepkg.ErrMigrationLocked) || time.Now().After(deadline) {
			t.Fatalf("finished supervisor stranded migration: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := jobStatus(id, root); err != nil {
		t.Fatalf("migration lost completed job: %v", err)
	}
}
