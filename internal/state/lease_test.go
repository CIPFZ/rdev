package state

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStateLeaseProcessHelper(t *testing.T) {
	root := os.Getenv("RDEV_TEST_STATE_LEASE_ROOT")
	if root == "" {
		return
	}
	lease, err := AcquireWriter(root)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	fmt.Println("lease-ready")
	var b [1]byte
	_, _ = os.Stdin.Read(b[:])
}

func TestWriterLeaseAcrossProcessAndCrash(t *testing.T) {
	root := privateTempDir(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStateLeaseProcessHelper$")
	cmd.Env = append(os.Environ(), "RDEV_TEST_STATE_LEASE_ROOT="+root)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	line, err := bufio.NewReader(out).ReadString('\n')
	if err != nil || line != "lease-ready\n" {
		t.Fatalf("helper barrier: %q %v", line, err)
	}
	// Independent open descriptions and a different spelling of the same root
	// share the inode, while another namespace remains usable.
	writer, err := AcquireWriter(root + "/.")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(root, false); !errors.Is(err, ErrMigrationLocked) {
		t.Fatalf("migration during writer: %v", err)
	}
	if _, err := Repair(root, false); !errors.Is(err, ErrMigrationLocked) {
		t.Fatalf("repair during writer: %v", err)
	}
	other := privateTempDir(t)
	if _, err := Migrate(other, false); err != nil {
		t.Fatalf("other namespace blocked: %v", err)
	}
	writer.Close()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if _, err := Migrate(root, false); err != nil {
		t.Fatalf("SIGKILL left stale kernel lease: %v", err)
	}
	if marker, err := os.ReadFile(filepath.Join(root, lockName)); err != nil || string(marker) != leaseMarker {
		t.Fatalf("permanent lease marker: %q %v", marker, err)
	}
	if _, err := os.OpenFile(filepath.Join(root, lockName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600); !errors.Is(err, os.ErrExist) {
		t.Fatalf("old O_EXCL migrator not fenced: %v", err)
	}
}

func TestStateLeaseRejectsUnsafeAndLegacyMarkers(t *testing.T) {
	for _, kind := range []string{"symlink", "broad-file", "broad-root", "legacy-marker", "initializer-collision"} {
		t.Run(kind, func(t *testing.T) {
			root := privateTempDir(t)
			p := filepath.Join(root, lockName)
			switch kind {
			case "symlink":
				outside := filepath.Join(privateTempDir(t), "lock")
				if err := os.WriteFile(outside, []byte(leaseMarker), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, p); err != nil {
					t.Fatal(err)
				}
			case "broad-file":
				if err := os.WriteFile(p, []byte(leaseMarker), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(p, 0644); err != nil {
					t.Fatal(err)
				}
			case "broad-root":
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal(err)
				}
			case "legacy-marker":
				if err := os.WriteFile(p, []byte("pid=123 token=legacy\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "initializer-collision":
				if err := os.WriteFile(p+".init", []byte("unknown existing state"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if lease, err := AcquireWriter(root); err == nil {
				lease.Close()
				t.Fatal("unsafe state lease accepted")
			}
		})
	}
}

func TestLeaseInitializationRecoveryAndConcurrentFirstWriters(t *testing.T) {
	root := privateTempDir(t)
	// The only prepublication crash residue is a fixed initializer; incomplete
	// bytes there are never confused with an old migrator's public marker.
	if err := os.WriteFile(filepath.Join(root, lockName+".init"), []byte("rdev-state-"), 0600); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	start := make(chan struct{})
	errorsCh := make(chan error, 16)
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			lease, err := AcquireWriter(root)
			if err == nil {
				err = lease.Close()
			}
			errorsCh <- err
		}()
	}
	close(start)
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent initialization: %v", err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 2 {
		t.Fatalf("lease initialization exceeded fixed budget: %v %v", entries, err)
	}
	a, err := os.Stat(filepath.Join(root, lockName))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(root, lockName+".init"))
	if err != nil || !os.SameFile(a, b) {
		t.Fatal("initializer and lease do not bind one inode")
	}
}

func TestMigrationRechecksFutureStateAfterLease(t *testing.T) {
	root := privateTempDir(t)
	dir := filepath.Join(root, "jobs", "legacy")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	record := []byte(`{"id":"legacy"}`)
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), record, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(root, true); err != nil {
		t.Fatal(err)
	}
	release, err := acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	future := []byte(`{"schema_version":99}`)
	if err := os.WriteFile(filepath.Join(root, manifestName), future, 0600); err != nil {
		t.Fatal(err)
	}
	release()
	if _, err := Migrate(root, false); !errors.Is(err, ErrFutureSchema) {
		t.Fatalf("stale inspection accepted: %v", err)
	}
	if _, err := Repair(root, false); err == nil {
		t.Fatal("repair accepted future manifest")
	}
	if got, _ := os.ReadFile(filepath.Join(root, manifestName)); string(got) != string(future) {
		t.Fatal("future manifest changed")
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "meta.json")); string(got) != string(record) {
		t.Fatal("legacy record changed under future manifest")
	}
}

func TestStrictStateMetadataAndFuturePreservation(t *testing.T) {
	for _, data := range []string{`{"schema_version":0}`, `{"schema_version":null}`, `{"schema_version":2}`, `{"schema_version":2,"schema_version":1}`, `{"SCHEMA_VERSION":2}`, `null`, `[]`, `{"schema_version":1} {}`} {
		if _, err := ValidateRecordSchema([]byte(data)); err == nil {
			t.Errorf("record accepted: %s", data)
		}
	}
	for _, data := range []string{`{"schema_version":1,"schema_version":2}`, `{"schema_version":1,"unknown":true}`, `{"SCHEMA_VERSION":1}`, `null`} {
		root := privateTempDir(t)
		if err := os.WriteFile(filepath.Join(root, manifestName), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := CheckCompatible(root); err == nil {
			t.Errorf("manifest accepted: %s", data)
		}
	}
	root := privateTempDir(t)
	dir := filepath.Join(root, "jobs", "future")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	future := []byte(`{"schema_version":2,"id":"future"}`)
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), future, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Migrate(root, false); !errors.Is(err, ErrFutureSchema) {
		t.Fatalf("future record migrated: %v", err)
	}
	if _, err := Repair(root, false); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "meta.json")); string(got) != string(future) {
		t.Fatal("future record quarantined or rewritten")
	}
	if _, err := ValidateRecordSchema([]byte(strings.Repeat(" ", MaxMetadataBytes+1))); err == nil {
		t.Fatal("unbounded record accepted")
	}
}

func FuzzStateMetadata(f *testing.F) {
	for _, seed := range []string{`{"schema_version":1}`, `{"id":"legacy"}`, `{"schema_version":2,"schema_version":1}`, `null`, "{"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		version, err := ValidateRecordSchema(data)
		if err == nil && (version < 0 || version > CurrentSchemaVersion) {
			t.Fatal("unsupported schema accepted")
		}
		var manifest Manifest
		_ = decodeMetadata(data, &manifest, true)
	})
}
