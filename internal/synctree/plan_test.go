package synctree

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCapturedPlanRetainsSourceAndDeletesExactEntries(t *testing.T) {
	source := t.TempDir()
	dest := t.TempDir()
	state := privateTemp(t)
	write(t, filepath.Join(source, "file"), "approved content")
	write(t, filepath.Join(dest, "file"), "old content")
	if err := os.Mkdir(filepath.Join(dest, "obsolete"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dest, "obsolete", "old"), "remove")
	captured, manifest, err := Capture(t.Context(), source, state, "preserve", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := Inspect(t.Context(), dest, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Build(manifest, snapshot, true, "overwrite")
	if err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(source, "file"), "source edited after approval")
	if err := Apply(t.Context(), captured, dest, plan, Limits{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "file"))
	if err != nil || string(data) != "approved content" {
		t.Fatal("source edit affected retained plan", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "obsolete")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("planned deletion missing")
	}
	if err := Apply(t.Context(), captured, dest, plan, Limits{}); !errors.Is(err, ErrChanged) {
		t.Fatal("changed target permitted replay", err)
	}
}

func TestPlanRejectsTargetAndStagingChangesBeforeWrites(t *testing.T) {
	for _, which := range []string{"target", "staging", "replacement"} {
		t.Run(which, func(t *testing.T) {
			source := t.TempDir()
			dest := t.TempDir()
			state := privateTemp(t)
			write(t, filepath.Join(source, "new"), "new content")
			captured, manifest, err := Capture(t.Context(), source, state, "preserve", Limits{})
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := Inspect(t.Context(), dest, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := Build(manifest, snapshot, true, "overwrite")
			if err != nil {
				t.Fatal(err)
			}
			switch which {
			case "target":
				write(t, filepath.Join(dest, "unapproved"), "keep")
			case "staging":
				write(t, filepath.Join(captured, "new"), "tampered content")
			case "replacement":
				if err := os.Rename(dest, dest+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(dest, 0700); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { os.RemoveAll(dest + "-old") })
			}
			if err := Apply(t.Context(), captured, dest, plan, Limits{}); !errors.Is(err, ErrChanged) {
				t.Fatal("substitution accepted", err)
			}
			if _, err := os.Stat(filepath.Join(dest, "new")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("rejection modified target")
			}
		})
	}
}

func TestPlanDirectoryReplacementAndCreation(t *testing.T) {
	source := t.TempDir()
	parent := t.TempDir()
	dest := filepath.Join(parent, "new")
	write(t, filepath.Join(source, "file"), "payload")
	captured, m, err := Capture(t.Context(), source, privateTemp(t), "preserve", Limits{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := Inspect(t.Context(), dest, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Build(m, snap, false, "overwrite")
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(t.Context(), captured, dest, plan, Limits{}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dest, "file")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dest, "file"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dest, "file", "nested"), "old")
	snap, err = Inspect(t.Context(), dest, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err = Build(m, snap, false, "overwrite")
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(t.Context(), captured, dest, plan, Limits{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, "file"))
	if err != nil || string(data) != "payload" {
		t.Fatal(err)
	}
}

func TestCaptureCancellationBudgetAndPrivateRoot(t *testing.T) {
	source := t.TempDir()
	parent := t.TempDir()
	write(t, filepath.Join(source, "file"), "long content")
	if _, _, err := Capture(t.Context(), source, parent, "preserve", Limits{ContentBytes: 2}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := Capture(ctx, source, parent, "preserve", Limits{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatal("failed capture retained private files")
	}
	public := filepath.Join(parent, "public")
	if err := os.Mkdir(public, 0755); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Capture(t.Context(), source, public, "preserve", Limits{}); err == nil {
		t.Fatal("public staging root accepted")
	}
}

func privateTemp(t *testing.T) string {
	t.Helper()
	p := t.TempDir()
	if err := os.Chmod(p, 0700); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestScopedPlanCannotInferProtectedReplacementChildren(t *testing.T) {
	source, destination := t.TempDir(), t.TempDir()
	write(t, filepath.Join(source, "node"), "replacement")
	if err := os.Mkdir(filepath.Join(destination, "node"), 0700); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(destination, "node", "protected"), "keep")
	manifest, err := Scan(t.Context(), source, "preserve", StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := Inspect(t.Context(), destination, StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BuildScoped(manifest, snapshot, nil, "overwrite"); err == nil {
		t.Fatal("replacement escaped explicit removal scope")
	}
	if _, err := BuildScoped(manifest, snapshot, map[string]bool{"node/protected": true}, "overwrite"); err != nil {
		t.Fatal(err)
	}
}

func TestApplyNormalizesDestinationParent(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprint(existing), func(t *testing.T) {
			source := t.TempDir()
			write(t, filepath.Join(source, "file"), "retained")
			destination := filepath.Join(t.TempDir(), "destination")
			if existing {
				if err := os.Mkdir(destination, 0700); err != nil {
					t.Fatal(err)
				}
			}
			captured, manifest, err := Capture(t.Context(), source, privateTemp(t), "preserve", StageLimits)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := Inspect(t.Context(), destination+"/", StageLimits)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := Build(manifest, snapshot, false, "overwrite")
			if err != nil {
				t.Fatal(err)
			}
			if err := Apply(t.Context(), captured, destination+"/", plan, StageLimits); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(filepath.Join(destination, "file")); err != nil || string(data) != "retained" {
				t.Fatal(err)
			}
		})
	}
}
