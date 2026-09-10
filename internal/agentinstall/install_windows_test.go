package agentinstall

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/winutil"
)

func windowsInstallFixture(t *testing.T) (string, string, artifact.Decision) {
	t.Helper()
	binary := os.Getenv("RDEV_WINDOWS_AGENT")
	if binary == "" {
		t.Fatal("RDEV_WINDOWS_AGENT must name the real built agent")
	}
	b, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "state")
	if err = winutil.EnsurePrivateDir(root); err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(root, "candidate.exe")
	if err = winutil.AtomicWrite(candidate, b); err != nil {
		t.Fatal(err)
	}
	return root, candidate, artifact.Decision{Digest: artifact.Hash(b), Unsigned: true, Version: "0.1.0-dev.1", Channel: "dev"}
}
func TestWindowsVersionedInstallAndRecovery(t *testing.T) {
	for _, point := range []string{"prepared", "verified", "switching", "published", "committed"} {
		t.Run(point, func(t *testing.T) {
			root, candidate, d := windowsInstallFixture(t)
			err := install(context.Background(), root, candidate, d, "", func(p string) error {
				if p == point {
					return errors.New("injected interruption")
				}
				return nil
			})
			if err == nil {
				t.Fatal("fault did not interrupt install")
			}
			if err = Recover(context.Background(), root); err != nil {
				t.Fatal(err)
			}
			tx := &transaction{dir: root}
			active, err := tx.decision(currentName)
			if point == "published" || point == "committed" {
				if err != nil || active.Digest != d.Digest {
					t.Fatalf("committed recovery: %+v %v", active, err)
				}
				if err = Health(context.Background(), tx.binary(d.Digest), root); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unpublished candidate activated: %+v %v", active, err)
			}
		})
	}
}
func TestWindowsInstallDigestFenceAndSignedDirection(t *testing.T) {
	root, candidate, d := windowsInstallFixture(t)
	d.Unsigned = false
	d.Version = "1.2.3"
	d.Channel = "stable"
	if err := Install(context.Background(), root, candidate, d, ""); err != nil {
		t.Fatal(err)
	}
	if err := Install(context.Background(), root, candidate, d, ""); err == nil {
		t.Fatal("stale installed digest accepted")
	}
	unsigned := d
	unsigned.Unsigned = true
	unsigned.Channel = "dev"
	unsigned.Version = "1.2.4-dev.0"
	if err := Install(context.Background(), root, candidate, unsigned, d.Digest); err == nil {
		t.Fatal("signed installation became unsigned")
	}
	if err := winutil.AtomicWrite(candidate, []byte("tampered")); err != nil {
		t.Fatal(err)
	}
	if err := Install(context.Background(), root, candidate, d, d.Digest); err == nil {
		t.Fatal("candidate digest mismatch accepted")
	}
}
