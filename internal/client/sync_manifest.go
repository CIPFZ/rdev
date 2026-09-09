package client

import (
	"context"
	"fmt"

	"github.com/CIPFZ/rdev/internal/synctree"
)

// SharedSyncManifestMemoryBudget covers both scans' entry/path allocations,
// bounded directory batches, open-directory buffers and hashing scratch space.
// Standalone scans retain the larger synctree defaults.
const SharedSyncManifestMemoryBudget int64 = 16 << 20

var sharedSyncManifestLimits = synctree.Limits{Entries: 8192, MetadataBytes: 2 << 20}

// syncManifest describes a bounded observation, not an immutable staged source.
// Shared mutation requires a separately retained snapshot and destination plan.
type syncManifest struct {
	Digest   string
	Entries  int
	Complete bool
}

func buildSyncManifest(root, policy string) (syncManifest, error) {
	return buildSyncManifestContext(context.Background(), root, policy)
}
func buildSyncManifestContext(ctx context.Context, root, policy string) (syncManifest, error) {
	return scanSyncManifest(ctx, root, policy, synctree.Limits{})
}
func scanSyncManifest(ctx context.Context, root, policy string, limits synctree.Limits) (syncManifest, error) {
	manifest, err := synctree.Scan(ctx, root, policy, limits)
	if err != nil {
		return syncManifest{}, err
	}
	return syncManifest{Digest: manifest.Digest, Entries: len(manifest.Entries), Complete: true}, nil
}
func verifySyncManifest(root, policy string, expected syncManifest) error {
	return verifySyncManifestContext(context.Background(), root, policy, expected)
}
func verifySyncManifestContext(ctx context.Context, root, policy string, expected syncManifest) error {
	if expected.Digest == "" {
		return nil
	}
	got, err := buildSyncManifestContext(ctx, root, policy)
	if err != nil {
		return err
	}
	if got.Digest != expected.Digest || got.Entries != expected.Entries {
		return fmt.Errorf("sync manifest source changed")
	}
	return nil
}
