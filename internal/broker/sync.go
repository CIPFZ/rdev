package broker

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
)

func isSyncOperation(op string) bool { return op == "sync.push" || op == "sync.pull" }

func validateSyncRoute(req Request) error {
	if req.Wire != nil || req.Secret != nil || req.Sync == nil || req.Host == "" || req.Sync.Host != "" {
		return errors.New("sync requires an exact outer host and sync parameters without a wire request")
	}
	if !req.Sync.DryRun {
		return errors.New("shared sync execution requires a prepared manifest; use dry_run to preview")
	}
	if req.Sync.Direction != strings.TrimPrefix(req.Operation, "sync.") || !filepath.IsAbs(req.Sync.Local) {
		return errors.New("sync direction must match the operation and local path must be absolute")
	}
	_, err := client.NormalizeSyncOptions(*req.Sync)
	return err
}

// SyncPreviewBudget reserves both bounded stdout/stderr captures and their
// worst-case JSON encoding. It stays charged until the process and captures
// finish, even when the frontend disconnects during scheduler execution.
func SyncPreviewBudget(req Request) int64 {
	if req.Sync == nil {
		return 0
	}
	opts, err := client.NormalizeSyncOptions(*req.Sync)
	if err != nil {
		return 0
	}
	return 16 * opts.MaxOutputBytes
}

func SyncPreviewWorkerBudget(req Request) int64 {
	budget := SyncPreviewBudget(req)
	if req.Sync != nil && req.Sync.Direction == "push" {
		budget += client.SharedSyncManifestMemoryBudget
	}
	return budget
}

func (s *Service) PreviewSync(ctx context.Context, req Request) (*client.SyncResult, error) {
	if err := validateSyncRoute(req); err != nil {
		return nil, err
	}
	target, err := s.client.ProtocolTargetIdentity(req.Host)
	if err != nil {
		return nil, errors.New("sync host unavailable")
	}
	// Scheduling may return cancellation before its worker exits. The worker
	// owns its capture reservation; the result channel avoids sharing a mutable
	// result pointer with a frontend that has already returned.
	results := make(chan *client.SyncResult, 1)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = s.DispatchScheduled(ctx, req.Host, req.Owner.Key(), LaneBulk, func(workCtx context.Context) (*proto.Response, error) {
		releaseBudget, err := s.Ingress.Hold(req.Owner.Key(), SyncPreviewWorkerBudget(req))
		if err != nil {
			return nil, err
		}
		defer releaseBudget()
		release, err := s.Pool.dispatchLease(workCtx, req.Host, LaneBulk)
		if err != nil {
			return nil, err
		}
		defer release()
		opts := *req.Sync
		opts.Host = req.Host
		result, err := s.client.PreviewSync(workCtx, opts, target)
		if err == nil {
			results <- result
		}
		return nil, err
	})
	if err != nil {
		return nil, err
	}
	return <-results, nil
}
