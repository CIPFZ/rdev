package agentrepair

import (
	"context"
	"errors"
	"fmt"
)

// Hooks are supplied by the transport. Each hook must be idempotent for the
// transaction ID; this is what makes a lost reply safe to resume.
type Hooks struct {
	Lock      func(context.Context) (func(), error)
	Snapshot  func(context.Context) ([]byte, error)
	Install   func(context.Context, []byte) error
	Reconnect func(context.Context) error
	Rollback  func(context.Context, []byte) error
}

func Execute(ctx context.Context, p Plan, tx *Transaction, candidate []byte, confirm bool, h Hooks) error {
	if tx == nil || h.Lock == nil || h.Snapshot == nil || h.Install == nil || h.Reconnect == nil || h.Rollback == nil {
		return errors.New("repair execution hooks are incomplete")
	}
	if err := tx.Authorize(p, confirm); err != nil {
		return err
	}
	if err := tx.Advance(Authorized); err != nil {
		return err
	}
	release, err := h.Lock(ctx)
	if err != nil {
		return err
	}
	defer release()
	current, err := h.Snapshot(ctx)
	if err != nil {
		_ = tx.Abort()
		return err
	}
	if err = p.ValidateCandidate(current, candidate); err != nil {
		_ = tx.Abort()
		return err
	}
	if err = tx.Prepare(p, current); err != nil {
		_ = tx.Abort()
		return err
	}
	if err = h.Install(ctx, candidate); err != nil {
		if rb := h.Rollback(ctx, current); rb != nil {
			_ = tx.Abort()
			return fmt.Errorf("repair install failed: %w; rollback failed: %v", err, rb)
		}
		_ = tx.Abort()
		return err
	}
	if err = h.Reconnect(ctx); err != nil {
		if rb := h.Rollback(ctx, current); rb != nil {
			_ = tx.Abort()
			return fmt.Errorf("repair reconnect failed: %w; rollback failed: %v", err, rb)
		}
		_ = tx.Abort()
		return err
	}
	return tx.Advance(Committed)
}
