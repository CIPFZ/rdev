package session

import (
	"errors"
	"fmt"
)

// ConfigWriteAmbiguousError means the atomic replacement crossed rename but
// rollback could not be durably verified. Callers must stop normal service
// rather than guess whether disk contains the old or new authorization state.
type ConfigWriteAmbiguousError struct {
	Cause    error
	Rollback error
}

func (e *ConfigWriteAmbiguousError) Error() string {
	return fmt.Sprintf("config write outcome is ambiguous after %v; rollback could not be verified: %v", e.Cause, e.Rollback)
}

func (e *ConfigWriteAmbiguousError) Unwrap() error { return e.Cause }

// ConfigWriteCommittedError is a post-commit cleanup warning. The new bytes
// are durably committed and callers must publish the matching in-memory state.
type ConfigWriteCommittedError struct {
	Cause error
}

func (e *ConfigWriteCommittedError) Error() string {
	return fmt.Sprintf("config write committed, but cleanup requires attention: %v", e.Cause)
}

func (e *ConfigWriteCommittedError) Unwrap() error { return e.Cause }

func configWriteAmbiguous(err error) bool {
	var target *ConfigWriteAmbiguousError
	return errors.As(err, &target)
}

func configWriteCommitted(err error) bool {
	_, ok := ConfigWriteCommittedWarning(err)
	return ok
}

// ConfigWriteCommittedWarning projects a committed cleanup outcome for CLI and
// MCP callers. They must report success plus this warning, not retry approval as
// though the durable commit had failed.
func ConfigWriteCommittedWarning(err error) (string, bool) {
	var target *ConfigWriteCommittedError
	if !errors.As(err, &target) {
		return "", false
	}
	return target.Error(), true
}
