//go:build !windows

package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	// Write-then-rename so a reader never observes a partial record.
	//
	// The temp name carries the pid: a fixed "<path>.tmp" is shared state between
	// every writer of that path, and two of them interleaving would let one
	// rename the other's half-written bytes into place. Writers of one job's
	// status now hold the job lock, but the supervisor writes child.json outside
	// it, and a unique name costs nothing.
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp) // do not leave the temp file behind on a failed rename
		return err
	}
	return nil
}
