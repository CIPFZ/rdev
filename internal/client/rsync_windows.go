//go:build windows

package client

import (
	"context"
	"io"
	"os/exec"
	"time"
)

func runRsync(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, "rsync", args...)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.WaitDelay = 2 * time.Second
	return cmd.Run()
}
