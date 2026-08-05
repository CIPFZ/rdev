// Supervisor mode for rdev-agent.
//
// A detached job cannot rely on the agent to record its exit code: the agent
// dies with the ssh connection, so its reaper goroutine usually never runs.
// Instead jobStart launches the agent binary in supervisor mode as the direct
// parent of the real command:
//
//	rdev-agent -supervise <jobdir> -- <argv...>
//
// The supervisor is part of the detached session, so it outlives the ssh
// connection, waits for the child, and writes status.json itself. Exit codes
// then survive disconnects, agent restarts, and host reboots.
//
// This keeps the no-shell guarantee: argv is passed as real arguments after
// "--" and exec'd directly.
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// superviseFlag is the argv[1] value that switches the binary into supervisor
// mode instead of serving the JSON protocol.
const superviseFlag = "-supervise"

// runSupervisor executes args[0:] as a child, waits for it, and records the
// outcome in <jobDir>/status.json. It never returns; it exits with the child's
// code so `ps` and any outer waiter see a faithful status.
func runSupervisor(jobDir string, argv []string) {
	if len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "rdev-agent -supervise: argv required after --")
		os.Exit(2)
	}

	cmd := exec.Command(argv[0], argv[1:]...)
	// Inherit the stdio the parent already redirected to the job's log files.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = nil
	// Keep the child in the supervisor's process group so that signalling the
	// group (job_stop uses -pgid) reaches supervisor and child together.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: false}

	if err := cmd.Start(); err != nil {
		writeJSON(filepath.Join(jobDir, "status.json"), map[string]any{
			"exit_code": -1,
			"ended_at":  time.Now().UTC().Format(time.RFC3339),
			"error":     fmt.Sprintf("start failed: %v", err),
		})
		os.Exit(127)
	}

	// Record the real child pid alongside the supervisor pid. Callers signal
	// the group, but the child pid is useful when diagnosing by hand.
	writeJSON(filepath.Join(jobDir, "child.json"), map[string]any{"child_pid": cmd.Process.Pid})

	err := cmd.Wait()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		code = -1
	}

	writeJSON(filepath.Join(jobDir, "status.json"), map[string]any{
		"exit_code": code,
		"ended_at":  time.Now().UTC().Format(time.RFC3339),
	})
	os.Exit(code)
}
