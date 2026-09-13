package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"

	"golang.org/x/term"
)

// Password bootstrap is intentionally a terminal-only boundary. The CLI
// parser never accepts a password value, and MCP/broker callers cannot invoke
// this path. The actual controlled terminal exchange is supplied by the host
// integration layer when available.
func cmdInteractiveSetup(command string, args []string) error {
	if len(args) != 1 || args[0] == "" {
		return fmt.Errorf("usage: rdev %s <host>", command)
	}
	if runtime.GOOS == "windows" {
		return errors.New("interactive password bootstrap is unsupported on Windows controller; configure the dedicated key manually")
	}
	stdin, err := os.Stdin.Stat()
	if err != nil {
		return err
	}
	if stdin.Mode()&os.ModeCharDevice == 0 {
		return errors.New("interactive setup requires a real terminal; password bootstrap is refused in non-interactive CLI, jobs and MCP")
	}
	return errors.New("interactive setup terminal flow is not available in this build; no password was read or transmitted; verify host key and use the documented dedicated-key bootstrap")
}

// readBootstrapPassword is deliberately usable only with a terminal file
// descriptor. It never accepts a password from argv, environment, or a pipe,
// and returns no diagnostic containing the secret.
func readBootstrapPassword(in *os.File, out *os.File) (string, error) {
	if in == nil || out == nil {
		return "", errors.New("interactive password bootstrap requires a terminal")
	}
	st, err := in.Stat()
	if err != nil || st.Mode()&os.ModeCharDevice == 0 || !term.IsTerminal(int(in.Fd())) {
		return "", errors.New("interactive password bootstrap requires a real terminal")
	}
	if _, err := fmt.Fprint(out, "Password: "); err != nil {
		return "", err
	}
	b, err := term.ReadPassword(int(in.Fd()))
	fmt.Fprintln(out)
	if err != nil {
		return "", errors.New("could not read password from terminal")
	}
	p := strings.TrimSuffix(string(b), "\r")
	if p == "" {
		return "", errors.New("password cannot be empty")
	}
	return p, nil
}
