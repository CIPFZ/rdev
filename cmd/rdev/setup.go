package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/CIPFZ/rdev/internal/bootstrap"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/keys"
	"github.com/CIPFZ/rdev/internal/transport"
	"golang.org/x/crypto/ssh"
	"golang.org/x/term"
)

// Password bootstrap is intentionally a terminal-only boundary. The CLI
// parser never accepts a password value, and MCP/broker callers cannot invoke
// this path. The actual controlled terminal exchange is supplied by the host
// integration layer when available.
func cmdInteractiveSetup(c *client.Client, command string, args []string) error {
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
	if strings.HasSuffix(command, " remove") {
		return cmdInteractiveRevoke(c, args[0])
	}
	h, err := c.Hosts.Host(args[0])
	if err != nil {
		return err
	}
	addr, port, err := transport.ParseDestination(h.Addr, h.Port)
	if err != nil {
		return err
	}
	user := os.Getenv("USER")
	if before, _, ok := strings.Cut(addr, "@"); ok {
		user, addr = before, strings.TrimPrefix(addr, before+"@")
	}
	if user == "" {
		return errors.New("host must specify an SSH user (user@host)")
	}
	sshAddr := addr
	if port != 0 {
		sshAddr = fmt.Sprintf("%s:%d", addr, port)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	known := filepath.Join(home, ".ssh", "known_hosts")
	cb, err := bootstrap.KnownHostsCallback(known)
	if err != nil {
		return err
	}
	root, err := keys.DefaultRoot()
	if err != nil {
		return err
	}
	id, err := keys.Open(root, user, addr, port, "default")
	if err != nil {
		return err
	}
	if _, err := os.Stat(id.Private); errors.Is(err, os.ErrNotExist) {
		if err := id.Generate(); err != nil {
			return err
		}
	}
	if err := id.Validate(); err != nil {
		return err
	}
	privRaw, err := os.ReadFile(id.Private)
	if err != nil {
		return err
	}
	pubRaw, err := os.ReadFile(id.Public)
	if err != nil {
		return err
	}
	privPEM, err := ssh.MarshalPrivateKey(ed25519.PrivateKey(privRaw), "rdev")
	if err != nil {
		return err
	}
	pubKey, err := ssh.NewPublicKey(ed25519.PublicKey(pubRaw))
	if err != nil {
		return err
	}
	pubLine := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pubKey)))
	fmt.Fprintf(os.Stdout, "Target: %s@%s", user, sshAddr)
	fmt.Fprintf(os.Stdout, "\nChange: append this dedicated key to %s@%s:$HOME/.ssh/authorized_keys", user, sshAddr)
	fmt.Fprintf(os.Stdout, "\nFingerprint: %s\nType 'yes' to continue: ", bootstrap.Fingerprint(pubRaw))
	confirmation, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil {
		return errors.New("could not read confirmation from terminal")
	}
	if strings.TrimSpace(confirmation) != "yes" {
		return errors.New("bootstrap cancelled; type exactly yes to authorize the authorized_keys change")
	}
	password, err := readBootstrapPassword(os.Stdin, os.Stdout)
	if err != nil {
		return err
	}
	defer func() { password = "" }()
	if err := bootstrap.Run(context.Background(), bootstrap.Config{Address: sshAddr, User: user, Password: password, PublicKey: []byte(pubLine), PrivateKey: pem.EncodeToMemory(privPEM), HostKeyCallback: cb}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "dedicated key installed and verified for %s\n", args[0])
	return nil
}

func cmdInteractiveRevoke(c *client.Client, name string) error {
	h, err := c.Hosts.Host(name)
	if err != nil {
		return err
	}
	addr, port, err := transport.ParseDestination(h.Addr, h.Port)
	if err != nil {
		return err
	}
	user := os.Getenv("USER")
	if before, _, ok := strings.Cut(addr, "@"); ok {
		user, addr = before, strings.TrimPrefix(addr, before+"@")
	}
	if user == "" {
		return errors.New("host must specify an SSH user (user@host)")
	}
	sshAddr := addr
	if port != 0 {
		sshAddr = fmt.Sprintf("%s:%d", addr, port)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	cb, err := bootstrap.KnownHostsCallback(filepath.Join(home, ".ssh", "known_hosts"))
	if err != nil {
		return err
	}
	root, err := keys.DefaultRoot()
	if err != nil {
		return err
	}
	id, err := keys.Open(root, user, addr, port, "default")
	if err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return err
	}
	privRaw, err := os.ReadFile(id.Private)
	if err != nil {
		return err
	}
	pubRaw, err := os.ReadFile(id.Public)
	if err != nil {
		return err
	}
	priv, err := ssh.MarshalPrivateKey(ed25519.PrivateKey(privRaw), "rdev")
	if err != nil {
		return err
	}
	pubKey, err := ssh.NewPublicKey(ed25519.PublicKey(pubRaw))
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Target: %s@%s\nChange: remove only this rdev key from $HOME/.ssh/authorized_keys\nFingerprint: %s\nType 'yes' to continue: ", user, sshAddr, bootstrap.Fingerprint(pubRaw))
	confirmation, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil || strings.TrimSpace(confirmation) != "yes" {
		return errors.New("revocation cancelled; type exactly yes to authorize")
	}
	if err := bootstrap.Revoke(context.Background(), bootstrap.Config{Address: sshAddr, User: user, PublicKey: ssh.MarshalAuthorizedKey(pubKey), PrivateKey: pem.EncodeToMemory(priv), HostKeyCallback: cb}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "dedicated key revoked for %s\n", name)
	return nil
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
