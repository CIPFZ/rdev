package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/bootstrap"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/keys"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
	"golang.org/x/crypto/ssh"
	"golang.org/x/term"
)

// Password bootstrap never accepts a password value in argv, env, stdin,
// logs, or protocol output. Agents provide it through an inherited private
// pipe descriptor; terminals remain supported for local diagnostics.
const minAgentPasswordFD = 10

func cmdInteractiveSetup(c *client.Client, command string, args []string) error {
	passwordFD := -1
	confirmed := false
	if len(args) > 1 {
		for i := 1; i < len(args); i++ {
			switch args[i] {
			case "-confirm":
				confirmed = true
			case "-password-fd":
				if i+1 >= len(args) {
					return errors.New("-password-fd requires a descriptor")
				}
				var parseErr error
				passwordFD, parseErr = strconv.Atoi(args[i+1])
				if parseErr != nil || passwordFD < minAgentPasswordFD || passwordFD > 255 {
					return fmt.Errorf("password-fd must be an inherited descriptor between %d and 255", minAgentPasswordFD)
				}
				i++
			default:
				return fmt.Errorf("unknown setup flag %q", args[i])
			}
		}
		args = args[:1]
	}
	if len(args) != 1 || args[0] == "" {
		return fmt.Errorf("usage: rdev %s <host> [-password-fd FD]", command)
	}
	if runtime.GOOS == "windows" {
		return errors.New("interactive password bootstrap is unsupported on Windows controller; configure the dedicated key manually")
	}
	stdin, err := os.Stdin.Stat()
	if err != nil {
		return err
	}
	if stdin.Mode()&os.ModeCharDevice == 0 && passwordFD < 0 && !confirmed {
		return errors.New("agent setup requires -password-fd FD and -confirm when stdin is not a terminal")
	}
	if strings.HasSuffix(command, " remove") {
		return cmdInteractiveRevoke(c, args[0])
	}
	if command == "setup" {
		trust := c.Hosts.ProjectTrustStatus()
		if trust.Path != "" && !trust.Approved {
			if !confirmed {
				return fmt.Errorf("setup stage project approval required for %s (sha256:%s); review it, then run `rdev hosts approve-project %s` or retry with agent -confirm", trust.Path, trust.Digest, trust.Digest)
			}
			if _, err := c.Hosts.ApproveProject(trust.Digest); err != nil {
				return fmt.Errorf("setup stage project approval failed: %w", err)
			}
			fmt.Fprintln(os.Stdout, "setup stage project approval passed")
		}
		policy := artifact.DiagnosePolicy(time.Now())
		if !policy.Valid {
			return fmt.Errorf("setup stage policy failed: %s; action: %s", policy.Error, policy.Action)
		}
		fmt.Fprintln(os.Stdout, "setup stage policy passed")
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
	if !confirmed {
		confirmation, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			return errors.New("could not read confirmation from terminal")
		}
		confirmed = strings.TrimSpace(confirmation) == "yes"
	}
	if !confirmed {
		return errors.New("bootstrap cancelled; type exactly yes to authorize the authorized_keys change")
	}
	var password string
	if passwordFD >= 0 {
		password, err = readBootstrapPasswordFD(passwordFD)
	} else {
		password, err = readBootstrapPassword(os.Stdin, os.Stdout)
	}
	if err != nil {
		return err
	}
	defer func() { password = "" }()
	if err := bootstrap.Run(context.Background(), bootstrap.Config{Address: sshAddr, User: user, Password: password, PublicKey: []byte(pubLine), PrivateKey: pem.EncodeToMemory(privPEM), HostKeyCallback: cb}); err != nil {
		return err
	}
	if id.OpenSSHPrivate != "" {
		h.IdentityFile = id.OpenSSHPrivate
		if _, err := c.Hosts.ApplyHostUpdate(session.HostUpdate{Name: args[0], Host: &h, Persist: true}); err != nil {
			return fmt.Errorf("dedicated key installed but saving identity selection failed: %w", err)
		}
	}
	fmt.Fprintf(os.Stdout, "dedicated key installed and verified for %s\n", args[0])
	if command == "setup" {
		if _, err := c.Ping(context.Background(), args[0]); err != nil {
			return fmt.Errorf("setup stage ping failed after key bootstrap: %w; key remains installed, retry ping after configuring SSH key selection", err)
		}
		fmt.Fprintf(os.Stdout, "setup stage ping passed for %s\n", args[0])
	}
	return nil
}

// readBootstrapPasswordFD is the agent boundary. The caller must explicitly
// inherit a private pipe descriptor; the secret is never in argv, env, stdin,
// logs or a file. A bounded read prevents an agent from smuggling a large
// payload into the bootstrap process.
func readBootstrapPasswordFD(fd int) (string, error) {
	if fd < minAgentPasswordFD || fd > 255 {
		return "", errors.New("invalid password pipe descriptor")
	}
	f := os.NewFile(uintptr(fd), "rdev-bootstrap-password")
	if f == nil {
		return "", errors.New("password pipe descriptor is unavailable")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return "", errors.New("could not read password from agent pipe")
	}
	if len(b) > 4096 {
		return "", errors.New("agent password payload is too large")
	}
	p := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	if p == "" {
		return "", errors.New("password cannot be empty")
	}
	return p, nil
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
	fmt.Fprintf(os.Stdout, "Target: %s@%s\nChange: remove only this rdev key from $HOME/.ssh/authorized_keys\nFingerprint: %s\nWarning: if this is the last usable key, access may be lost; keep another login path available.\nType 'yes' to continue: ", user, sshAddr, bootstrap.Fingerprint(pubRaw))
	confirmed := false
	for _, arg := range os.Args[1:] {
		if arg == "-confirm" {
			confirmed = true
			break
		}
	}
	if !confirmed {
		confirmation, readErr := bufio.NewReader(os.Stdin).ReadString('\n')
		if readErr != nil || strings.TrimSpace(confirmation) != "yes" {
			return errors.New("revocation cancelled; type exactly yes to authorize")
		}
	}
	if err := bootstrap.Revoke(context.Background(), bootstrap.Config{Address: sshAddr, User: user, PublicKey: ssh.MarshalAuthorizedKey(pubKey), PrivateKey: pem.EncodeToMemory(priv), HostKeyCallback: cb}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "dedicated key revoked for %s\nLocal key retained at %s; rerun bootstrap-key for recovery if another login path is available.\n", name, id.Private)
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
