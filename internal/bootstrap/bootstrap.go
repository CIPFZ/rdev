// Package bootstrap performs one-shot, password-authenticated installation of
// an rdev-owned SSH public key. Passwords are accepted only in memory.
package bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

type Config struct {
	Address         string
	User            string
	Password        string
	PublicKey       []byte
	PrivateKey      []byte
	HostKeyCallback ssh.HostKeyCallback
	Timeout         time.Duration
}

// Run installs PublicKey exactly once and proves that the matching private key
// can authenticate afterwards. HostKeyCallback is mandatory; callers must not
// disable host-key verification in production.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Address == "" || cfg.User == "" || cfg.Password == "" || len(cfg.PublicKey) == 0 || len(cfg.PrivateKey) == 0 {
		return errors.New("bootstrap requires address, user, password and key pair")
	}
	if cfg.HostKeyCallback == nil {
		return errors.New("bootstrap requires host-key verification")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	pub := strings.TrimSpace(string(cfg.PublicKey))
	if strings.ContainsAny(pub, "\r\n") {
		return errors.New("public key must be one line")
	}
	priv, err := ssh.ParsePrivateKey(cfg.PrivateKey)
	if err != nil {
		return fmt.Errorf("parse dedicated private key: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	password := cfg.Password
	defer func() { password = ""; cfg.Password = "" }()
	client, err := dial(ctx, cfg.Address, cfg.User, password, cfg.HostKeyCallback)
	if err != nil {
		return fmt.Errorf("password authentication: %w", err)
	}
	if err := appendKey(client, pub); err != nil {
		client.Close()
		return err
	}
	client.Close()
	// Reconnect with the dedicated signer only; no password and no agent.
	client, err = dialSigner(ctx, cfg.Address, cfg.User, priv, cfg.HostKeyCallback)
	if err != nil {
		return fmt.Errorf("dedicated-key verification: %w", err)
	}
	defer client.Close()
	s, err := client.NewSession()
	if err != nil {
		return err
	}
	defer s.Close()
	if err := s.Run("true"); err != nil {
		return fmt.Errorf("dedicated-key command: %w", err)
	}
	return nil
}

func dial(ctx context.Context, address, user, password string, cb ssh.HostKeyCallback) (*ssh.Client, error) {
	return dialConfig(ctx, address, &ssh.ClientConfig{User: user, Auth: []ssh.AuthMethod{ssh.Password(password)}, HostKeyCallback: cb, Timeout: 10 * time.Second})
}
func dialSigner(ctx context.Context, address, user string, signer ssh.Signer, cb ssh.HostKeyCallback) (*ssh.Client, error) {
	return dialConfig(ctx, address, &ssh.ClientConfig{User: user, Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)}, HostKeyCallback: cb, Timeout: 10 * time.Second})
}
func dialConfig(ctx context.Context, address string, cfg *ssh.ClientConfig) (*ssh.Client, error) {
	d := net.Dialer{}
	c, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	cc, chans, reqs, err := ssh.NewClientConn(c, address, cfg)
	if err != nil {
		c.Close()
		return nil, err
	}
	return ssh.NewClient(cc, chans, reqs), nil
}
func appendKey(client *ssh.Client, pub string) error {
	s, err := client.NewSession()
	if err != nil {
		return err
	}
	defer s.Close()
	payload := base64.StdEncoding.EncodeToString([]byte(pub + "\n"))
	cmd := "umask 077; mkdir -p \"$HOME/.ssh\"; touch \"$HOME/.ssh/authorized_keys\"; chmod 600 \"$HOME/.ssh/authorized_keys\"; k=$(printf '%s' '" + payload + "' | base64 -d); grep -Fqx -- \"$k\" \"$HOME/.ssh/authorized_keys\" || printf '%s\\n' \"$k\" >> \"$HOME/.ssh/authorized_keys\""
	if err := s.Run(cmd); err != nil {
		return fmt.Errorf("install dedicated key: %w", err)
	}
	return nil
}

func Fingerprint(public []byte) string {
	h := sha256.Sum256(public)
	return fmt.Sprintf("SHA256:%x", h[:])
}
