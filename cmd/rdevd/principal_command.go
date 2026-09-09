package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
)

// principalTokenCommand is an administrator-only provisioning path. Only its
// output token, never RDEV_PRINCIPAL_SECRET, should be given to the client.
func principalTokenCommand(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("principal-token", flag.ContinueOnError)
	client := flags.String("client-id", "", "client receiving the credential")
	project := flags.String("project-id", "", "project receiving the credential")
	ttl := flags.Duration("ttl", time.Hour, "credential lifetime (maximum 24h)")
	keyFile := flags.String("key-file", "", "0600 administrator signing key file")
	if err := parseSingleFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *ttl <= 0 || *ttl > broker.MaxPrincipalTokenTTL {
		return fmt.Errorf("principal-token requires a positive TTL of at most 24h and no positional arguments")
	}
	secret, err := principalSecret(*keyFile)
	if err != nil {
		return err
	}
	token, err := broker.MintPrincipalToken(secret, broker.Owner{ClientID: *client, ProjectID: *project}, time.Now().Add(*ttl))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, token)
	return err
}

func principalSecret(path string) (string, error) {
	secret := os.Getenv("RDEV_PRINCIPAL_SECRET")
	if path != "" {
		data, err := broker.ReadPrivateFile(path, 4096)
		if err != nil {
			return "", fmt.Errorf("principal key file rejected: %w", err)
		}
		secret = strings.TrimSpace(string(data))
	}
	return secret, broker.ValidatePrincipalSecret(secret)
}

func principalKeygenCommand(args []string) error {
	flags := flag.NewFlagSet("principal-keygen", flag.ContinueOnError)
	path := flags.String("out", "", "new 0600 key file (must not exist)")
	if err := parseSingleFlags(flags, args); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("principal-keygen requires -out and no positional arguments")
	}
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return err
	}
	f, err := os.OpenFile(*path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, hex.EncodeToString(key[:])); err != nil {
		return err
	}
	return f.Sync()
}
