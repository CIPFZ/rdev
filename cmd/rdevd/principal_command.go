package main

import (
	"flag"
	"fmt"
	"io"
	"os"
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
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *ttl <= 0 || *ttl > broker.MaxPrincipalTokenTTL {
		return fmt.Errorf("principal-token requires a positive TTL of at most 24h and no positional arguments")
	}
	token, err := broker.MintPrincipalToken(os.Getenv("RDEV_PRINCIPAL_SECRET"), broker.Owner{ClientID: *client, ProjectID: *project}, time.Now().Add(*ttl))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, token)
	return err
}
