// releasecheck is an internal release gate helper, not an installed rdev command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/CIPFZ/rdev/internal/release"
)

func run(args []string) error {
	if len(args) == 3 && args[0] == "licenses" {
		return release.Licenses(args[1], args[2])
	}
	if len(args) == 3 && args[0] == "sign" {
		return release.Sign(context.Background(), args[1], args[2])
	}
	if len(args) == 3 && args[0] == "verify-signed" {
		return release.VerifySigned(context.Background(), args[1], args[2])
	}
	if len(args) == 7 && args[0] == "prepare" {
		issued, err := time.Parse(time.RFC3339, args[5])
		if err != nil {
			return err
		}
		expires, err := time.Parse(time.RFC3339, args[6])
		if err != nil {
			return err
		}
		return release.PrepareSigned(args[1], args[2], args[3], args[4], issued, expires)
	}

	if len(args) == 1 && args[0] == "snapshot" {
		s, err := release.Snapshot()
		if err != nil {
			return err
		}
		if s.Dirty && os.Getenv("RDEV_RELEASE_ALLOW_DIRTY") != "1" {
			return errors.New("release gate requires a clean tree; RDEV_RELEASE_ALLOW_DIRTY=1 marks development evidence explicitly")
		}
		return json.NewEncoder(os.Stdout).Encode(s)
	}
	if len(args) == 2 && (args[0] == "audit" || args[0] == "modules") {
		f, err := os.Open(args[1])
		if err != nil {
			return err
		}
		defer f.Close()
		if args[0] == "modules" {
			return release.AuditModules(f)
		}
		return release.Audit(f)
	}
	if len(args) == 2 && args[0] == "verify" {
		return release.Verify(args[1])
	}
	if len(args) == 3 && args[0] == "generate" {
		var s release.Source
		if err := release.ReadJSON(args[1]+"/source.json", &s); err != nil {
			return err
		}
		return release.Generate(args[1], s, args[2])
	}
	return errors.New("usage: releasecheck snapshot | audit REPORT | modules REPORT | licenses DIR GO | generate DIR GO_VERSION | verify DIR | prepare DIR VERSION CHANNEL SIGNER ISSUED EXPIRES | sign DIR KEY | verify-signed DIR POLICY")
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
