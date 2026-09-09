package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/support"
)

func cmdSupport(ctx context.Context, c *client.Client, args []string) error {
	fs, err := parseFlags(args, "support")
	if err != nil {
		return err
	}
	if len(fs.pos) > 1 || len(fs.pos) == 0 && fs.bools["refresh"] {
		return errors.New("usage: rdev support [host] [-refresh]")
	}
	out := support.Discover("standalone")
	if len(fs.pos) == 1 {
		probe, err := c.CapabilityProbe(ctx, fs.pos[0], fs.bools["refresh"])
		if err != nil {
			return err
		}
		out.SetRuntime(probe)
	}
	return printJSON(c, out)
}

func cmdBrokerSupport(ctx context.Context, args []string) error {
	fs, err := parseFlags(args, "support")
	if err != nil {
		return err
	}
	if len(fs.pos) > 1 || len(fs.pos) == 0 && fs.bools["refresh"] {
		return errors.New("usage: rdev support [host] [-refresh]")
	}
	// The no-host static contract remains usable without broker credentials.
	out := support.Discover("broker")
	if len(fs.pos) == 1 {
		owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
		c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
		if err != nil {
			return err
		}
		defer c.Close()
		out, err = broker.DiscoverSupport(ctx, c, fs.pos[0], fs.bools["refresh"])
		if err != nil {
			return err
		}
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func cmdBrokerState(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: rdev state inspect|migrate|repair <host> [-dry-run]")
	}
	fs, err := parseFlags(args[1:], "state."+args[0])
	if err != nil {
		return err
	}
	if len(fs.pos) != 1 {
		return errors.New("unexpected state operand")
	}
	op := ""
	switch args[0] {
	case "inspect":
		op = proto.OpStateInspect
	case "migrate":
		op = proto.OpStateMigrate
	case "repair":
		op = proto.OpStateRepair
	default:
		return errors.New("state action must be inspect, migrate or repair")
	}
	r, err := brokerWire(ctx, op, fs.pos[0], &proto.Request{Op: op, State: &proto.StateParams{DryRun: fs.bools["dry-run"]}})
	if err != nil {
		return err
	}
	if r.State == nil {
		return errors.New("broker state returned no result")
	}
	return json.NewEncoder(os.Stdout).Encode(r.State)
}
