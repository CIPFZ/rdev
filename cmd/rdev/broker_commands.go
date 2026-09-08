package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/support"
)

// Shared mode is an exclusive execution path. Unsupported commands must never
// instantiate a private Client and bypass daemon policy, ownership or audit.
func runBrokerCommand(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("broker command required")
	}
	switch args[0] {
	case "ping":
		return brokerPing(ctx, args[1:])
	case "exec":
		return brokerExec(ctx, args[1:])
	case "read":
		return brokerRead(ctx, args[1:])
	case "ls":
		return brokerList(ctx, args[1:])
	case "write":
		return brokerWrite(ctx, args[1:])
	case "capability":
		return brokerCapability(ctx, args[1:])
	case "job":
		return brokerJob(ctx, args[1:])
	case "mutation":
		return brokerMutation(ctx, args[1:])
	case "serve":
		if len(args) != 1 {
			return errors.New("usage: rdev serve")
		}
		return brokerServe(ctx)
	case "broker":
		if len(args) == 3 && args[1] == "status" && args[2] == "--pool" {
			return brokerPoolStatus(ctx)
		}
		if len(args) != 2 || args[1] != "status" {
			return errors.New("usage: rdev broker status [--pool]")
		}
		return brokerStatus(ctx)
	case "support":
		return json.NewEncoder(os.Stdout).Encode(support.Snapshot())
	case "version", "-version", "--version":
		printVersion()
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		return errors.New("command is not available in shared broker mode")
	}
}

func brokerPoolStatus(ctx context.Context) error {
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		return err
	}
	defer c.Close()
	r, err := c.DoContext(ctx, broker.Request{Operation: "pool.health"})
	if err != nil {
		return err
	}
	health, err := broker.ProjectPoolHealth(r)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(health)
}

func brokerStatus(ctx context.Context) error {
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		return err
	}
	defer c.Close()
	r, err := c.DoContext(ctx, broker.Request{Operation: "status"})
	if err != nil {
		return err
	}
	status, err := broker.ProjectStatus(r)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(status)
}

func brokerList(ctx context.Context, args []string) error {
	fs, err := parseFlags(args, nil, nil)
	if err != nil {
		return err
	}
	if len(fs.pos) < 1 || len(fs.pos) > 2 {
		return errors.New("usage: rdev ls <host> [<path>] [-limit N]")
	}
	path := "."
	if len(fs.pos) == 2 {
		path = fs.pos[1]
	}
	r, err := brokerWire(ctx, proto.OpList, fs.pos[0], &proto.Request{Op: proto.OpList, List: &proto.ListParams{Path: path, Limit: fs.num("limit")}})
	if err != nil {
		return err
	}
	if r.List == nil {
		return errors.New("broker list returned no result")
	}
	return json.NewEncoder(os.Stdout).Encode(r.List)
}
