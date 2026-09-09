package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
)

// Shared mode is an exclusive execution path. Unsupported commands must never
// instantiate a private Client and bypass daemon policy, ownership or audit.
func runBrokerCommand(ctx context.Context, args []string) error {
	if err := validateCLI(args); err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("broker command required")
	}
	switch args[0] {
	case "fleet":
		return brokerFleet(ctx, args[1:])
	case "sync":
		return brokerSync(ctx, args[1:])
	case "secret":
		return brokerSecret(ctx, args[1:])
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
	case "state":
		return cmdBrokerState(ctx, args[1:])
	case "env":
		return brokerCapability(ctx, args[2:])
	case "capability":
		return brokerCapability(ctx, args[1:])
	case "job":
		return brokerJob(ctx, args[1:])
	case "mutation":
		return brokerMutation(ctx, args[1:])
	case "serve":
		return brokerServe(ctx)
	case "broker":
		fs, err := parseFlags(args[2:], "broker.status")
		if err != nil {
			return err
		}
		if fs.bools["pool"] {
			return brokerPoolStatus(ctx)
		}
		return brokerStatus(ctx)
	case "compat":
		return cmdCompat()
	case "support":
		return cmdBrokerSupport(ctx, args[1:])
	case "version", "-version", "--version":
		printVersion()
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		return fmt.Errorf("%w: command is not available in shared broker mode; see rdev support for alternatives", proto.NewError(proto.CodeUnsupportedFeature, "", proto.StateNotSent))
	}
}

func brokerSync(ctx context.Context, args []string) error {
	opts, err := parseSyncOptions(args)
	if err != nil {
		return err
	}
	opts, err = client.NormalizeSyncOptions(opts)
	if err != nil {
		return err
	}
	local, err := filepath.Abs(opts.Local)
	if err != nil {
		return err
	}
	if os.IsPathSeparator(opts.Local[len(opts.Local)-1]) && !os.IsPathSeparator(local[len(local)-1]) {
		local += string(os.PathSeparator)
	}
	opts.Local = local
	host := opts.Host
	opts.Host = ""
	if opts.Direction == "" {
		opts.Direction = "push"
	}
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		return err
	}
	defer c.Close()
	r, err := c.DoContext(ctx, broker.Request{Operation: "sync." + opts.Direction, Host: host, Sync: &opts})
	if err != nil {
		return err
	}
	if err := r.Failure(); err != nil {
		return err
	}
	if r.Sync == nil {
		return errors.New("broker sync returned no result")
	}
	return printSyncResult(r.Sync)
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
	fs, err := parseFlags(args, "ls")
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

// Secret set reads bounded stdin, keeping values out of argv and stdout.
func brokerSecret(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: rdev secret list HOST | set HOST NAME < value | delete HOST NAME")
	}
	fs, err := parseFlags(args[1:], "secret."+args[0])
	if err != nil {
		return err
	}
	args = append([]string{args[0]}, fs.pos...)
	req := broker.Request{Operation: "secret." + args[0], Host: args[1], Secret: &broker.SecretParams{}}
	switch args[0] {
	case "list":
		if len(args) != 2 {
			return errors.New("usage: rdev secret list HOST")
		}
	case "set_from_file":
		if len(args) != 4 {
			return errors.New("usage: rdev secret set_from_file HOST NAME REMOTE_PATH")
		}
		req.Secret.Name, req.Secret.Path = args[2], args[3]
	case "set", "delete":
		if len(args) != 3 {
			return errors.New("usage: rdev secret set|delete HOST NAME")
		}
		req.Secret.Name = args[2]
		if args[0] == "set" {
			b, err := io.ReadAll(io.LimitReader(os.Stdin, 65537))
			if err != nil || len(b) > 65536 {
				return errors.New("secret input unreadable or too large")
			}
			req.Secret.Value = string(b)
		}
	default:
		return errors.New("unknown secret action")
	}
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.DoContext(ctx, req)
	if err != nil {
		return err
	}
	if err := resp.Failure(); err != nil {
		return err
	}
	if args[0] == "list" {
		entries := resp.Secrets
		if entries == nil {
			entries = []broker.SecretDescriptor{}
		}
		return json.NewEncoder(os.Stdout).Encode(entries)
	}
	return json.NewEncoder(os.Stdout).Encode(resp.Mutation)
}
