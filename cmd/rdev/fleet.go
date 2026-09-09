package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/CIPFZ/rdev/internal/broker"
)

// fleetExit carries the daemon's aggregate result without inventing a second
// state machine in the CLI. Control commands report successful admission only.
type fleetExit int

func (e fleetExit) Error() string { return fmt.Sprintf("fleet exit status %d", e) }

func readFleetJSON(r io.Reader, max int64, out any) error {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return fmt.Errorf("read Fleet input: %w", err)
	}
	if int64(len(data)) > max {
		return errors.New("Fleet input exceeds size limit")
	}
	if err := broker.DecodeFleetJSON(data, out); err != nil {
		return errors.New("invalid Fleet JSON document: unknown, duplicate or trailing fields")
	}
	return nil
}

func fleetFile(path string, max int64, out any) error {
	if path == "" {
		return errors.New("-file is required")
	}
	if path == "-" {
		return readFleetJSON(os.Stdin, max, out)
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return readFleetJSON(f, max, out)
}

func brokerFleet(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("Fleet action required; see rdev help")
	}
	action := args[0]
	f, err := parseFlags(args[1:], "fleet."+action)
	if err != nil {
		return err
	}
	q := &broker.FleetRequest{}
	req := broker.Request{Operation: "fleet." + strings.ReplaceAll(action, "-", "."), Fleet: q}
	if len(f.pos) > 0 {
		q.PlanID = f.pos[0]
	}
	switch action {
	case "plan":
		if err := fleetFile(f.str("file"), broker.FleetMaxSpecBytes, &q.Spec); err != nil {
			return err
		}
		if q.Spec == nil {
			return errors.New("Fleet spec must be an object")
		}
	case "inventory-update":
		if err := fleetFile(f.str("file"), broker.FleetMaxInventoryBytes, &q.Inventory); err != nil {
			return err
		}
		if q.Inventory == nil {
			return errors.New("Fleet inventory must be an object")
		}
		q.Revision = q.Inventory.Revision
	case "inventory-import":
		if f.str("revision") == "" || f.str("revision") == "0" {
			return errors.New("-revision is required and must be positive; inspect inventory-list first")
		}
		if value := f.str("revision"); value != "" {
			q.Revision, _ = strconv.ParseUint(value, 10, 64)
		}
	case "approve", "execute":
		q.Digest = f.str("digest")
		if q.Digest == "" {
			return errors.New("-digest is required")
		}
		if action == "approve" {
			q.TTLSeconds = f.num("ttl")
		} else {
			req.Approval = f.str("approval")
			if req.Approval == "" {
				return errors.New("-approval is required")
			}
		}
	case "status", "results", "list":
		q.Offset, q.Limit = f.num("offset"), f.num("limit")
		if q.Limit == 0 {
			if f.str("limit") != "" {
				return errors.New("-limit must be 1..32")
			}
			q.Limit = 32
		}
	case "retry":
		q.HostIDs = append([]string(nil), f.pos[1:]...)
	}
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		return err
	}
	defer c.Close()
	r, err := c.DoContext(ctx, req)
	if err != nil {
		return err
	}
	if !r.OK {
		return errors.New(r.Error)
	}
	var output any
	switch {
	case r.Approval != nil:
		output = r.Approval
	case r.Fleet != nil:
		output = r.Fleet
	case r.Fleets != nil:
		output = r.Fleets
	case r.Inventory != nil:
		output = r.Inventory
	default:
		return errors.New("broker returned no Fleet result")
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		return err
	}
	if (action == "status" || action == "results") && r.Fleet != nil && r.Fleet.ExitStatus != 0 {
		if r.Fleet.ExitStatus < 0 || r.Fleet.ExitStatus > 2 {
			return errors.New("invalid Fleet exit status")
		}
		return fleetExit(r.Fleet.ExitStatus)
	}
	return nil
}
