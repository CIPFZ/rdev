package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/client"
)

type doctorItem struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
	Action string `json:"action,omitempty"`
}
type doctorReport struct {
	Version      string      `json:"version"`
	Mode         string      `json:"mode"`
	Policy       doctorItem  `json:"policy"`
	Host         string      `json:"host,omitempty"`
	Connectivity doctorItem  `json:"connectivity,omitempty"`
	Capability   *doctorItem `json:"capability,omitempty"`
}

type agentPlan struct {
	Host             string `json:"host"`
	Mode             string `json:"mode"`
	CurrentVersion   string `json:"current_version"`
	CandidateVersion string `json:"candidate_version"`
	Policy           string `json:"policy"`
	Upload           string `json:"upload"`
	Transaction      string `json:"transaction"`
	Action           string `json:"action"`
}

func cmdAgent(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 2 || args[1] == "" || (args[0] != "status" && args[0] != "plan" && args[0] != "repair") {
		return errors.New("usage: rdev agent status|plan <host>; agent repair <host> -dry-run")
	}
	if args[0] == "repair" {
		if len(args) != 3 || args[2] != "-dry-run" {
			return errors.New("agent repair requires -dry-run; applying repair is not available without a transaction and approval contract")
		}
		probe, err := c.CapabilityProbe(ctx, args[1], false)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(agentPlan{Host: args[1], Mode: "repair-preview", CurrentVersion: probe.ProbeVersion, CandidateVersion: "unknown", Policy: "unknown", Upload: "unknown", Transaction: "preview-only", Action: "no mutation; obtain an explicit transaction and approval before repair"})
	}
	if len(args) != 2 {
		return errors.New("usage: rdev agent status|plan <host>")
	}
	probe, err := c.CapabilityProbe(ctx, args[1], false)
	if err != nil {
		return err
	}
	p := artifact.DiagnosePolicy(time.Now())
	policy := "invalid"
	if p.Valid {
		policy = "valid"
	}
	r := agentPlan{Host: args[1], Mode: args[0], CurrentVersion: probe.ProbeVersion, CandidateVersion: "unknown", Policy: policy, Upload: "unknown", Transaction: "unknown", Action: "read-only; no installation requested"}
	return json.NewEncoder(os.Stdout).Encode(r)
}

func cmdDoctor(ctx context.Context, c *client.Client, args []string) error {
	fs, err := parseFlags(args, "doctor")
	if err != nil {
		return err
	}
	if len(fs.pos) > 1 {
		return errors.New("usage: rdev doctor [host]")
	}
	p := artifact.DiagnosePolicy(time.Now())
	item := doctorItem{Status: "FAIL", Detail: p.Error, Action: p.Action}
	if p.Valid {
		item = doctorItem{Status: "PASS", Detail: "policy is valid"}
	}
	r := doctorReport{Version: "local", Mode: "standalone", Policy: item}
	if len(fs.pos) == 1 {
		r.Host = fs.pos[0]
		probe, e := c.CapabilityProbe(ctx, r.Host, false)
		if e != nil {
			r.Connectivity = doctorItem{Status: "FAIL", Detail: e.Error(), Action: "verify host trust and public-key authentication"}
		} else {
			r.Connectivity = doctorItem{Status: "PASS", Detail: "agent responded"}
			x := doctorItem{Status: "PASS", Detail: probe.ProbeVersion}
			r.Capability = &x
		}
	} else {
		r.Connectivity = doctorItem{Status: "SKIP", Detail: "no host supplied"}
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
