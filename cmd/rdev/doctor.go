package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/CIPFZ/rdev/internal/agentrepair"
	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

type doctorItem struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
	Action string `json:"action,omitempty"`
}
type doctorReport struct {
	Version       string      `json:"version"`
	Mode          string      `json:"mode"`
	Policy        doctorItem  `json:"policy"`
	Host          string      `json:"host,omitempty"`
	Connectivity  doctorItem  `json:"connectivity,omitempty"`
	Capability    *doctorItem `json:"capability,omitempty"`
	ConfigSource  string      `json:"config_source,omitempty"`
	Scope         string      `json:"scope,omitempty"`
	HostKeySource string      `json:"host_key_source,omitempty"`
	RemoteDir     string      `json:"remote_dir,omitempty"`
	Cwd           string      `json:"cwd,omitempty"`
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
	if len(args) == 2 && args[0] == "reinstall" {
		out, err := c.AgentReinstall(ctx, args[1])
		if err != nil {
			return err
		}
		return printJSON(c, out)
	}
	if len(args) < 2 || args[1] == "" || (args[0] != "status" && args[0] != "plan" && args[0] != "repair") {
		return errors.New("usage: rdev agent status|plan <host>; agent repair <host> -dry-run")
	}
	if args[0] == "repair" {
		fs, err := parseFlags(args[2:], "agent.repair")
		if err != nil {
			return err
		}
		if !fs.bools["dry-run"] && !(fs.bools["confirm"] && fs.str("transaction") != "" && fs.str("plan-digest") != "" && fs.str("candidate") != "" && fs.str("current") != "") {
			return errors.New("agent repair requires -dry-run or explicit -confirm with -transaction, -plan-digest, -candidate and -current")
		}
		if fs.bools["confirm"] {
			candidate, err := os.ReadFile(fs.str("candidate"))
			if err != nil {
				return fmt.Errorf("read candidate: %w", err)
			}
			current, err := os.ReadFile(fs.str("current"))
			if err != nil {
				return fmt.Errorf("read current snapshot: %w", err)
			}
			cap, err := c.CapabilityProbe(ctx, args[1], true)
			if err != nil {
				return err
			}
			hostSnap, err := c.Hosts.Inspect(args[1])
			if err != nil {
				return err
			}
			dir, err := transport.ValidateRemoteDir(hostSnap.Host.RemoteDir)
			if err != nil {
				return err
			}
			plan := agentrepair.Plan{Host: args[1], CurrentDigest: agentrepair.DigestBytes(current), CandidateDigest: agentrepair.DigestBytes(candidate)}
			if fs.str("plan-digest") != agentrepair.Digest(plan) {
				return errors.New("repair plan digest mismatch")
			}
			tx := &agentrepair.Transaction{ID: fs.str("transaction"), PlanDigest: fs.str("plan-digest"), Phase: agentrepair.Planned}
			decision, err := artifact.AuthorizeAgent(ctx, candidate, cap.OS, cap.Arch, artifact.TargetKey(hostSnap.Host.Addr, hostSnap.Host.Port, dir), true)
			if err != nil {
				return err
			}
			if fs.str("key") == "" || fs.str("known-hosts") == "" {
				return errors.New("repair requires -key and -known-hosts for fresh authentication")
			}
			if err := c.RepairAgentFreshAuth(ctx, args[1], plan, tx, candidate, current, true, decision, fs.str("key"), fs.str("known-hosts")); err != nil {
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(map[string]any{"host": args[1], "transaction": tx.ID, "plan_digest": agentrepair.Digest(plan), "phase": tx.Phase, "committed": true})
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
	policy := "invalid"
	if path, e := artifact.DefaultPolicyPath(); e == nil {
		if p, e := artifact.LoadPolicy(path, time.Now()); e == nil && p.Validate(time.Now()) == nil {
			policy = "valid"
		}
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
	item := doctorItem{Status: "FAIL", Detail: "release policy unavailable", Action: "configure an administrator release policy"}
	if path, e := artifact.DefaultPolicyPath(); e == nil {
		if p, e := artifact.LoadPolicy(path, time.Now()); e == nil {
			if e = p.Validate(time.Now()); e == nil {
				item = doctorItem{Status: "PASS", Detail: "policy is valid"}
			} else {
				item.Detail = e.Error()
			}
		} else {
			item.Detail = e.Error()
		}
	}
	r := doctorReport{Version: "local", Mode: "standalone", Policy: item}
	if len(fs.pos) == 1 {
		r.Host = fs.pos[0]
		if snap, e := c.Hosts.Inspect(r.Host); e == nil {
			r.Scope = string(snap.Scope)
			r.RemoteDir = snap.Host.RemoteDir
			r.Cwd = snap.State.Cwd
			if snap.Scope == session.ScopeProject {
				if path, e := session.ProjectConfigPath(); e == nil {
					r.ConfigSource = path
				}
			} else if path, e := session.ConfigPath(); e == nil {
				r.ConfigSource = path
			}
		}
		if home, e := os.UserHomeDir(); e == nil {
			r.HostKeySource = filepath.Join(home, ".ssh", "known_hosts")
		}
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
