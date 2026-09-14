package mcpsrv

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/CIPFZ/rdev/internal/agentrepair"
	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type agentRepairIn struct {
	Host           string `json:"host"`
	CandidateB64   string `json:"candidate_b64"`
	CurrentB64     string `json:"current_b64"`
	PlanDigest     string `json:"plan_digest"`
	Transaction    string `json:"transaction"`
	Confirm        bool   `json:"confirm"`
	KeyPath        string `json:"key_path"`
	KnownHostsPath string `json:"known_hosts_path"`
}

func registerAgentRepair(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_agent_repair", Description: "Execute an explicitly confirmed agent repair with exact plan/candidate digests and dedicated-key fresh authentication. Candidate/current are base64 bytes; no password or TTY input is accepted."}, func(ctx context.Context, _ *mcp.CallToolRequest, in agentRepairIn) (*mcp.CallToolResult, map[string]any, error) {
		if !in.Confirm || in.Host == "" || in.Transaction == "" || in.PlanDigest == "" || in.KeyPath == "" || in.KnownHostsPath == "" {
			return nil, nil, errors.New("agent repair requires confirm, transaction, plan digest, host, key and known_hosts")
		}
		candidate, err := base64.StdEncoding.DecodeString(in.CandidateB64)
		if err != nil {
			return nil, nil, fmt.Errorf("candidate_b64: %w", err)
		}
		current, err := base64.StdEncoding.DecodeString(in.CurrentB64)
		if err != nil {
			return nil, nil, fmt.Errorf("current_b64: %w", err)
		}
		hash := func(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
		plan := agentrepair.Plan{Host: in.Host, CurrentDigest: hash(current), CandidateDigest: hash(candidate)}
		if agentrepair.Digest(plan) != in.PlanDigest {
			return nil, nil, errors.New("repair plan digest mismatch")
		}
		snap, err := c.Hosts.Inspect(in.Host)
		if err != nil {
			return nil, nil, err
		}
		cap, err := c.CapabilityProbe(ctx, in.Host, true)
		if err != nil {
			return nil, nil, err
		}
		dir, err := transport.ValidateRemoteDir(snap.Host.RemoteDir)
		if err != nil {
			return nil, nil, err
		}
		decision, err := artifact.AuthorizeAgent(ctx, candidate, cap.OS, cap.Arch, artifact.TargetKey(snap.Host.Addr, snap.Host.Port, dir), true)
		if err != nil {
			return nil, nil, err
		}
		tx := &agentrepair.Transaction{ID: in.Transaction, PlanDigest: in.PlanDigest, Phase: agentrepair.Planned}
		if err := c.RepairAgentFreshAuth(ctx, in.Host, plan, tx, candidate, current, true, decision, in.KeyPath, in.KnownHostsPath); err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{"host": in.Host, "transaction": tx.ID, "plan_digest": in.PlanDigest, "phase": tx.Phase, "committed": true}, nil
	})
}
