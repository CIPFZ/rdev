package mcpsrv

import (
	"context"
	"errors"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type agentDiagnosticsIn struct {
	Host string `json:"host" jsonschema:"Remote host alias to inspect."`
}

type agentDiagnosticsOut struct {
	Host             string `json:"host"`
	Mode             string `json:"mode"`
	CurrentVersion   string `json:"current_version"`
	CandidateVersion string `json:"candidate_version"`
	Policy           string `json:"policy"`
	Upload           string `json:"upload"`
	Transaction      string `json:"transaction"`
	Action           string `json:"action"`
}

func registerAgentDiagnostics(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_agent_plan", Description: "Read-only agent status and installation plan. Probes the target and reports unknown fields without uploading, installing, repairing, or changing trust."}, func(ctx context.Context, _ *mcp.CallToolRequest, in agentDiagnosticsIn) (*mcp.CallToolResult, agentDiagnosticsOut, error) {
		if in.Host == "" {
			return nil, agentDiagnosticsOut{}, errors.New("host is required")
		}
		probe, err := c.CapabilityProbe(ctx, in.Host, false)
		if err != nil {
			return nil, agentDiagnosticsOut{}, err
		}
		policy := artifact.DiagnosePolicy(time.Now())
		decision := "invalid"
		if policy.Valid {
			decision = "valid"
		}
		return nil, agentDiagnosticsOut{Host: in.Host, Mode: "status-plan", CurrentVersion: probe.ProbeVersion, CandidateVersion: "unknown", Policy: decision, Upload: "unknown", Transaction: "unknown", Action: "read-only; no installation requested"}, nil
	})
}
