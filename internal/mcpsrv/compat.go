package mcpsrv

import (
	"context"

	"github.com/CIPFZ/rdev/internal/compat"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerCompat(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_compat", Description: "Machine-readable error, configuration, state and client/broker/agent version contracts. Local build metadata only; no connection or authorization to a target is implied."}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, compat.Contract, error) {
		return nil, compat.Current(), nil
	})
}
