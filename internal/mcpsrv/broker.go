package mcpsrv

import (
	"context"
	"errors"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewBroker exposes the MCP operations that are backed by the local broker.
// It deliberately has no direct client or secret store: rdevd remains the
// owner of transport, policy, quota, and audit state.
func NewBroker(socket string, owner broker.Owner) (*mcp.Server, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "rdev", Title: "Remote dev environment proxy", Version: Version}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_ping", Description: "Verify a remote host through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in struct {
		Host string `json:"host"`
	}) (*mcp.CallToolResult, proto.PingResult, error) { c, err := broker.DialClient(ctx, socket, owner); if err != nil {
		return nil, proto.PingResult{}, err
	}; defer c.Close(); resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "ping", Host: in.Host, Wire: &proto.Request{Op: proto.OpPing}}); if err != nil {
		return nil, proto.PingResult{}, err
	}; if !resp.OK {
		return nil, proto.PingResult{}, errors.New(resp.Error)
	}; if resp.Wire == nil || resp.Wire.Ping == nil {
		return nil, proto.PingResult{}, errors.New("broker ping returned no result")
	}; return nil, *resp.Wire.Ping, nil })
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_exec", Description: "Run a command through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ExecIn) (*mcp.CallToolResult, ExecOut, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, ExecOut{}, err
		}
		defer c.Close()
		login := true
		if in.LoginShell != nil {
			login = *in.LoginShell
		}
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "exec", Host: in.Host, Wire: &proto.Request{Op: proto.OpExec, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Exec: &proto.ExecParams{Argv: in.Argv, Cwd: in.Cwd, Env: in.Env, LoginShell: login, Stdin: in.Stdin, TimeoutSec: in.TimeoutSec, MaxOutputBytes: in.MaxOutputBytes}}})
		if err != nil {
			return nil, ExecOut{}, err
		}
		if !resp.OK {
			return nil, ExecOut{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Exec == nil {
			return nil, ExecOut{}, errors.New("broker exec returned no result")
		}
		return nil, toExecOut(&client.ExecResult{ExecResult: resp.Wire.Exec}), nil
	})
	return s, nil
}
