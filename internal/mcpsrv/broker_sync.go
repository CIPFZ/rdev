package mcpsrv

import (
	"context"
	"errors"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerBrokerSync(s *mcp.Server, socket string, owner broker.Owner) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_sync", Description: "Preview a push or pull with rsync through the shared broker. Set dry_run=true and use an absolute local path. Delete previews additionally require sync.delete permission. Shared execution is not yet available."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SyncIn) (*mcp.CallToolResult, SyncOut, error) {
		direction := in.Direction
		if direction == "" {
			direction = "push"
		}
		opts := &client.SyncOptions{Direction: direction, Local: in.Local, Remote: in.Remote, Exclude: in.Exclude, DryRun: in.DryRun, Delete: in.Delete, ConfirmDelete: in.ConfirmDelete, SymlinkPolicy: in.SymlinkPolicy, ConflictPolicy: in.ConflictPolicy, MaxOutputBytes: in.MaxOutputBytes}
		r, err := callBroker(ctx, socket, owner, broker.Request{Operation: "sync." + direction, Host: in.Host, Sync: opts})
		if err != nil {
			return nil, SyncOut{}, err
		}
		if r.Sync == nil {
			return nil, SyncOut{}, errors.New("broker sync returned no result")
		}
		return nil, SyncOut(*r.Sync), nil
	})
}
