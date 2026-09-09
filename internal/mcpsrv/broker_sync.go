package mcpsrv

import (
	"context"
	"errors"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func registerBrokerSync(s *mcp.Server, socket string, owner broker.Owner) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_sync", Description: "Prepare a shared push or pull with prepare=true, review its changes, then execute the same options with plan_id and approval_token. Use an absolute local path. Delete additionally requires sync.delete permission and confirm_delete at execution. dry_run alone gives a disposable preview."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SyncIn) (*mcp.CallToolResult, SyncOut, error) {
		direction := in.Direction
		if direction == "" {
			direction = "push"
		}
		opts := &client.SyncOptions{Direction: direction, Local: in.Local, Remote: in.Remote, Exclude: in.Exclude, DryRun: in.DryRun || in.Prepare, Prepare: in.Prepare, PlanID: in.PlanID, Delete: in.Delete, ConfirmDelete: in.ConfirmDelete, SymlinkPolicy: in.SymlinkPolicy, ConflictPolicy: in.ConflictPolicy, MaxOutputBytes: in.MaxOutputBytes}
		r, err := callBroker(ctx, socket, owner, broker.Request{Operation: "sync." + direction, Host: in.Host, Sync: opts, Approval: in.ApprovalToken, OperationID: in.OperationID})
		if err != nil {
			return nil, SyncOut{}, err
		}
		if r.Sync == nil {
			return nil, SyncOut{}, errors.New("broker sync returned no result")
		}
		return nil, SyncOut(*r.Sync), nil
	})
}
