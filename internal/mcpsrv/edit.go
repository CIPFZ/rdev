package mcpsrv

import (
	"context"
	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/fileedit"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const editDescription = "Edit one existing remote UTF-8 file (max 4 MiB). First rdev_read with include_digest=true; copy digest to base_digest. kind=lines: inclusive 1-based original line ranges, end_line=0 inserts before start_line (line_count+1 appends), optional expected exact guard. kind=patch: single-file unified diff or bare @@ hunks, strict counts/context, no fuzzy matching. kind=replace: complete content including empty string. Send only the selected payload. All ranges use the original snapshot; overlaps reject the whole request. Newlines are literal, never added/normalized. Success returns new_digest for the next edit. Conflict/mismatch: reread and regenerate. Ambiguous transport outcome: inspect state; never blindly repeat. Linux/macOS; new files use rdev_write."

type EditIn struct {
	Host          string           `json:"host"`
	Path          string           `json:"path"`
	Kind          string           `json:"kind" jsonschema:"One of lines, patch, replace"`
	BaseDigest    string           `json:"base_digest" jsonschema:"Exact digest copied from rdev_read(include_digest=true) or preceding successful edit"`
	Content       *string          `json:"content,omitempty"`
	Patch         string           `json:"patch,omitempty"`
	Lines         []proto.LineEdit `json:"lines,omitempty"`
	OperationID   string           `json:"operation_id,omitempty" jsonschema:"Stable mutation identity, unchanged for recovery; never reuse for different content"`
	ApprovalToken string           `json:"approval_token,omitempty" jsonschema:"Exact request approval when required by the shared broker"`
}

func (in EditIn) params() proto.EditParams {
	return proto.EditParams{Path: in.Path, Kind: in.Kind, BaseDigest: in.BaseDigest, Content: in.Content, Patch: in.Patch, Lines: in.Lines}
}
func registerEdit(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_edit", Description: editDescription}, func(ctx context.Context, _ *mcp.CallToolRequest, in EditIn) (*mcp.CallToolResult, proto.EditResult, error) {
		r, err := c.EditFile(ctx, in.Host, in.params(), in.OperationID)
		if err != nil {
			return nil, proto.EditResult{}, err
		}
		return nil, *r, nil
	})
}
func registerBrokerEdit(s *mcp.Server, socket string, owner broker.Owner) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_edit", Description: editDescription}, func(ctx context.Context, _ *mcp.CallToolRequest, in EditIn) (*mcp.CallToolResult, proto.EditResult, error) {
		p := in.params()
		if err := fileedit.Validate(&p); err != nil {
			return nil, proto.EditResult{}, err
		}
		r, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Host: in.Host, Operation: proto.OpEditFile, Approval: in.ApprovalToken, Wire: &proto.Request{Op: proto.OpEditFile, OperationID: in.OperationID, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Edit: &p}})
		if err != nil {
			return nil, proto.EditResult{}, err
		}
		if r.Wire == nil || r.Wire.Edit == nil {
			return nil, proto.EditResult{}, proto.NewError(proto.CodeInvalidFrame, in.OperationID, proto.StatePossiblyExecuted)
		}
		return nil, *r.Wire.Edit, nil
	})
}
