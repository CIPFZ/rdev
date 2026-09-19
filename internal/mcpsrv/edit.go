package mcpsrv

import (
	"context"
	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/fileedit"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const editDescription = "Edit one existing remote UTF-8 file (max 4 MiB). First rdev_read with include_digest=true; copy digest to base_digest. kind=lines: inclusive 1-based original line ranges, end_line=0 inserts before start_line (line_count+1 appends), optional expected exact guard. kind=patch: single-file unified diff or bare @@ hunks, strict counts/context, no fuzzy matching. kind=search: literal exact replacement of the first match, or every match with replace_all=true. kind=replace: complete content including empty string. Send only the selected payload. All ranges use the original snapshot; overlaps reject the whole request. Newlines are literal, never added/normalized. Success returns new_digest for the next edit. Conflict/mismatch: reread and regenerate. Ambiguous transport outcome: inspect state; never blindly repeat. Linux/macOS; new files use rdev_write."

type EditIn struct {
	Host          string           `json:"host"`
	Path          string           `json:"path"`
	Kind          string           `json:"kind" jsonschema:"One of lines, patch, replace, search"`
	BaseDigest    string           `json:"base_digest" jsonschema:"Exact digest copied from rdev_read(include_digest=true) or preceding successful edit"`
	Content       *string          `json:"content,omitempty"`
	Patch         string           `json:"patch,omitempty"`
	Lines         []proto.LineEdit `json:"lines,omitempty"`
	Search        string           `json:"search,omitempty" jsonschema:"Literal UTF-8 text to find when kind=search"`
	Replacement   string           `json:"replacement,omitempty" jsonschema:"Literal replacement text when kind=search"`
	ReplaceAll    bool             `json:"replace_all,omitempty" jsonschema:"Replace every match; otherwise replace the first exact match"`
	OperationID   string           `json:"operation_id,omitempty" jsonschema:"Stable mutation identity, unchanged for recovery; never reuse for different content"`
	ApprovalToken string           `json:"approval_token,omitempty" jsonschema:"Exact request approval when required by the shared broker"`
	Backup        bool             `json:"backup,omitempty" jsonschema:"Keep a digest-addressed backup for rollback"`
}

type EditPreviewResult struct {
	OldDigest   string `json:"old_digest"`
	NewDigest   string `json:"new_digest"`
	BytesBefore int    `json:"bytes_before"`
	BytesAfter  int    `json:"bytes_after"`
	Changed     bool   `json:"changed"`
	Diff        string `json:"diff"`
}

func (in EditIn) params() proto.EditParams {
	return proto.EditParams{Path: in.Path, Kind: in.Kind, BaseDigest: in.BaseDigest, Content: in.Content, Patch: in.Patch, Lines: in.Lines, Search: in.Search, Replacement: in.Replacement, ReplaceAll: in.ReplaceAll, Backup: in.Backup}
}

type EditRollbackIn struct {
	Host           string `json:"host"`
	Path           string `json:"path"`
	BackupID       string `json:"backup_id" jsonschema:"Backup ID returned by rdev_edit with backup=true"`
	ExpectedDigest string `json:"expected_digest,omitempty"`
	OperationID    string `json:"operation_id,omitempty"`
	ApprovalToken  string `json:"approval_token,omitempty"`
}

func registerEdit(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_edit", Description: editDescription}, func(ctx context.Context, _ *mcp.CallToolRequest, in EditIn) (*mcp.CallToolResult, proto.EditResult, error) {
		r, err := c.EditFile(ctx, in.Host, in.params(), in.OperationID)
		if err != nil {
			return nil, proto.EditResult{}, err
		}
		return nil, *r, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_edit_preview", Description: "Preview a digest-bound remote text edit without changing the file. Returns a bounded unified diff; apply the same payload with rdev_edit after reviewing it."}, func(ctx context.Context, _ *mcp.CallToolRequest, in EditIn) (*mcp.CallToolResult, EditPreviewResult, error) {
		read, err := c.ReadFileSnapshot(ctx, in.Host, in.Path, 0, fileedit.MaxBytes)
		if err != nil {
			return nil, EditPreviewResult{}, err
		}
		preview, err := previewEdit(read.Content, read.ContentB64, read.Digest, in.params())
		return nil, preview, err
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_edit_rollback", Description: "Restore the backup returned by rdev_edit with backup=true."}, func(ctx context.Context, _ *mcp.CallToolRequest, in EditRollbackIn) (*mcp.CallToolResult, proto.EditRollbackResult, error) {
		r, err := c.EditRollback(ctx, in.Host, proto.EditRollbackParams{Path: in.Path, BackupID: in.BackupID, ExpectedDigest: in.ExpectedDigest}, in.OperationID)
		if err != nil {
			return nil, proto.EditRollbackResult{}, err
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
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_edit_preview", Description: "Preview a digest-bound remote text edit through the shared broker without changing the file."}, func(ctx context.Context, _ *mcp.CallToolRequest, in EditIn) (*mcp.CallToolResult, EditPreviewResult, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Host: in.Host, Operation: proto.OpReadFile, Wire: &proto.Request{Op: proto.OpReadFile, Read: &proto.ReadParams{Path: in.Path, Limit: fileedit.MaxBytes, IncludeDigest: true}}})
		if err != nil {
			return nil, EditPreviewResult{}, err
		}
		if resp.Wire == nil || resp.Wire.Read == nil {
			return nil, EditPreviewResult{}, proto.NewError(proto.CodeInvalidFrame, "", proto.StatePossiblyExecuted)
		}
		read := resp.Wire.Read
		preview, err := previewEdit(read.Content, read.ContentB64, read.Digest, in.params())
		return nil, preview, err
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_edit_rollback", Description: "Restore a backup created by rdev_edit through the shared broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in EditRollbackIn) (*mcp.CallToolResult, proto.EditRollbackResult, error) {
		r, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Host: in.Host, Operation: proto.OpEditRollback, Approval: in.ApprovalToken, Wire: &proto.Request{Op: proto.OpEditRollback, OperationID: in.OperationID, ClientID: owner.ClientID, ProjectID: owner.ProjectID, EditRollback: &proto.EditRollbackParams{Path: in.Path, BackupID: in.BackupID, ExpectedDigest: in.ExpectedDigest}}})
		if err != nil {
			return nil, proto.EditRollbackResult{}, err
		}
		if r.Wire == nil || r.Wire.EditRollback == nil {
			return nil, proto.EditRollbackResult{}, proto.NewError(proto.CodeInvalidFrame, in.OperationID, proto.StatePossiblyExecuted)
		}
		return nil, *r.Wire.EditRollback, nil
	})
}

func previewEdit(content string, encoded bool, digest string, p proto.EditParams) (EditPreviewResult, error) {
	if encoded || digest == "" {
		return EditPreviewResult{}, proto.NewError(proto.CodeEditText, "", proto.StateFailed)
	}
	if p.BaseDigest == "" {
		p.BaseDigest = digest
	}
	before := []byte(content)
	after, err := fileedit.Apply(before, &p)
	if err != nil {
		return EditPreviewResult{}, err
	}
	return EditPreviewResult{OldDigest: fileedit.Digest(before), NewDigest: fileedit.Digest(after), BytesBefore: len(before), BytesAfter: len(after), Changed: string(before) != string(after), Diff: fileedit.UnifiedDiff(before, after)}, nil
}
