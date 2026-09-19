package mcpsrv

import (
	"context"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type operationStatusIn struct {
	Host        string `json:"host"`
	OperationID string `json:"operation_id" jsonschema:"Operation identity returned by the original request; query only after an ambiguous result"`
}

func registerOperationStatus(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_operation_status", Description: "Read a prior operation outcome retained by the remote agent without replaying it. Use the operation_id from an ambiguous result; standalone CLI callers may query it from a later process while the agent retains the record."}, func(ctx context.Context, _ *mcp.CallToolRequest, in operationStatusIn) (*mcp.CallToolResult, proto.OperationStatusResult, error) {
		result, err := c.OperationStatus(ctx, in.Host, in.OperationID)
		if err != nil {
			return nil, proto.OperationStatusResult{}, err
		}
		return nil, *result, nil
	})
}

func registerBrokerOperationStatus(s *mcp.Server, socket string, owner broker.Owner) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_operation_status", Description: "Read a prior operation outcome from the shared remote agent without replaying it."}, func(ctx context.Context, _ *mcp.CallToolRequest, in operationStatusIn) (*mcp.CallToolResult, proto.OperationStatusResult, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Host: in.Host, Operation: proto.OpOperationStatus, Wire: &proto.Request{Op: proto.OpOperationStatus, ClientID: owner.ClientID, ProjectID: owner.ProjectID, OperationStatus: &proto.OperationStatusParams{OperationID: in.OperationID}}})
		if err != nil {
			return nil, proto.OperationStatusResult{}, err
		}
		if resp.Wire == nil || resp.Wire.OperationStatus == nil {
			return nil, proto.OperationStatusResult{}, proto.NewError(proto.CodeInvalidFrame, in.OperationID, proto.StatePossiblyExecuted)
		}
		return nil, *resp.Wire.OperationStatus, nil
	})
}
