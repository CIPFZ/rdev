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
	}) (*mcp.CallToolResult, proto.PingResult, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, proto.PingResult{}, err
		}
		defer c.Close()
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "ping", Host: in.Host, Wire: &proto.Request{Op: proto.OpPing}})
		if err != nil {
			return nil, proto.PingResult{}, err
		}
		if !resp.OK {
			return nil, proto.PingResult{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Ping == nil {
			return nil, proto.PingResult{}, errors.New("broker ping returned no result")
		}
		return nil, *resp.Wire.Ping, nil
	})
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
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_read", Description: "Read a remote file through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ReadIn) (*mcp.CallToolResult, ReadOut, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, ReadOut{}, err
		}
		defer c.Close()
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "read_file", Host: in.Host, Wire: &proto.Request{Op: proto.OpReadFile, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Read: &proto.ReadParams{Path: in.Path, Offset: in.Offset, Limit: in.Limit}}})
		if err != nil {
			return nil, ReadOut{}, err
		}
		if !resp.OK {
			return nil, ReadOut{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Read == nil {
			return nil, ReadOut{}, errors.New("broker read returned no result")
		}
		r := resp.Wire.Read
		return nil, ReadOut{Content: r.Content, Base64: r.ContentB64, Size: r.Size, EOF: r.EOF, Truncation: r.Truncation, OperationID: r.OperationID, Terminal: r.Terminal, ExecutionState: r.Execution}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_write", Description: "Write a remote file through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in WriteIn) (*mcp.CallToolResult, WriteOut, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, WriteOut{}, err
		}
		defer c.Close()
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "write_file", Host: in.Host, Wire: &proto.Request{Op: proto.OpWriteFile, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Cat: &proto.WriteParams{Path: in.Path, Content: in.Content, Mode: in.Mode, Append: in.Append}}})
		if err != nil {
			return nil, WriteOut{}, err
		}
		if !resp.OK {
			return nil, WriteOut{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Cat == nil {
			return nil, WriteOut{}, errors.New("broker write returned no result")
		}
		w := resp.Wire.Cat
		return nil, WriteOut{Path: w.Path, BytesWritten: w.BytesWritten, OperationID: w.OperationID, Terminal: w.Terminal, ExecutionState: w.Execution}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_capability", Description: "Probe remote capabilities through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in struct {
		Host    string `json:"host"`
		Refresh bool   `json:"refresh,omitempty"`
	}) (*mcp.CallToolResult, proto.CapabilityResult, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, proto.CapabilityResult{}, err
		}
		defer c.Close()
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "capability_probe", Host: in.Host, Wire: &proto.Request{Op: proto.OpCapabilityProbe, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Capability: &proto.CapabilityParams{Refresh: in.Refresh}}})
		if err != nil {
			return nil, proto.CapabilityResult{}, err
		}
		if !resp.OK {
			return nil, proto.CapabilityResult{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Capability == nil {
			return nil, proto.CapabilityResult{}, errors.New("broker capability returned no result")
		}
		return nil, *resp.Wire.Capability, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_start", Description: "Start a supervised background job through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobStartIn) (*mcp.CallToolResult, JobOut, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, JobOut{}, err
		}
		defer c.Close()
		login := true
		if in.LoginShell != nil {
			login = *in.LoginShell
		}
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "job_start", Host: in.Host, Wire: &proto.Request{Op: proto.OpJobStart, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{Label: in.Label, Spec: &proto.ExecParams{Argv: in.Argv, Cwd: in.Cwd, Env: in.Env, LoginShell: login}}}})
		if err != nil {
			return nil, JobOut{}, err
		}
		if !resp.OK {
			return nil, JobOut{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Job == nil || resp.Wire.Job.Info == nil {
			return nil, JobOut{}, errors.New("broker job start returned no result")
		}
		return nil, toJobOut(resp.Wire.Job.Info), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_list", Description: "List supervised background jobs through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobListIn) (*mcp.CallToolResult, JobListOut, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, JobListOut{}, err
		}
		defer c.Close()
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "job_list", Host: in.Host, Wire: &proto.Request{Op: proto.OpJobList, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{Limit: in.Limit}}})
		if err != nil {
			return nil, JobListOut{}, err
		}
		if !resp.OK {
			return nil, JobListOut{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Job == nil {
			return nil, JobListOut{}, errors.New("broker job list returned no result")
		}
		out := JobListOut{Jobs: make([]JobOut, 0, len(resp.Wire.Job.List))}
		for _, j := range resp.Wire.Job.List {
			out.Jobs = append(out.Jobs, toJobOut(j))
		}
		out.Total = resp.Wire.Job.Total
		out.Truncated = resp.Wire.Job.Truncated
		return nil, out, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_status", Description: "Read a supervised background job status through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobRefIn) (*mcp.CallToolResult, JobOut, error) {
		c, err := broker.DialClient(ctx, socket, owner)
		if err != nil {
			return nil, JobOut{}, err
		}
		defer c.Close()
		resp, err := c.DoContext(ctx, broker.Request{Owner: owner, Operation: "job_status", Host: in.Host, Wire: &proto.Request{Op: proto.OpJobStatus, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{ID: in.ID}}})
		if err != nil {
			return nil, JobOut{}, err
		}
		if !resp.OK {
			return nil, JobOut{}, errors.New(resp.Error)
		}
		if resp.Wire == nil || resp.Wire.Job == nil || resp.Wire.Job.Info == nil {
			return nil, JobOut{}, errors.New("broker job status returned no result")
		}
		return nil, toJobOut(resp.Wire.Job.Info), nil
	})
	return s, nil
}
