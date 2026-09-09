package mcpsrv

import (
	"context"
	"errors"
	"fmt"

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
	s.AddReceivingMiddleware(projectResults(nil, nil))
	registerFleet(s, socket, owner)
	registerCompat(s)
	registerBrokerSupport(s, socket, owner)
	registerBrokerState(s, socket, owner)
	registerBrokerSecrets(s, socket, owner)
	registerBrokerSync(s, socket, owner)
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_broker_pool", Description: "Read global shared connection capacity, active leases and eviction reasons. Requires a separate pool.health grant."}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, broker.PoolHealth, error) {
		r, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "pool.health"})
		if err != nil {
			return nil, broker.PoolHealth{}, err
		}
		health, err := broker.ProjectPoolHealth(r)
		return nil, health, err
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_broker_status", Description: "Read this principal's shared broker connections, request bytes, execution quotas, lane counts and wait observers."}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, broker.StatusSnapshot, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "status"})
		if err != nil {
			return nil, broker.StatusSnapshot{}, err
		}
		status, err := broker.ProjectStatus(resp)
		return nil, status, err
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_list", Description: "List a remote directory through the shared broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListIn) (*mcp.CallToolResult, proto.ListResult, error) {
		path := in.Path
		if path == "" {
			path = "."
		}
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: proto.OpList, Host: in.Host, Wire: &proto.Request{Op: proto.OpList, List: &proto.ListParams{Path: path, Limit: in.Limit}}})
		if err != nil {
			return nil, proto.ListResult{}, err
		}
		if resp.Wire == nil || resp.Wire.List == nil {
			return nil, proto.ListResult{}, errors.New("broker list returned no result")
		}
		return nil, *resp.Wire.List, nil
	})

	mcp.AddTool(s, &mcp.Tool{Name: "rdev_ping", Description: "Verify a remote host through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in struct {
		Host string `json:"host"`
	}) (*mcp.CallToolResult, proto.PingResult, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "ping", Host: in.Host, Wire: &proto.Request{Op: proto.OpPing}})
		if err != nil {
			return nil, proto.PingResult{}, err
		}
		if resp.Wire == nil || resp.Wire.Ping == nil {
			return nil, proto.PingResult{}, errors.New("broker ping returned no result")
		}
		return nil, *resp.Wire.Ping, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_exec", Description: "Run a command through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ExecIn) (*mcp.CallToolResult, ExecOut, error) {
		if len(in.Argv) == 0 {
			return nil, ExecOut{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		login := true
		if in.LoginShell != nil {
			login = *in.LoginShell
		}
		resp, err := callBroker(ctx, socket, owner, broker.Request{Approval: in.ApprovalToken, Owner: owner, Operation: "exec", Host: in.Host, Wire: &proto.Request{OperationID: in.OperationID, Op: proto.OpExec, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Exec: &proto.ExecParams{Argv: in.Argv, Cwd: in.Cwd, Env: in.Env, LoginShell: login, Stdin: in.Stdin, TimeoutSec: in.TimeoutSec, MaxOutputBytes: in.MaxOutputBytes}}})
		if err != nil {
			return nil, ExecOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Exec == nil {
			return nil, ExecOut{}, errors.New("broker exec returned no result")
		}
		return nil, toExecOut(&client.ExecResult{ExecResult: resp.Wire.Exec, Cwd: in.Cwd}), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_read", Description: "Read a remote file through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ReadIn) (*mcp.CallToolResult, ReadOut, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "read_file", Host: in.Host, Wire: &proto.Request{Op: proto.OpReadFile, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Read: &proto.ReadParams{Path: in.Path, Offset: in.Offset, Limit: in.Limit}}})
		if err != nil {
			return nil, ReadOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Read == nil {
			return nil, ReadOut{}, errors.New("broker read returned no result")
		}
		r := resp.Wire.Read
		return nil, ReadOut{Content: r.Content, Base64: r.ContentB64, Size: r.Size, EOF: r.EOF, Truncation: r.Truncation, OperationID: r.OperationID, Terminal: r.Terminal, ExecutionState: r.Execution}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_write", Description: "Write a remote file through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in WriteIn) (*mcp.CallToolResult, WriteOut, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Approval: in.ApprovalToken, Owner: owner, Operation: "write_file", Host: in.Host, Wire: &proto.Request{OperationID: in.OperationID, Op: proto.OpWriteFile, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Cat: &proto.WriteParams{Path: in.Path, Content: in.Content, Mode: in.Mode, Append: in.Append}}})
		if err != nil {
			return nil, WriteOut{}, err
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
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "capability_probe", Host: in.Host, Wire: &proto.Request{Op: proto.OpCapabilityProbe, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Capability: &proto.CapabilityParams{Refresh: in.Refresh}}})
		if err != nil {
			return nil, proto.CapabilityResult{}, err
		}
		if resp.Wire == nil || resp.Wire.Capability == nil {
			return nil, proto.CapabilityResult{}, errors.New("broker capability returned no result")
		}
		return nil, *resp.Wire.Capability, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_start", Description: "Start a supervised background job through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobStartIn) (*mcp.CallToolResult, JobOut, error) {
		if len(in.Argv) == 0 {
			return nil, JobOut{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		login := true
		if in.LoginShell != nil {
			login = *in.LoginShell
		}
		resp, err := callBroker(ctx, socket, owner, broker.Request{Approval: in.ApprovalToken, Owner: owner, Operation: "job_start", Host: in.Host, Wire: &proto.Request{OperationID: in.OperationID, Op: proto.OpJobStart, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{Label: in.Label, Resources: in.Resources, Spec: &proto.ExecParams{Argv: in.Argv, Cwd: in.Cwd, Env: in.Env, LoginShell: login}}}})
		if err != nil {
			return nil, JobOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Job == nil || resp.Wire.Job.Info == nil {
			return nil, JobOut{}, errors.New("broker job start returned no result")
		}
		return nil, toJobOut(resp.Wire.Job.Info), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_list", Description: "List supervised background jobs through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobListIn) (*mcp.CallToolResult, JobListOut, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "job_list", Host: in.Host, Wire: &proto.Request{Op: proto.OpJobList, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{Limit: in.Limit}}})
		if err != nil {
			return nil, JobListOut{}, err
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
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "job_status", Host: in.Host, Wire: &proto.Request{Op: proto.OpJobStatus, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{ID: in.ID}}})
		if err != nil {
			return nil, JobOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Job == nil || resp.Wire.Job.Info == nil {
			return nil, JobOut{}, errors.New("broker job status returned no result")
		}
		return nil, toJobOut(resp.Wire.Job.Info), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_logs", Description: "Read supervised job output through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobLogsIn) (*mcp.CallToolResult, JobLogsOut, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "job_logs", Host: in.Host, Wire: &proto.Request{Op: proto.OpJobLogs, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{ID: in.ID, Stream: in.Stream, TailLines: in.TailLines, Grep: in.Grep, SinceOffset: in.SinceOffset}}})
		if err != nil {
			return nil, JobLogsOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Job == nil {
			return nil, JobLogsOut{}, errors.New("broker job logs returned no result")
		}
		j := resp.Wire.Job
		return nil, toJobLogsOut(j), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_stop", Description: "Stop a supervised job through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobStopIn) (*mcp.CallToolResult, JobOut, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Approval: in.ApprovalToken, Owner: owner, Operation: "job_stop", Host: in.Host, Wire: &proto.Request{OperationID: in.OperationID, Op: proto.OpJobStop, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{ID: in.ID, Signal: in.Signal, GraceSec: in.GraceSec}}})
		if err != nil {
			return nil, JobOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Job == nil || resp.Wire.Job.Info == nil {
			return nil, JobOut{}, errors.New("broker job stop returned no result")
		}
		return nil, toJobOut(resp.Wire.Job.Info), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_rm", Description: "Remove supervised job records through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobRmIn) (*mcp.CallToolResult, JobRmOut, error) {
		if in.ID == "" && in.OlderThanSec <= 0 && in.KeepLast <= 0 {
			return nil, JobRmOut{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		resp, err := callBroker(ctx, socket, owner, broker.Request{Approval: in.ApprovalToken, Owner: owner, Operation: "job_rm", Host: in.Host, Wire: &proto.Request{OperationID: in.OperationID, Op: proto.OpJobRm, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{ID: in.ID, OlderThanSec: in.OlderThanSec, KeepLast: in.KeepLast}}})
		if err != nil {
			return nil, JobRmOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Job == nil {
			return nil, JobRmOut{}, errors.New("broker job rm returned no result")
		}
		j := resp.Wire.Job
		return nil, JobRmOut{Removed: j.Removed, RemovedCount: len(j.Removed), Skipped: j.Skipped, Missing: j.Missing, FreedBytes: j.FreedBytes}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_wait", Description: "Wait for supervised jobs through the shared local broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in JobWaitIn) (*mcp.CallToolResult, JobWaitOut, error) {
		if in.ID == "" && len(in.IDs) == 0 {
			return nil, JobWaitOut{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "job_wait", Host: in.Host, Wire: &proto.Request{Op: proto.OpJobWait, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Job: &proto.JobParams{ID: in.ID, IDs: in.IDs, WaitAny: in.WaitAny, WaitTimeoutSec: in.TimeoutSec, TailOnExit: in.TailOnExit}}})
		if err != nil {
			return nil, JobWaitOut{}, err
		}
		if resp.Wire == nil || resp.Wire.Job == nil || (resp.Wire.Job.Info == nil && len(resp.Wire.Job.Waited) == 0) {
			return nil, JobWaitOut{}, errors.New("broker job wait returned no result")
		}
		j := resp.Wire.Job
		out := JobWaitOut{TimedOut: j.TimedOut, WaitedMS: j.WaitedMS, Logs: j.Logs, LogsTruncation: j.LogsTruncation, OperationID: j.OperationID, Terminal: j.Terminal, ExecutionState: j.Execution}
		if j.Info != nil {
			out.Job = toJobOut(j.Info)
		}
		for _, w := range j.Waited {
			out.Waited = append(out.Waited, WaitedJobOut{ID: w.ID, Job: toJobOut(w.Info), Err: w.Err, Logs: w.Logs, LogsTruncation: w.LogsTruncation})
		}
		return nil, out, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_mutation_status", Description: "Read this principal's durable mutation outcome without executing it again."}, func(ctx context.Context, _ *mcp.CallToolRequest, in struct {
		OperationID string `json:"operation_id"`
	}) (*mcp.CallToolResult, broker.MutationIntent, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Operation: "mutation.status", MutationID: in.OperationID})
		if err != nil {
			return nil, broker.MutationIntent{}, err
		}
		if resp.Mutation == nil {
			return nil, broker.MutationIntent{}, errors.New("broker returned no mutation state")
		}
		return nil, *resp.Mutation, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_job_events", Description: "Replay this principal's retained job state history using its cursor. Truncated reports unavailable earlier history; events exclude command text and output."}, func(ctx context.Context, _ *mcp.CallToolRequest, in struct {
		Host   string                `json:"host"`
		ID     string                `json:"id"`
		Cursor broker.JobEventCursor `json:"cursor,omitempty"`
		Limit  int                   `json:"limit,omitempty"`
	}) (*mcp.CallToolResult, broker.JobEventPage, error) {
		resp, err := callBroker(ctx, socket, owner, broker.Request{Operation: "job.events", Host: in.Host, JobEvents: &broker.JobEventQuery{ID: in.ID, Cursor: in.Cursor, Limit: in.Limit}})
		if err != nil {
			return nil, broker.JobEventPage{}, err
		}
		if resp.History == nil {
			return nil, broker.JobEventPage{}, errors.New("broker returned no job history")
		}
		return nil, *resp.History, nil
	})
	return s, nil
}

// callBroker enforces both local and remote failure envelopes before a handler
// projects typed output. Secrets and session state are resolved only by rdevd.
func callBroker(ctx context.Context, socket string, owner broker.Owner, req broker.Request) (broker.Response, error) {
	if req.Wire != nil {
		_, err := proto.NormalizeTimeouts(req.Wire)
		if err != nil {
			return broker.Response{}, err
		}
	}
	if req.Wire != nil {
		req.Wire.ClientID = owner.ClientID
		req.Wire.ProjectID = owner.ProjectID
	}
	c, err := broker.DialClient(ctx, socket, owner)
	if err != nil {
		return broker.Response{}, err
	}
	defer c.Close()
	resp, err := c.DoContext(ctx, req)
	if err != nil {
		return broker.Response{}, err
	}
	if !resp.OK {
		if resp.Error != "" {
			return broker.Response{}, errors.New(resp.Error)
		}
		return broker.Response{}, fmt.Errorf("broker %s failed", req.Operation)
	}
	if resp.Wire != nil {
		if resp.Wire.Error != nil {
			return broker.Response{}, resp.Wire.Error
		}
		if resp.Wire.Err != "" {
			return broker.Response{}, errors.New(resp.Wire.Err)
		}
		if !resp.Wire.OK {
			return broker.Response{}, fmt.Errorf("remote %s failed", req.Operation)
		}
	}
	return resp, nil
}

func registerBrokerSecrets(s *mcp.Server, socket string, owner broker.Owner) {
	type secretInput struct {
		Action        string `json:"action" jsonschema:"set, set_from_file, delete or list"`
		Path          string `json:"path,omitempty"`
		Host          string `json:"host"`
		Name          string `json:"name,omitempty"`
		Value         string `json:"value,omitempty"`
		ApprovalToken string `json:"approval_token,omitempty"`
		OperationID   string `json:"operation_id,omitempty"`
	}
	type secretOutput struct {
		Secrets  []broker.SecretDescriptor `json:"secrets,omitempty"`
		Mutation *broker.MutationIntent    `json:"mutation,omitempty"`
	}
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_secrets", Description: "Manage this principal's host-scoped credentials in rdevd. Values are never returned. Set/import/delete require exact approval; remote imports also require file-read permission. Credentials persist in private daemon state. Exec/job secret references require secret.use."}, func(ctx context.Context, _ *mcp.CallToolRequest, in secretInput) (*mcp.CallToolResult, secretOutput, error) {
		if in.Action != "set" && in.Action != "delete" && in.Action != "list" && in.Action != "set_from_file" {
			return nil, secretOutput{}, errors.New("unknown secret action")
		}
		r, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Host: in.Host, Operation: "secret." + in.Action, Secret: &broker.SecretParams{Name: in.Name, Value: in.Value, Path: in.Path}, Approval: in.ApprovalToken, OperationID: in.OperationID})
		if err != nil {
			return nil, secretOutput{}, err
		}
		return nil, secretOutput{Secrets: r.Secrets, Mutation: r.Mutation}, nil
	})
}
