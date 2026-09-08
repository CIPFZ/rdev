package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func main() {
	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "approval-create":
		err = approvalCreateCommand(os.Args[2:], os.Stdout)
	case len(os.Args) > 1 && os.Args[1] == "principal-token":
		err = principalTokenCommand(os.Args[2:], os.Stdout)
	case len(os.Args) > 1 && os.Args[1] == "principal-keygen":
		err = principalKeygenCommand(os.Args[2:])
	default:
		err = runDaemon(os.Args[1:])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func agentLookup(dir string) func(string, string) (*transport.AgentBinary, error) {
	return func(goos, goarch string) (*transport.AgentBinary, error) {
		path := filepath.Join(dir, fmt.Sprintf("rdev-agent-%s-%s", goos, goarch))
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("agent %s/%s unavailable: %w", goos, goarch, err)
		}
		sum := sha256.Sum256(data)
		return &transport.AgentBinary{Data: data, SHA256: hex.EncodeToString(sum[:])}, nil
	}
}

func serveConn(conn net.Conn, service *broker.Service) {
	defer conn.Close()
	if !service.Ready() {
		return
	}
	if unixConn, ok := conn.(*net.UnixConn); ok {
		if same, err := broker.PeerIsCurrentUser(unixConn); err != nil || !same {
			return
		}
	}
	var hello proto.BrokerHello
	reader := bufio.NewReader(conn)
	dec := json.NewDecoder(reader)
	if err := dec.Decode(&hello); err != nil {
		return
	}
	local := proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}
	resp := proto.BrokerHelloResponse{Version: local.Version, MinVersion: local.MinVersion}
	if err := proto.ValidateBrokerHello(local, hello); err != nil {
		resp.Error = err.Error()
		_ = json.NewEncoder(conn).Encode(resp)
		return
	}
	var boundOwner broker.Owner
	if service.Principals.Required() || hello.ClientID != "" || hello.ProjectID != "" {
		boundOwner = broker.Owner{ClientID: hello.ClientID, ProjectID: hello.ProjectID}
		if err := boundOwner.Validate(); err != nil {
			resp.Error = err.Error()
			_ = json.NewEncoder(conn).Encode(resp)
			return
		}
	}
	var revoked <-chan struct{}
	principalDeadline, revoked, err := service.Principals.Authenticate(boundOwner, hello.PrincipalToken)
	if err != nil {
		resp.Error = err.Error()
		_ = json.NewEncoder(conn).Encode(resp)
		return
	}
	if !principalDeadline.IsZero() {
		_ = conn.SetDeadline(principalDeadline)
	}
	// Rotation closes authenticated sessions, including idle clients and calls.
	sessionDone := make(chan struct{})
	defer close(sessionDone)
	if revoked != nil {
		go func() {
			select {
			case <-revoked:
				_ = conn.Close()
			case <-sessionDone:
			}
		}()
	}

	if !service.AttachClient() {
		return
	}
	defer service.DetachClient()
	resp.OK = true
	if err := json.NewEncoder(conn).Encode(resp); err != nil {
		return
	}
	enc := json.NewEncoder(conn)
	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requests := make(chan broker.Request, 16)
	decodeErr := make(chan error, 1)
	go func() {
		for {
			var req broker.Request
			if err := dec.Decode(&req); err != nil {
				cancel()
				decodeErr <- err
				return
			}
			select {
			case requests <- req:
			case <-connCtx.Done():
				return
			}
		}
	}()
	for {
		var req broker.Request
		select {
		case req = <-requests:
		case <-decodeErr:
			return
		case <-connCtx.Done():
			return
		}
		select {
		case <-revoked:
			return
		default:
		}
		if !principalDeadline.IsZero() && !time.Now().Before(principalDeadline) {
			return
		}
		if !service.BeginRequest() {
			_ = enc.Encode(broker.Response{ID: req.ID, Error: "broker draining"})
			continue
		}
		admitDone := true
		endRequest := func() {
			if admitDone {
				admitDone = false
				service.EndRequest()
			}
		}
		requestCtx := connCtx
		if boundOwner != (broker.Owner{}) && req.Owner != boundOwner {
			_ = enc.Encode(broker.Response{ID: req.ID, Error: "owner cannot change on an authenticated connection"})
			endRequest()
			continue
		}
		if boundOwner == (broker.Owner{}) {
			boundOwner = req.Owner
		}
		if err := req.Owner.Validate(); err != nil {
			_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
			endRequest()
			continue
		}
		decision := service.DecideRequest(req.Owner, req.Operation, req.Host)
		if req.Capability != "" && req.Capability != decision.Capability {
			decision.Allow = false
			decision.Reason = "capability mismatch"
		}
		if !decision.Allow {
			service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), PolicyDigest: decision.Digest, Owner: req.Owner.Key(), Operation: req.Operation, Decision: decision.Reason, Result: "denied"})
			_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: decision.Reason})
			endRequest()
			continue
		}
		if req.Wire != nil {
			if req.Host == "" {
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "host required for wire request"})
				endRequest()
				continue
			}
			if req.Operation != "" && req.Wire.Op != req.Operation {
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "operation mismatch"})
				endRequest()
				continue
			}
			if (req.Wire.ClientID != "" && req.Wire.ClientID != req.Owner.ClientID) || (req.Wire.ProjectID != "" && req.Wire.ProjectID != req.Owner.ProjectID) {
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "wire owner mismatch"})
				endRequest()
				continue
			}
			req.Wire.ClientID = req.Owner.ClientID
			req.Wire.ProjectID = req.Owner.ProjectID
			if broker.IsWireMutation(req) {
				if req.Wire.OperationID == "" {
					id, err := proto.NewOperationID()
					if err != nil {
						endRequest()
						return
					}
					req.Wire.OperationID = id
				}
				if proto.ValidateOperationID(req.Wire.OperationID) != nil {
					_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "invalid mutation operation ID"})
					endRequest()
					continue
				}
			}
			if req.Wire.Job != nil || strings.HasPrefix(req.Wire.Op, "job_") {
				bound, err := service.Jobs.BindRequest(req.Host, req.Owner.Key(), req.Wire)
				if err != nil {
					_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
					endRequest()
					continue
				}
				req.Wire = bound
				if err := service.ValidateJobRemoval(req.Host, req.Owner.Key(), req.Wire); err != nil {
					_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
					endRequest()
					continue
				}
			}
		}
		if req.Operation == "approval.create" {
			if req.ApprovalSpec == nil {
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "approval_spec required"})
			} else {
				approval, err := service.IssueApproval(*req.ApprovalSpec)
				if err != nil {
					_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
				} else {
					service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), PolicyDigest: decision.Digest, Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "approval_issued", RequestDigest: approval.Plan.RequestDigest, TargetDigest: approval.Plan.TargetDigest, ApprovalID: broker.ApprovalReference(approval.Token)})
					_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Approval: &approval})
				}
			}
			endRequest()
			continue
		}
		var approvedTarget string
		var approvedPlan broker.ApprovalPlan
		if broker.RequiresApproval(req) {
			plan, err := service.AuthorizeApproval(req, decision)
			if err != nil {
				service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), PolicyDigest: decision.Digest, Owner: req.Owner.Key(), Operation: req.Operation, Decision: "approval_denied", Result: "approval_invalid"})
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
				endRequest()
				continue
			}
			approvedTarget = plan.TargetDigest
			approvedPlan = plan
			service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), PolicyDigest: decision.Digest, Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "approval_used", RequestDigest: plan.RequestDigest, TargetDigest: plan.TargetDigest, ApprovalID: plan.ApprovalID})
		}
		if req.Wire != nil {
			service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), RequestDigest: approvedPlan.RequestDigest, TargetDigest: approvedPlan.TargetDigest, ApprovalID: approvedPlan.ApprovalID, PolicyDigest: decision.Digest, Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "admitted"})
		}
		if req.Operation == "audit.health" {
			health := service.Audit.SinkStatus()
			_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, AuditHealth: &health})
			endRequest()
			continue
		}
		if req.Operation == "mutation.status" {
			m, err := service.Mutations.Get(req.Owner.Key(), req.MutationID)
			if err != nil {
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
			} else {
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Mutation: &m})
			}
			endRequest()
			continue
		}
		if req.Operation == "audit_query" {
			flushCtx, flushCancel := context.WithTimeout(requestCtx, time.Second)
			flushErr := service.Audit.Flush(flushCtx)
			flushCancel()
			_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Audit: service.Audit.QueryOwner(req.Since, req.Owner.Key()), AuditIncomplete: flushErr != nil || service.Audit.HasLegacyRecords() || service.Audit.SinkStatus().State == "degraded"})
			endRequest()
			continue
		}
		if req.Operation == "policy.grant" {
			var policyErr error
			if req.GrantHost != "" {
				policyErr = service.GrantHost(req.GrantOwner, req.GrantHost, req.GrantCapability, req.GrantOperation, req.Revoke)
			} else if req.GrantCapability != "" {
				if req.Revoke {
					policyErr = service.RevokeCapability(req.GrantOwner, req.GrantCapability, req.GrantOperation)
				} else {
					policyErr = service.GrantCapability(req.GrantOwner, req.GrantCapability, req.GrantOperation)
				}
			} else if req.Revoke {
				policyErr = service.Revoke(req.GrantOwner, req.GrantOperation)
			} else {
				policyErr = service.Grant(req.GrantOwner, req.GrantOperation)
			}
			if policyErr != nil {
				service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), RequestDigest: approvedPlan.RequestDigest, TargetDigest: approvedPlan.TargetDigest, ApprovalID: approvedPlan.ApprovalID, PolicyDigest: decision.Digest, Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "policy_update_failed"})
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: policyErr.Error()})
			} else {
				service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), RequestDigest: approvedPlan.RequestDigest, TargetDigest: approvedPlan.TargetDigest, ApprovalID: approvedPlan.ApprovalID, PolicyDigest: decision.Digest, At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "policy_updated"})
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true})
			}
			endRequest()
			continue
		}
		lane := broker.LaneForOperation(req.Operation)
		if req.Wire != nil {
			dispatch := func(ctx context.Context) (*proto.Response, error) {
				return service.DispatchScheduled(ctx, req.Host, req.Owner.Key(), lane, func(dispatchCtx context.Context) (*proto.Response, error) {
					if approvedTarget != "" {
						return service.DispatchApproved(dispatchCtx, req.Host, req.Wire, approvedTarget)
					}
					return service.Dispatch(dispatchCtx, req.Host, req.Wire)
				})
			}
			var wireResp *proto.Response
			var mutation *broker.MutationIntent
			var err error
			if req.Wire.Op == proto.OpJobWait && req.Wire.Job != nil {
				jobKey, _ := json.Marshal([]any{req.Owner, req.Host, req.Wire.Job, req.Wire.DeadlineUnixMilli})
				wireResp, err = service.DispatchShared(requestCtx, req.Owner.Key(), string(jobKey), dispatch)
			} else if broker.IsWireMutation(req) {
				wireResp, mutation, err = service.DispatchMutation(requestCtx, req, approvedPlan)
			} else {
				wireResp, err = dispatch(requestCtx)
			}
			if err != nil {
				result := "dispatch_error"
				if errors.Is(err, broker.ErrQueueFull) {
					result = "quota_rejected"
				}
				service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), RequestDigest: approvedPlan.RequestDigest, TargetDigest: approvedPlan.TargetDigest, ApprovalID: approvedPlan.ApprovalID, PolicyDigest: decision.Digest, At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: result})
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error(), Mutation: mutation})
				endRequest()
				continue
			}
			var recordErr error
			if !broker.IsWireMutation(req) {
				recordErr = service.Jobs.RecordResponse(req.Host, req.Owner.Key(), req.Wire, wireResp)
			}
			if recordErr == nil && wireResp != nil && wireResp.Job != nil {
				recordErr = service.ResolveMutationJob(req.Host, req.Owner.Key(), wireResp.Job.Info)
			}
			if recordErr != nil {
				// Remote mutation completed but durable ownership state did not.
				// Return an ambiguous outcome and never replay the mutation.
				failure := "remote job response could not be validated"
				if broker.RequiresApproval(req) {
					failure = "mutation completed but broker state was not persisted; query remote status"
				}
				service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), RequestDigest: approvedPlan.RequestDigest, TargetDigest: approvedPlan.TargetDigest, ApprovalID: approvedPlan.ApprovalID, PolicyDigest: decision.Digest, Owner: req.Owner.Key(), Operation: req.Operation, Result: "state_persist_failed"})
				_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: failure})
				endRequest()
				continue
			}
			service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), RequestDigest: approvedPlan.RequestDigest, TargetDigest: approvedPlan.TargetDigest, ApprovalID: approvedPlan.ApprovalID, PolicyDigest: decision.Digest, At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "completed"})
			_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Wire: wireResp, Mutation: mutation})
			endRequest()
			continue
		}
		var scheduler *broker.SchedulerSnapshot
		var sharedWaits *broker.SharedWaitStatus
		if req.Operation == "status" {
			snapshot := service.Scheduler.Snapshot(req.Owner.Key())
			scheduler = &snapshot
			waits := service.SharedWaitStatus(req.Owner.Key())
			sharedWaits = &waits
		}
		service.Audit.Append(broker.AuditEvent{OperationRef: broker.OperationReference(req), RequestDigest: approvedPlan.RequestDigest, TargetDigest: approvedPlan.TargetDigest, ApprovalID: approvedPlan.ApprovalID, PolicyDigest: decision.Digest, At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "accepted"})
		_ = enc.Encode(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Scheduler: scheduler, SharedWaits: sharedWaits})
		endRequest()
	}
}
