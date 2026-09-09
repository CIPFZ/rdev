package main

import (
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
	"github.com/CIPFZ/rdev/internal/client"
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
	lease, err := service.Ingress.Open()
	if err != nil {
		_ = conn.Close()
		return
	}
	serveIngressConn(conn, service, lease)
}

func serveIngressConn(conn net.Conn, service *broker.Service, lease *broker.IngressLease) {
	defer lease.Close()
	defer conn.Close()
	connCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	enc := &boundedBrokerEncoder{conn: conn, cancel: cancel}

	if !service.Ready() {
		return
	}
	if unixConn, ok := conn.(*net.UnixConn); ok {
		if same, err := broker.PeerIsCurrentUser(unixConn); err != nil || !same {
			return
		}
	}
	var hello proto.BrokerHello
	handshakeDeadline := time.Now().Add(brokerFrameTimeout)
	_ = conn.SetReadDeadline(handshakeDeadline)
	dec := &boundedBrokerDecoder{conn: conn, lease: lease, deadline: handshakeDeadline}
	releaseHello, err := dec.Decode(&hello, broker.MaxBrokerHelloBytes)
	if err != nil {
		return
	}
	releaseHello()
	local := proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}
	resp := proto.BrokerHelloResponse{Version: local.Version, MinVersion: local.MinVersion}
	if err := proto.ValidateBrokerHello(local, hello); err != nil {
		resp.Error = err.Error()
		_ = enc.Encode(resp)
		return
	}
	var boundOwner broker.Owner
	if service.Principals.Required() || hello.ClientID != "" || hello.ProjectID != "" {
		boundOwner = broker.Owner{ClientID: hello.ClientID, ProjectID: hello.ProjectID}
		if err := boundOwner.Validate(); err != nil {
			resp.Error = err.Error()
			_ = enc.Encode(resp)
			return
		}
	}
	var revoked <-chan struct{}
	principalDeadline, revoked, err := service.Principals.Authenticate(boundOwner, hello.PrincipalToken)
	if err != nil {
		resp.Error = err.Error()
		_ = enc.Encode(resp)
		return
	}
	if boundOwner != (broker.Owner{}) {
		if err := lease.Bind(boundOwner); err != nil {
			resp.Error = err.Error()
			_ = enc.Encode(resp)
			return
		}
	}
	dec.deadline = principalDeadline
	enc.deadline = principalDeadline
	_ = conn.SetReadDeadline(principalDeadline)
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
	if err := enc.Encode(resp); err != nil {
		return
	}
	type queuedRequest struct {
		request broker.Request
		bytes   int64
		release func()
	}
	requests := make(chan queuedRequest, 2)
	go func() {
		defer cancel()
		for {
			var req broker.Request
			release, err := dec.Decode(&req, maxBrokerRequestBytes)
			if err != nil {
				_ = conn.Close()
				return
			}
			select {
			case requests <- queuedRequest{req, dec.lastBytes, release}:
			case <-connCtx.Done():
				release()
				return
			default:
				// Never block the reader behind a full queue: it must detect disconnect
				// and cancel the active request independently of response consumption.
				release()
				_ = conn.Close()
				return
			}
		}
	}()
	for {
		var item queuedRequest
		select {
		case item = <-requests:
		case <-connCtx.Done():
			return
		}
		if connCtx.Err() != nil {
			item.release()
			return
		}
		req := item.request
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
			item.release()
			continue
		}
		admitDone := true
		var expandedBytes int64
		endRequest := func() {
			if admitDone {
				admitDone = false
				service.EndRequest()
				item.release()
				lease.Release(expandedBytes)
			}
		}
		requestCtx := connCtx
		if boundOwner != (broker.Owner{}) && req.Owner != boundOwner {
			_ = enc.Encode(broker.Response{ID: req.ID, Error: "owner cannot change on an authenticated connection"})
			endRequest()
			continue
		}
		if boundOwner == (broker.Owner{}) {
			if err := lease.Bind(req.Owner); err != nil {
				_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
				endRequest()
				return
			}
			boundOwner = req.Owner
		}
		if err := req.Owner.Validate(); err != nil {
			_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
			endRequest()
			continue
		}
		requestRef, refErr := broker.NewRequestReference()
		if refErr != nil {
			_ = enc.Encode(broker.Response{ID: req.ID, Error: "broker request identity unavailable"})
			endRequest()
			continue
		}
		respond := func(response broker.Response) error {
			response.RequestRef = requestRef
			return enc.Encode(response)
		}
		decision := service.DecideBrokerRequest(req)
		if req.Capability != "" && req.Capability != decision.Capability {
			decision.Allow = false
			decision.Reason = "capability mismatch"
		}
		auditBase, auditErr := service.Audit.RequestEvent(req, requestRef, decision)
		if auditErr != nil {
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "broker audit identity unavailable"})
			endRequest()
			continue
		}
		recordDecision := func(result, reason string) {
			event := auditBase
			event.OperationRef = broker.OperationReference(req)
			event.Decision, event.Result = reason, result
			service.Audit.Append(event)
		}
		recordResult := func(result string) { recordDecision(result, "allow") }
		if !decision.Allow {
			recordDecision("denied", decision.Reason)
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: decision.Reason})
			endRequest()
			continue
		}
		if err := broker.ValidateRoute(req); err != nil {
			recordResult("route_rejected")
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
			endRequest()
			continue
		}
		if req.Host != "" && (req.Wire != nil || req.Secret != nil || req.Sync != nil) {
			if err := service.BindAuditTarget(&auditBase, req.Host); err != nil {
				recordResult("request_rejected")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "broker audit identity unavailable"})
				endRequest()
				continue
			}
		}
		if req.Wire != nil {
			if (req.Wire.ClientID != "" && req.Wire.ClientID != req.Owner.ClientID) || (req.Wire.ProjectID != "" && req.Wire.ProjectID != req.Owner.ProjectID) {
				recordResult("request_rejected")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "wire owner mismatch"})
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
					recordResult("request_rejected")
					_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "invalid mutation operation ID"})
					endRequest()
					continue
				}
			}
			if req.Wire.Job != nil || strings.HasPrefix(req.Wire.Op, "job_") {
				bound, err := service.Jobs.BindRequest(req.Host, req.Owner.Key(), req.Wire)
				if err != nil {
					recordResult("request_rejected")
					_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
					endRequest()
					continue
				}
				req.Wire = bound
				if err := service.ValidateJobRemoval(req.Host, req.Owner.Key(), req.Wire); err != nil {
					recordResult("request_rejected")
					_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
					endRequest()
					continue
				}
			}
		}
		if req.Operation == "secret.set" || req.Operation == "secret.delete" || req.Operation == "secret.set_from_file" || (req.Operation == "sync.push" || req.Operation == "sync.pull") && req.Sync != nil && !req.Sync.DryRun {
			if req.OperationID == "" {
				req.OperationID, refErr = proto.NewOperationID()
			}
			if refErr != nil || proto.ValidateOperationID(req.OperationID) != nil {
				recordResult("request_rejected")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "invalid mutation operation ID"})
				endRequest()
				continue
			}
		}
		if req.Operation == "approval.create" {
			if req.ApprovalSpec == nil {
				recordResult("request_rejected")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "approval_spec required"})
			} else {
				approval, err := service.IssueApprovalContext(requestCtx, *req.ApprovalSpec)
				if err != nil {
					recordResult("request_rejected")
					_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
				} else {
					auditBase.BindApproval(*approval.Plan, broker.ApprovalReference(approval.Token))
					recordResult("approval_issued")
					_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Approval: &approval})
				}
			}
			endRequest()
			continue
		}
		var approvedTarget string
		var approvedPlan broker.ApprovalPlan
		if broker.RequiresApproval(req) {
			plan, err := service.AuthorizeApprovalContext(requestCtx, req, decision)
			if err != nil {
				recordDecision("approval_invalid", "approval_denied")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
				endRequest()
				continue
			}
			approvedTarget = plan.TargetDigest
			approvedPlan = plan
			auditBase.BindApproval(plan, plan.ApprovalID)
			recordResult("approval_used")
			expandedBytes = plan.ExpandedRequestBytes()
			if err := lease.Reserve(expandedBytes); err != nil {
				expandedBytes = 0
				recordResult("quota_rejected")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
				endRequest()
				continue
			}
			broker.ApplyApprovedWire(&req, plan)
		}
		if req.Wire != nil || req.Sync != nil {
			recordResult("admitted")
		}
		if req.Operation == "sync.push" || req.Operation == "sync.pull" {
			budget := broker.SyncPreviewBudget(req)
			if err := lease.Reserve(budget); err != nil {
				recordResult("quota_rejected")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
				endRequest()
				continue
			}
			expandedBytes += budget
			var result *client.SyncResult
			var mutation *broker.MutationIntent
			var err error
			switch {
			case req.Sync.Prepare:
				result, err = service.PrepareSync(requestCtx, req)
			case !req.Sync.DryRun:
				result, mutation, err = service.ExecuteSync(requestCtx, req, approvedPlan)
			default:
				result, err = service.PreviewSync(requestCtx, req)
			}
			outcome, message := "completed", ""
			if err != nil {
				outcome, message = "dispatch_error", service.Client().Secrets.Redact(err.Error())
			}
			recordResult(outcome)
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: err == nil, Error: message, Sync: result, Mutation: mutation})
			endRequest()
			continue
		}
		if req.Operation == "secret.list" {
			entries, err := service.SecretList(req)
			result := "completed"
			message := ""
			if err != nil {
				result = "dispatch_error"
				message = err.Error()
			}
			recordResult(result)
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: err == nil, Error: message, Secrets: entries})
			endRequest()
			continue
		}
		if req.Operation == "secret.set" || req.Operation == "secret.delete" || req.Operation == "secret.set_from_file" {
			recordResult("admitted")
			mutation, err := service.DispatchSecretMutation(requestCtx, req, approvedPlan)
			result := "completed"
			message := ""
			if err != nil {
				result = "dispatch_error"
				message = err.Error()
			}
			recordResult(result)
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: err == nil, Error: message, Mutation: mutation})
			endRequest()
			continue
		}
		if req.Operation == "pool.health" {
			recordResult("completed")
			health := service.PoolHealth()
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Pool: &health})
			endRequest()
			continue
		}
		if req.Operation == "audit.health" {
			recordResult("completed")
			health := service.Audit.SinkStatus()
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, AuditHealth: &health})
			endRequest()
			continue
		}
		if req.Operation == "mutation.status" {
			m, err := service.Mutations.Get(req.Owner.Key(), req.MutationID)
			if err == nil && req.Host != "" && m.Host != req.Host {
				err = errors.New("mutation unknown for principal")
			}
			if err == nil && (m.Operation == "sync.push" || m.Operation == "sync.pull") {
				m, err = service.ResolveSyncMutation(requestCtx, req.Owner, m)
			}
			if err != nil {
				recordResult("request_rejected")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
			} else {
				recordResult("completed")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Mutation: &m})
			}
			endRequest()
			continue
		}
		if req.Operation == "job.events" {
			result := "completed"
			if req.JobEvents == nil {
				result = "dispatch_error"
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "job event query required"})
			} else {
				page, err := service.Events.Query(req.Owner.Key(), req.Host, req.JobEvents.ID, req.JobEvents.Cursor, req.JobEvents.Limit)
				if err != nil {
					result = "dispatch_error"
					_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error()})
				} else {
					_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, History: &page})
				}
			}
			recordResult(result)
			endRequest()
			continue
		}
		if req.Operation == "audit_query" {
			recordResult("completed")
			flushCtx, flushCancel := context.WithTimeout(requestCtx, time.Second)
			flushErr := service.Audit.Flush(flushCtx)
			flushCancel()
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Audit: service.Audit.QueryOwner(req.Since, req.Owner.Key()), AuditIncomplete: flushErr != nil || service.Audit.HasLegacyRecords() || service.Audit.SinkStatus().Incomplete || service.Audit.SinkStatus().State == "degraded"})
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
				recordResult("policy_update_failed")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: policyErr.Error()})
			} else {
				recordResult("policy_updated")
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true})
			}
			endRequest()
			continue
		}
		lane := broker.LaneForOperation(req.Operation)
		if req.Wire != nil {
			dispatch := func(ctx context.Context) (*proto.Response, error) {
				response, err := service.DispatchScheduled(ctx, req.Host, req.Owner.Key(), lane, func(dispatchCtx context.Context) (*proto.Response, error) {
					if approvedTarget != "" {
						return service.DispatchApproved(dispatchCtx, req.Host, req.Wire, approvedTarget)
					}
					return service.Dispatch(dispatchCtx, req.Host, req.Wire)
				})
				if err != nil {
					return response, err
				}
				if err := service.RecordJobResponse(req.Host, req.Owner.Key(), req.Wire, response); err != nil {
					return nil, errors.New("remote job response could not be validated or its history persisted")
				}
				return response, nil
			}
			var wireResp *proto.Response
			var mutation *broker.MutationIntent
			var err error
			if req.Wire.Op == proto.OpJobWait && req.Wire.Job != nil {
				jobKey, _ := json.Marshal([]any{req.Owner, req.Host, req.Wire.Job, req.Wire.DeadlineUnixMilli})
				jobDigest := sha256.Sum256(jobKey)
				wireResp, err = service.DispatchSharedIngress(requestCtx, req.Owner.Key(), hex.EncodeToString(jobDigest[:]), item.bytes, dispatch)
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
				recordResult(result)
				_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: err.Error(), Mutation: mutation})
				endRequest()
				continue
			}
			recordResult("completed")
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Wire: wireResp, Mutation: mutation})
			endRequest()
			continue
		}
		if req.Operation != "status" {
			_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, Error: "unsupported broker operation"})
			endRequest()
			continue
		}
		var ingress *broker.IngressSnapshot
		var scheduler *broker.SchedulerSnapshot
		var sharedWaits *broker.SharedWaitStatus
		if req.Operation == "status" {
			in := service.Ingress.Snapshot(req.Owner.Key())
			ingress = &in
			snapshot := service.Scheduler.Snapshot(req.Owner.Key())
			scheduler = &snapshot
			waits := service.SharedWaitStatus(req.Owner.Key())
			sharedWaits = &waits
		}
		recordResult("accepted")
		_ = respond(broker.Response{ID: req.ID, PolicyDigest: decision.Digest, OK: true, Scheduler: scheduler, SharedWaits: sharedWaits, Ingress: ingress})
		endRequest()
	}
}
