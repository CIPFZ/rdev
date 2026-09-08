package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func main() {
	var err error
	switch {
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
		// Detached job observation must outlive the local frontend connection:
		// a watcher closing its socket cannot cancel the shared remote wait used
		// by other watchers (or leave the job without an observer).
		requestCtx := connCtx
		if req.Wire != nil && req.Wire.Op == proto.OpJobWait {
			requestCtx = context.Background()
		}
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
		if req.Risk {
			if err := service.ConsumeApproval(req.Approval, req.Owner.Key(), req.Operation, req.Target); err != nil {
				service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "approval_denied", Result: "approval_invalid"})
				_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
				endRequest()
				continue
			}
		}
		if req.Wire != nil {
			if req.Host == "" {
				_ = enc.Encode(broker.Response{ID: req.ID, Error: "host required for wire request"})
				endRequest()
				continue
			}
			if req.Operation != "" && req.Wire.Op != req.Operation {
				_ = enc.Encode(broker.Response{ID: req.ID, Error: "operation mismatch"})
				endRequest()
				continue
			}
			if (req.Wire.ClientID != "" && req.Wire.ClientID != req.Owner.ClientID) || (req.Wire.ProjectID != "" && req.Wire.ProjectID != req.Owner.ProjectID) {
				_ = enc.Encode(broker.Response{ID: req.ID, Error: "wire owner mismatch"})
				endRequest()
				continue
			}
			req.Wire.ClientID = req.Owner.ClientID
			req.Wire.ProjectID = req.Owner.ProjectID
			if req.Wire.Job != nil {
				if err := service.Jobs.ValidateRequest(req.Host, req.Owner.Key(), req.Wire); err != nil {
					_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
					endRequest()
					continue
				}
				ids := append([]string{}, req.Wire.Job.IDs...)
				if req.Wire.Job.ID != "" {
					ids = append(ids, req.Wire.Job.ID)
				}
				ownerMismatch := false
				for _, id := range ids {
					if ref, ok := service.Jobs.Get(id); ok && ref.Owner != req.Owner.Key() {
						ownerMismatch = true
						break
					}
				}
				if ownerMismatch {
					_ = enc.Encode(broker.Response{ID: req.ID, Error: "job owner mismatch"})
					endRequest()
					continue
				}
			}
		}
		decision := service.Decide(req.Owner, req.Operation)
		if req.Capability != "" {
			decision = service.PolicyDecisionForCapability(req.Owner, req.Capability, req.Operation)
		}
		if !decision.Allow {
			service.Audit.Append(broker.AuditEvent{Owner: req.Owner.Key(), Operation: req.Operation, Decision: decision.Reason, Result: "denied"})
			_ = enc.Encode(broker.Response{ID: req.ID, Error: decision.Reason})
			endRequest()
			continue
		}
		if req.Operation == "audit_query" {
			_ = enc.Encode(broker.Response{ID: req.ID, OK: true, Audit: service.Audit.QueryOwner(req.Since, req.Owner.Key()), AuditIncomplete: service.Audit.HasLegacyRecords()})
			endRequest()
			continue
		}
		if req.Operation == "policy.grant" {
			var policyErr error
			if req.GrantCapability != "" {
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
				_ = enc.Encode(broker.Response{ID: req.ID, Error: policyErr.Error()})
			} else {
				service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "policy_updated"})
				_ = enc.Encode(broker.Response{ID: req.ID, OK: true})
			}
			endRequest()
			continue
		}
		quotaHost := req.Host
		if err := service.Quota.AcquireHostContext(requestCtx, quotaHost, req.Owner.Key()); err != nil {
			service.Audit.Append(broker.AuditEvent{Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "quota_rejected"})
			_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
			endRequest()
			continue
		}
		lane := broker.LaneControl
		switch req.Operation {
		case "exec", "job_start", "job_stop", "job_wait":
			lane = broker.LaneExec
		case "sync.push", "sync.pull", "write", "write_file":
			lane = broker.LaneBulk
		}
		if err := service.Lanes.AcquireContext(requestCtx, lane); err != nil {
			service.Quota.ReleaseHost(quotaHost, req.Owner.Key())
			_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
			endRequest()
			continue
		}
		if req.Wire != nil {
			dispatch := func() (*proto.Response, error) {
				return service.DispatchFair(requestCtx, req.Owner.Key(), lane, func() (*proto.Response, error) { return service.Dispatch(requestCtx, req.Host, req.Wire) })
			}
			var wireResp *proto.Response
			var err error
			if req.Wire.Op == proto.OpJobWait && req.Wire.Job != nil {
				jobKey, _ := json.Marshal(req.Wire.Job)
				wireResp, err = service.DispatchShared(requestCtx, req.Host+":"+string(jobKey), dispatch)
			} else {
				wireResp, err = dispatch()
			}
			if err != nil {
				service.Lanes.Release(lane)
				service.Quota.ReleaseHost(quotaHost, req.Owner.Key())
				service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "dispatch_error"})
				_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
				endRequest()
				continue
			}
			service.Lanes.Release(lane)
			service.Quota.ReleaseHost(quotaHost, req.Owner.Key())
			service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "completed"})
			if err := service.Jobs.RecordResponse(req.Host, req.Owner.Key(), req.Wire, wireResp); err != nil {
				// Remote mutation completed but durable ownership state did not.
				// Return an ambiguous outcome and never replay the mutation.
				_ = enc.Encode(broker.Response{ID: req.ID, Error: "mutation completed but broker state was not persisted; query remote status"})
				endRequest()
				continue
			}
			_ = enc.Encode(broker.Response{ID: req.ID, OK: true, Wire: wireResp})
			endRequest()
			continue
		}
		service.Lanes.Release(lane)
		service.Quota.ReleaseHost(quotaHost, req.Owner.Key())
		service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "accepted"})
		_ = enc.Encode(broker.Response{ID: req.ID, OK: true})
		endRequest()
	}
}
