package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func main() {
	defaultSocket := filepath.Join(os.TempDir(), "rdev", "rdevd.sock")
	if home, err := os.UserHomeDir(); err == nil {
		defaultSocket = filepath.Join(home, ".cache", "rdev", "rdevd.sock")
	}
	socket := flag.String("socket", defaultSocket, "Unix socket path")
	defaultAgents := filepath.Join(os.Getenv("HOME"), ".local", "share", "rdev", "agents")
	if v := os.Getenv("RDEV_AGENT_DIR"); v != "" {
		defaultAgents = v
	}
	agentDir := flag.String("agent-dir", defaultAgents, "directory containing rdev-agent-<os>-<arch> binaries")
	configPath := flag.String("config", defaultSocket+".json", "broker JSON config path")
	flag.Parse()
	ln, err := broker.Listen(*socket)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	service := broker.NewService(agentLookup(*agentDir))
	service.SetReady(false)
	loadConfig := func() {
		data, err := os.ReadFile(*configPath)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			log.Printf("rdevd: config read failed: %v", err)
			return
		}
		var cfg broker.Config
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Printf("rdevd: config parse failed: %v", err)
			return
		}
		if err := service.ReloadConfig(cfg); err != nil {
			log.Printf("rdevd: config rejected: %v", err)
		}
	}
	loadConfig()
	policyPath := *socket + ".policy"
	if err := service.LoadPolicy(policyPath); err != nil && !os.IsNotExist(err) {
		log.Printf("rdevd: policy load failed: %v", err)
	}
	defer func() {
		if err := service.SavePolicy(policyPath); err != nil {
			log.Printf("rdevd: policy save failed: %v", err)
		}
	}()
	if err := service.Audit.ConfigureFile(*socket+".audit", 8<<20); err != nil {
		log.Printf("rdevd: warning: audit persistence disabled: %v", err)
	}
	if err := service.Client().Hosts.Load(); err != nil {
		log.Printf("rdevd: warning: host registry not loaded: %v", err)
	}
	jobsPath := *socket + ".jobs"
	_ = service.Jobs.Load(jobsPath)
	defer service.Jobs.Save(jobsPath)
	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), 30*time.Second)
	service.RecoverJobs(recoveryCtx)
	cancelRecovery()
	service.SetReady(true)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.Close(shutdownCtx); err != nil {
			log.Printf("rdevd: shutdown: %v", err)
		}
	}()
	var ready broker.Readiness
	ready.SetReady(true)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for {
			select {
			case <-hup:
				loadConfig()
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				if service.ReapIdle(now) {
					log.Printf("rdevd: reaped idle broker connections")
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		go serveConn(conn, service)
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
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&hello); err != nil {
		return
	}
	local := proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}
	resp := proto.BrokerHelloResponse{Version: local.Version, MinVersion: local.MinVersion}
	if err := proto.ValidateBrokerHello(local, hello); err != nil {
		resp.Error = err.Error()
		_ = json.NewEncoder(conn).Encode(resp)
		return
	} else {
		resp.OK = true
	}
	_ = json.NewEncoder(conn).Encode(resp)
	if !service.AttachClient() {
		return
	}
	defer service.DetachClient()
	dec := json.NewDecoder(bufio.NewReader(conn))
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
		if err := req.Owner.Validate(); err != nil {
			_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
			endRequest()
			continue
		}
		if req.Risk {
			if err := service.ConsumeApproval(req.Approval, req.Owner.Key(), req.Operation, req.Target); err != nil {
				service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "approval_denied", Result: err.Error()})
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
			_ = enc.Encode(broker.Response{ID: req.ID, OK: true, Audit: service.Audit.QueryOwner(req.Since, req.Owner.Key())})
			endRequest()
			continue
		}
		quotaHost := req.Host
		if err := service.Quota.AcquireHost(connCtx, quotaHost, req.Owner.Key()); err != nil {
			service.Audit.Append(broker.AuditEvent{Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "quota_rejected"})
			_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
			endRequest()
			continue
		}
		lane := broker.LaneControl
		switch req.Operation {
		case "exec", "job.start", "job.stop", "job.wait":
			lane = broker.LaneExec
		case "sync.push", "sync.pull", "write":
			lane = broker.LaneBulk
		}
		if !service.Lanes.Acquire(lane) {
			service.Quota.ReleaseHost(quotaHost, req.Owner.Key())
			_ = enc.Encode(broker.Response{ID: req.ID, Error: "control lane unavailable"})
			endRequest()
			continue
		}
		if req.Wire != nil {
			dispatch := func() (*proto.Response, error) {
				return service.DispatchFair(connCtx, req.Owner.Key(), lane, func() (*proto.Response, error) { return service.Dispatch(connCtx, req.Host, req.Wire) })
			}
			var wireResp *proto.Response
			var err error
			if req.Wire.Op == proto.OpJobWait && req.Wire.Job != nil {
				jobKey, _ := json.Marshal(req.Wire.Job)
				wireResp, err = service.DispatchShared(connCtx, req.Host+":"+string(jobKey), dispatch)
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
			if req.Wire.Op == proto.OpJobStart && wireResp.Job != nil && wireResp.Job.Info != nil {
				service.Jobs.Put(broker.JobRef{ID: wireResp.Job.Info.ID, Owner: req.Owner.Key(), Host: req.Host})
			}
			if req.Wire.Op == proto.OpJobRm && req.Wire.Job != nil {
				if req.Wire.Job.ID != "" {
					service.Jobs.Remove(req.Wire.Job.ID)
				}
				for _, id := range req.Wire.Job.IDs {
					service.Jobs.Remove(id)
				}
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
