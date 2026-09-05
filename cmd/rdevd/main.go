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
	flag.Parse()
	ln, err := broker.Listen(*socket)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	service := broker.NewService(agentLookup(*agentDir))
	service.SetReady(false)
	if err := service.Client().Hosts.Load(); err != nil {
		log.Printf("rdevd: warning: host registry not loaded: %v", err)
	}
	service.SetReady(true)
	defer service.Close(context.Background())
	jobs := broker.NewJobRegistry()
	_ = jobs.Load(*socket + ".jobs")
	defer jobs.Save(*socket + ".jobs")
	var ready broker.Readiness
	ready.SetReady(true)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
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
	for {
		var req broker.Request
		if err := dec.Decode(&req); err != nil {
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
		}
		decision := service.Decide(req.Owner, req.Operation)
		if !decision.Allow {
			service.Audit.Append(broker.AuditEvent{Owner: req.Owner.Key(), Operation: req.Operation, Decision: decision.Reason, Result: "denied"})
			_ = enc.Encode(broker.Response{ID: req.ID, Error: decision.Reason})
			endRequest()
			continue
		}
		if err := service.Quota.Acquire(context.Background(), req.Owner.Key()); err != nil {
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
			service.Quota.Release(req.Owner.Key())
			_ = enc.Encode(broker.Response{ID: req.ID, Error: "control lane unavailable"})
			endRequest()
			continue
		}
		if req.Wire != nil {
			wireResp, err := service.Dispatch(context.Background(), req.Host, req.Wire)
			if err != nil {
				service.Lanes.Release(lane)
				service.Quota.Release(req.Owner.Key())
				service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "dispatch_error"})
				_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
				endRequest()
				continue
			}
			service.Lanes.Release(lane)
			service.Quota.Release(req.Owner.Key())
			service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "completed"})
			_ = enc.Encode(broker.Response{ID: req.ID, OK: true, Wire: wireResp})
			endRequest()
			continue
		}
		service.Lanes.Release(lane)
		service.Quota.Release(req.Owner.Key())
		service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "accepted"})
		_ = enc.Encode(broker.Response{ID: req.ID, OK: true})
		endRequest()
	}
}
