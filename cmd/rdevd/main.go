package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func main() {
	defaultSocket := filepath.Join(os.TempDir(), "rdev", "rdevd.sock")
	if home, err := os.UserHomeDir(); err == nil {
		defaultSocket = filepath.Join(home, ".cache", "rdev", "rdevd.sock")
	}
	socket := flag.String("socket", defaultSocket, "Unix socket path")
	flag.Parse()
	ln, err := broker.Listen(*socket)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	service := broker.NewService(nil)
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

func serveConn(conn net.Conn, service *broker.Service) {
	defer conn.Close()
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
			req.Wire.ClientID = req.Owner.ClientID
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
		if !service.Lanes.Acquire(broker.LaneControl) {
			service.Quota.Release(req.Owner.Key())
			_ = enc.Encode(broker.Response{ID: req.ID, Error: "control lane unavailable"})
			endRequest()
			continue
		}
		service.Lanes.Release(broker.LaneControl)
		service.Quota.Release(req.Owner.Key())
		if req.Wire != nil {
			wireResp, err := service.Dispatch(context.Background(), req.Host, req.Wire)
			if err != nil {
				service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "dispatch_error"})
				_ = enc.Encode(broker.Response{ID: req.ID, Error: err.Error()})
				endRequest()
				continue
			}
			service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "completed"})
			_ = enc.Encode(broker.Response{ID: req.ID, OK: true, Wire: wireResp})
			endRequest()
			continue
		}
		service.Audit.Append(broker.AuditEvent{At: time.Now(), Owner: req.Owner.Key(), Operation: req.Operation, Decision: "allow", Result: "accepted"})
		_ = enc.Encode(broker.Response{ID: req.ID, OK: true})
		endRequest()
	}
}
