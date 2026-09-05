package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"os/signal"
	"syscall"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func main() {
	socket := flag.String("socket", "", "Unix socket path")
	flag.Parse()
	if *socket == "" {
		log.Fatal("-socket is required")
	}
	ln, err := broker.Listen(*socket)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
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
		go serveConn(conn)
	}
}

func serveConn(conn net.Conn) {
	defer conn.Close()
	var hello proto.BrokerHello
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&hello); err != nil {
		return
	}
	local := proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion}
	resp := proto.BrokerHelloResponse{Version: local.Version, MinVersion: local.MinVersion}
	if err := proto.ValidateBrokerHello(local, hello); err != nil {
		resp.Error = err.Error()
	} else {
		resp.OK = true
	}
	_ = json.NewEncoder(conn).Encode(resp)
}
