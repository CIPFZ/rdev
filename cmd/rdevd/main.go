package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/CIPFZ/rdev/internal/broker"
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
		_ = conn.Close()
	}
}
