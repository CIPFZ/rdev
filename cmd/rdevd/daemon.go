package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
)

func parseConfig(path string, allowMissing bool) (broker.Config, error) {
	cfg := broker.Config{MaxHosts: 128, IdleTTL: 5 * time.Minute}
	data, err := broker.ReadPrivateFile(path, 1<<20)
	if allowMissing && os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
		return cfg, fmt.Errorf("config requires a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return cfg, fmt.Errorf("config requires exactly one JSON object")
	}
	return cfg, cfg.Validate()
}

func runDaemon(args []string) error {
	defaultSocket := filepath.Join(os.TempDir(), "rdev", "rdevd.sock")
	if home, err := os.UserHomeDir(); err == nil {
		defaultSocket = filepath.Join(home, ".cache", "rdev", "rdevd.sock")
	}
	flags := flag.NewFlagSet("rdevd", flag.ContinueOnError)
	socket := flags.String("socket", defaultSocket, "Unix socket path in a private 0700 directory")
	defaultAgents := os.Getenv("RDEV_AGENT_DIR")
	if defaultAgents == "" {
		defaultAgents = filepath.Join(os.Getenv("HOME"), ".local", "share", "rdev", "agents")
	}
	agentDir := flags.String("agent-dir", defaultAgents, "directory containing rdev-agent-<os>-<arch> binaries")
	configPath := flags.String("config", "", "broker JSON config path (default: <socket>.json)")
	readyFile := flags.String("ready-file", "", "optional readiness file")
	keyFile := flags.String("principal-key-file", "", "0600 administrator signing key; reloaded on SIGHUP")
	hostsFile := flags.String("hosts-file", "", "private administrator host registry (replaces default global/project discovery)")
	unauthenticated := flags.Bool("allow-unauthenticated", false, "explicit single-user compatibility mode; declared owners are not authenticated")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	implicitConfig := *configPath == ""
	if implicitConfig {
		*configPath = *socket + ".json"
	}
	ln, err := broker.Listen(*socket)
	if err != nil {
		return err
	}
	// Keep the single-instance lock until drain and persistence both finish.
	defer ln.Close()
	if *readyFile != "" {
		if err := os.Remove(*readyFile); err != nil && !os.IsNotExist(err) {
			return err
		}
		defer os.Remove(*readyFile)
	}
	secret, err := principalSecret(*keyFile)
	if err != nil && !(*unauthenticated && *keyFile == "" && os.Getenv("RDEV_PRINCIPAL_SECRET") == "") {
		return err
	}
	if *unauthenticated && secret != "" {
		return fmt.Errorf("cannot combine credentials and unauthenticated mode")
	}
	cfg, err := parseConfig(*configPath, implicitConfig)
	if err != nil {
		return fmt.Errorf("config rejected: %w", err)
	}
	_, configStatErr := os.Stat(*configPath)
	configEverPresent := configStatErr == nil
	service := broker.NewService(agentLookup(*agentDir))
	service.SetReady(false)
	policyPath, jobsPath := *socket+".policy", *socket+".jobs"
	policyLoaded, jobsLoaded := false, false
	defer func() {
		service.SetReady(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.Close(shutdownCtx); err != nil {
			log.Printf("rdevd: shutdown: %v", err)
		}
		if err := func() error {
			if policyLoaded {
				return service.SavePolicy(policyPath)
			}
			return nil
		}(); err != nil {
			log.Printf("rdevd: policy save failed: %v", err)
		}
		if err := func() error {
			if jobsLoaded {
				return service.Jobs.Save(jobsPath)
			}
			return nil
		}(); err != nil {
			log.Printf("rdevd: job save failed: %v", err)
		}
		if err := service.Audit.Close(); err != nil {
			log.Printf("rdevd: audit close failed: %v", err)
		}
	}()
	if secret != "" {
		if err := service.Principals.Rotate(secret); err != nil {
			return err
		}
	}
	if err := service.ReloadConfig(cfg); err != nil {
		return err
	}
	if err := service.LoadPolicy(policyPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("policy load failed: %w", err)
	}
	policyLoaded = true
	if err := service.Audit.ConfigureFile(*socket+".audit", 8<<20); err != nil {
		return fmt.Errorf("audit initialization failed: %w", err)
	}
	if *hostsFile != "" {
		data, err := broker.ReadPrivateFile(*hostsFile, 4<<20)
		if err == nil {
			err = service.Client().Hosts.LoadGlobalData(*hostsFile, data)
		}
		if err != nil {
			return fmt.Errorf("host registry load failed: %w", err)
		}
	} else if err := service.Client().Hosts.Load(); err != nil {
		return fmt.Errorf("host registry load failed: %w", err)
	}
	if err := service.Jobs.ConfigurePersistence(jobsPath); err != nil {
		return fmt.Errorf("job registry load failed: %w", err)
	}
	jobsLoaded = true
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	recoveryCtx, cancelRecovery := context.WithTimeout(ctx, 30*time.Second)
	service.RecoverJobs(recoveryCtx)
	cancelRecovery()
	if ctx.Err() != nil {
		return nil
	}
	service.SetReady(true)
	if *readyFile != "" {
		if err := writeReady(*readyFile); err != nil {
			return fmt.Errorf("readiness file: %w", err)
		}
	}
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	var workers sync.WaitGroup
	workers.Add(1)
	defer workers.Wait()
	defer stop()
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				service.SetReady(false)
				if *readyFile != "" {
					_ = os.Remove(*readyFile)
				}
				_ = ln.StopAccepting()
				return
			case <-hup:
				next, err := parseConfig(*configPath, implicitConfig && !configEverPresent)
				var nextSecret string
				if err == nil && secret != "" {
					nextSecret, err = principalSecret(*keyFile)
				}
				if err == nil {
					err = service.ReloadConfig(next)
				}
				if err == nil && nextSecret != "" {
					err = service.Principals.Rotate(nextSecret)
				}
				if err != nil {
					log.Printf("rdevd: reload rejected; previous configuration retained")
				} else {
					if _, err := os.Stat(*configPath); err == nil {
						configEverPresent = true
					}
					log.Printf("rdevd: configuration reloaded")
				}
			case now := <-ticker.C:
				if service.ReapIdle(now) {
					log.Printf("rdevd: reaped idle broker connections")
				}
			}
		}
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go serveConn(conn, service)
	}
}

func writeReady(path string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".rdev-ready-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.WriteString("READY\n"); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
