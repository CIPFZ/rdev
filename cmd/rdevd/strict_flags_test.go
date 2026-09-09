package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSingleFlagsRejectAmbiguousArguments(t *testing.T) {
	for _, args := range [][]string{
		{"-unknown", "value"},
		{"-path"},
		{"-path", "--"},
		{"-path", "-enabled"},
		{"-path", "one", "--path=two"},
		{"-enabled", "--enabled=false"},
		{"-enabled=true", "-enabled=true"},
		{"-ttl", "nonsense"},
		{"-ttl", "999999999999999999999999h"},
		{"-ttl", "1h", "-ttl", "2h"},
		{"operand"},
		{"--", "-operand"},
		{"---path", "value"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			fs.String("path", "", "")
			fs.Bool("enabled", false, "")
			fs.Duration("ttl", time.Hour, "")
			if err := parseSingleFlags(fs, args); err == nil {
				t.Fatal("accepted invalid arguments")
			}
		})
	}
}

func TestSingleFlagsPreserveStandardSyntax(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	path := fs.String("path", "", "")
	enabled := fs.Bool("enabled", true, "")
	ttl := fs.Duration("ttl", time.Hour, "")
	if err := parseSingleFlags(fs, []string{"--path=-雪 path", "-enabled=false", "--ttl", "-1s", "--"}); err != nil {
		t.Fatal(err)
	}
	if *path != "-雪 path" || *enabled || *ttl != -time.Second || fs.NArg() != 0 {
		t.Fatalf("parsed values path=%q enabled=%t ttl=%s", *path, *enabled, *ttl)
	}
	fs = flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	if err := parseSingleFlags(fs, []string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help error = %v", err)
	}
}

func TestDaemonFlagErrorsDoNotTouchSocketOrReadiness(t *testing.T) {
	t.Setenv("RDEV_PRINCIPAL_SECRET", "")
	root := t.TempDir()
	socket := filepath.Join(root, "new", "broker.sock")
	ready := filepath.Join(root, "ready")
	if err := os.WriteFile(ready, []byte("existing-ready"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tail := range [][]string{
		{"-socket", socket},
		{"-allow-unauthenticated", "-allow-unauthenticated=false"},
		{"-allow-unauthenticated", "-principal-key-file", filepath.Join(root, "missing-key")},
		{"-config", "-allow-unauthenticated"},
		{"extra"},
	} {
		args := append([]string{"-socket", socket, "-ready-file", ready}, tail...)
		if err := runDaemon(args); err == nil {
			t.Fatalf("accepted %q", args)
		}
		if _, err := os.Stat(filepath.Dir(socket)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid arguments touched socket directory: %v", err)
		}
		got, err := os.ReadFile(ready)
		if err != nil || string(got) != "existing-ready" {
			t.Fatalf("invalid arguments touched readiness: %q, %v", got, err)
		}
	}
}

func TestProvisioningFlagsRejectBeforeWriting(t *testing.T) {
	t.Setenv("RDEV_PRINCIPAL_SECRET", strings.Repeat("test-only-key-", 4))
	first, second := filepath.Join(t.TempDir(), "one"), filepath.Join(t.TempDir(), "two")
	if err := principalKeygenCommand([]string{"-out", first, "--out=" + second}); err == nil {
		t.Fatal("accepted duplicate output path")
	}
	for _, path := range []string{first, second} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid keygen wrote %s: %v", path, err)
		}
	}
	for _, tail := range [][]string{
		{"-client-id", "other"},
		{"-ttl", "0"},
		{"-ttl", "-1s"},
		{"-ttl", "25h"},
		{"-ttl", "overflow"},
		{"-ttl", "99999999999999999999h"},
		{"-ttl", "1h", "--ttl=2h"},
		{"extra"},
	} {
		var output bytes.Buffer
		args := append([]string{"-client-id", "one", "-project-id", "project"}, tail...)
		if err := principalTokenCommand(args, &output); err == nil || output.Len() != 0 {
			t.Fatalf("invalid token arguments %q returned %v, output length=%d", args, err, output.Len())
		}
	}
	var output bytes.Buffer
	if err := principalTokenCommand([]string{"--client-id=one", "-project-id", "project", "-ttl=1h"}, &output); err != nil || output.Len() == 0 {
		t.Fatalf("legacy valid provisioning arguments rejected: %v", err)
	}
	output.Reset()
	if err := approvalCreateCommand([]string{"-socket", first, "--socket=" + second, "-request-file", first}, &output); err == nil || !strings.Contains(err.Error(), "only be supplied once") || output.Len() != 0 {
		t.Fatalf("duplicate approval destination was not rejected before request read: %v", err)
	}
}
