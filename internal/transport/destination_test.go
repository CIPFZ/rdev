package transport

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDestinationGrammar(t *testing.T) {
	for _, tt := range []struct {
		in   string
		port int
		addr string
		want int
	}{
		{"service-deploy", 0, "service-deploy", 0}, {"user@host.example", 22, "user@host.example", 22},
		{"127.0.0.1:2222", 0, "127.0.0.1", 2222}, {"2001:db8::22", 0, "2001:db8::22", 0},
		{"user@[2001:0db8::1]:2222", 0, "user@2001:db8::1", 2222}, {"[::1]", 22, "::1", 22},
		{"user@fe80::1%eth0", 0, "user@fe80::1%eth0", 0},
	} {
		t.Run(tt.in, func(t *testing.T) {
			addr, port, err := ParseDestination(tt.in, tt.port)
			if err != nil || addr != tt.addr || port != tt.want {
				t.Fatalf("got %q:%d %v, want %q:%d", addr, port, err, tt.addr, tt.want)
			}
		})
	}
	for _, tt := range []struct {
		in   string
		port int
	}{
		{"", 0}, {"-host", 0}, {"user@-host", 0}, {"@host", 0}, {"a@b@c", 0}, {"host name", 0}, {"host\nname", 0},
		{"host:0", 0}, {"host:-1", 0}, {"host:+1", 0}, {"host:65536", 0}, {"host:99999999999999999999999999999", 0}, {"host:", 0},
		{"[127.0.0.1]:22", 0}, {"[::1", 0}, {"[::1]x", 0}, {"[::1]:", 0}, {"::1:port", 0},
		{"host:22", 22}, {"[::1]:22", 23}, {"host", -1}, {"host", 65536}, {"host;touch", 0}, {"fe80::1%eth0;bad", 0},
	} {
		if _, _, err := ParseDestination(tt.in, tt.port); err == nil {
			t.Errorf("accepted %q with port %d", tt.in, tt.port)
		}
	}
}

func TestSSHProcessReceivesNormalizedDestination(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "args.json")
	// A real child process records the actual argv after the final SSH sink.
	if err := os.WriteFile(filepath.Join(dir, "ssh"), []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$RDEV_CAPTURE_ARGV\"\nprintf 'ok\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("RDEV_CAPTURE_ARGV", output)
	c := &Conn{host: Host{Name: "v6", Addr: "alice@[2001:db8::1]:2222"}, ctlPath: filepath.Join(dir, "ctl")}
	if _, err := c.runSSH(context.Background(), "true"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if !reflect.DeepEqual(args[len(args)-4:], []string{"-p", "2222", "alice@2001:db8::1", "true"}) {
		t.Fatalf("actual SSH argv=%q", args)
	}
	if strings.Contains(strings.Join(args, " "), "[2001") {
		t.Fatal("SSH received rsync-style brackets")
	}
	if c.host.Addr != "alice@[2001:db8::1]:2222" {
		t.Fatal("final sink mutated connection identity")
	}
}

func TestControlIdentityAndRsyncIPv6Grammar(t *testing.T) {
	a, err := controlPath(Host{Addr: "alice@[::1]:2222"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := controlPath(Host{Addr: "alice@::1", Port: 2222})
	if err != nil {
		t.Fatal(err)
	}
	c, err := controlPath(Host{Addr: "bob@::1", Port: 2222})
	if err != nil {
		t.Fatal(err)
	}
	d, err := controlPath(Host{Addr: "alice@::1", Port: 2223})
	if err != nil {
		t.Fatal(err)
	}
	if a != b || a == c || a == d {
		t.Fatalf("control identity aliases/users/ports: %q %q %q %q", a, b, c, d)
	}
	if got := RsyncDestination("alice@::1"); got != "alice@[::1]" {
		t.Fatalf("rsync destination=%q", got)
	}
}
