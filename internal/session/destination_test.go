package session

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/transport"
)

func TestIPv6RegistryPublicationAndIdentity(t *testing.T) {
	r := NewRegistry()
	if err := r.Add(transport.Host{Name: "v6", Addr: "alice@[2001:0db8::1]:2222"}); err != nil {
		t.Fatal(err)
	}
	a, err := r.Resolve("v6")
	if err != nil {
		t.Fatal(err)
	}
	if a.Host.Addr != "alice@2001:db8::1" || a.Host.Port != 2222 {
		t.Fatalf("registry=%+v", a.Host)
	}
	if _, err := r.ApplyHostUpdate(HostUpdate{Name: "v6", Host: &transport.Host{Addr: "alice@2001:db8::1", Port: 2222}}); err != nil {
		t.Fatal(err)
	}
	b, err := r.Resolve("v6")
	if err != nil {
		t.Fatal(err)
	}
	if a.Fingerprint != b.Fingerprint {
		t.Fatal("equivalent IPv6 spellings changed target identity")
	}
	if _, err := r.ApplyHostUpdate(HostUpdate{Name: "v6", Host: &transport.Host{Addr: "bob@2001:db8::1", Port: 2222}}); err != nil {
		t.Fatal(err)
	}
	c, err := r.Resolve("v6")
	if err != nil {
		t.Fatal(err)
	}
	if b.Fingerprint == c.Fingerprint {
		t.Fatal("different SSH users shared identity")
	}
	for _, input := range []string{"127.0.0.1", "host.example", "::1", "[::1]:2222", "user@[::1]:2222"} {
		if _, err := r.Resolve(input); err != nil {
			t.Errorf("ad-hoc %q: %v", input, err)
		}
	}
}

func TestIPv6HostConfigCanonicalizesAndRejectsPortConflict(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "hosts.json")
	data := []byte(`{"hosts":[{"name":"v6","addr":"alice@[::1]:2222"}]}`)
	if err := os.WriteFile(filename, data, 0o600); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry()
	candidates, err := r.parseCandidates(filename, data)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].host.Addr != "alice@::1" || candidates[0].host.Port != 2222 || candidates[0].entry.Port != 2222 {
		t.Fatalf("candidates=%+v", candidates)
	}
	bad := []byte(`{"hosts":[{"name":"v6","addr":"alice@[::1]:2222","port":2222}]}`)
	if _, err := r.parseCandidates(filename, bad); err == nil {
		t.Fatal("conflicting embedded and separate ports accepted")
	}
}
