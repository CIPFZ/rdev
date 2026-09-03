package transport

import "testing"

func TestCanonicalConnectionKeyAliasesAndIdentity(t *testing.T) {
	base := Host{Name: "one", Addr: "User@Example.COM", RemoteDir: "~/.cache/rdev", Port: 0}
	other := base
	other.Name = "two"
	a, err := CanonicalConnectionKey(base)
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalConnectionKey(other)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatal("alias changed connection key")
	}
	for name, mutate := range map[string]func(*Host){"user": func(h *Host) { h.Addr = "other@Example.COM" }, "port": func(h *Host) { h.Port = 2200 }, "state": func(h *Host) { h.RemoteDir = ".cache/other" }, "identity": func(h *Host) { h.IdentityFile = "~/.ssh/id_ed25519" }, "proxy": func(h *Host) { h.ProxyJump = "jump.example" }, "policy": func(h *Host) { h.HostKeyPolicy = "yes" }} {
		h := base
		mutate(&h)
		k, e := CanonicalConnectionKey(h)
		if e != nil {
			t.Fatalf("%s: %v", name, e)
		}
		if k == a {
			t.Fatalf("%s did not change key", name)
		}
	}
	forced := base
	forced.ForceAgentUpload = true
	k, err := CanonicalConnectionKey(forced)
	if err != nil || k != a {
		t.Fatalf("force upload changed key: %v", err)
	}
}
