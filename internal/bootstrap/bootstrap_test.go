package bootstrap

import "testing"

func TestKnownHostsCallbackRequiresExistingFile(t *testing.T) {
	if _, err := KnownHostsCallback("/definitely/missing/rdev-known-hosts"); err == nil {
		t.Fatal("missing known_hosts unexpectedly accepted")
	}
}

func TestRunRejectsMissingHostKeyCallback(t *testing.T) {
	err := Run(t.Context(), Config{Address: "127.0.0.1:2222", User: "root", Password: "secret", PublicKey: []byte("ssh-ed25519 AAAA"), PrivateKey: []byte("bad")})
	if err == nil {
		t.Fatal("bootstrap without host-key callback unexpectedly proceeded")
	}
}
