package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPolicyDefaultDeny(t *testing.T) {
	p := NewPolicy()
	if p.Decide("c", "exec").Allow {
		t.Fatal("default must deny")
	}
	p.Grant("c", "exec")
	if !p.Decide("c", "exec").Allow {
		t.Fatal("grant ignored")
	}
}

func TestPolicyPersistsGrantsAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	a := NewPolicy()
	a.Grant("client/project", "exec")
	if err := a.Save(path); err != nil {
		t.Fatal(err)
	}
	b := NewPolicy()
	if err := b.Load(path); err != nil {
		t.Fatal(err)
	}
	if !b.Decide("client/project", "exec").Allow {
		t.Fatal("grant not restored")
	}
}

func TestPolicyCapabilityDecision(t *testing.T) {
	p := NewPolicy()
	p.GrantCapability("c", "operator", "exec")
	if !p.DecideCapability("c", "operator", "exec").Allow {
		t.Fatal("capability grant ignored")
	}
	if p.DecideCapability("c", "operator", "delete").Allow {
		t.Fatal("capability broadened operation")
	}
}

func TestPolicyScopeAndImmutableDecision(t *testing.T) {
	p := NewPolicy()
	if err := p.GrantHost("alice", "host-a", "exec", "exec"); err != nil {
		t.Fatal(err)
	}
	before := p.DecideRequest("alice", "exec", "host-a")
	if !before.Allow || len(before.Digest) != 64 {
		t.Fatal("scoped grant missing")
	}
	for _, query := range [][3]string{{"alice", "exec", "host-b"}, {"bob", "exec", "host-a"}, {"alice", "read_file", "host-a"}} {
		if p.DecideRequest(query[0], query[1], query[2]).Allow {
			t.Fatalf("scope broadened: %v", query)
		}
	}
	if err := p.GrantCapability("confused", "file.read", "exec"); err != nil {
		t.Fatal(err)
	}
	if p.DecideRequest("confused", "exec", "host-a").Allow {
		t.Fatal("unrelated capability selected for exec")
	}
	if err := p.RevokeHost("alice", "host-a", "exec", "exec"); err != nil {
		t.Fatal(err)
	}
	after := p.DecideRequest("alice", "exec", "host-a")
	if after.Allow || before.Digest == after.Digest || !before.Allow {
		t.Fatal("decision snapshot changed or revoke failed")
	}
}

func TestPolicyDurableMutationRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy")
	p := NewPolicy()
	if err := p.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	if err := p.GrantHost("a", "h", "exec", "exec"); err != nil {
		t.Fatal(err)
	}
	disk := NewPolicy()
	if err := disk.Load(path); err != nil {
		t.Fatal(err)
	}
	if disk.DecideRequest("a", "exec", "h") != p.DecideRequest("a", "exec", "h") {
		t.Fatal("acknowledged mutation not durable")
	}
	previous := p.DecideRequest("a", "exec", "h")
	if err := os.Rename(path, path+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.RevokeHost("a", "h", "exec", "exec"); err == nil {
		t.Fatal("persistence failure reported success")
	}
	if p.DecideRequest("a", "exec", "h") != previous {
		t.Fatal("failed mutation published a new policy")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".saved", path); err != nil {
		t.Fatal(err)
	}
	if err := p.RevokeHost("a", "h", "exec", "exec"); err != nil {
		t.Fatal(err)
	}
	if err := disk.Load(path); err != nil {
		t.Fatal(err)
	}
	if disk.DecideRequest("a", "exec", "h").Allow {
		t.Fatal("revoke not durable")
	}
}

func TestPolicyRejectsAmbiguousOrUnsafePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy")
	p := NewPolicy()
	if err := p.Grant("a", "exec"); err != nil {
		t.Fatal(err)
	}
	before := p.Decide("a", "exec")
	for _, data := range []string{"null", "[]", `{"a":null}`, `{"a":{}}`, `{"a":{"exec":true,"exec":false}}`, `{"a":{"exec":true},"a":{"exec":false}}`, `{"a":{"exec":null}}`, `{} {}`, strings.Repeat(" ", 4<<20) + "{}"} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if err := p.Load(path); err == nil {
			t.Fatalf("accepted ambiguous policy (bytes=%d)", len(data))
		}
		if p.Decide("a", "exec") != before {
			t.Fatal("invalid load replaced active policy")
		}
	}
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if err := p.Load(path); err == nil {
		t.Fatal("accepted public policy")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, path+".link"); err != nil {
		t.Fatal(err)
	}
	if err := p.Load(path + ".link"); err == nil {
		t.Fatal("accepted symlink policy")
	}
}

func TestPolicyUncertainCommitFailsClosedAndPreservesDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy")
	p := NewPolicy()
	if err := p.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	if err := p.Grant("a", "exec"); err != nil {
		t.Fatal(err)
	}
	// Inject an error after an actual atomic replacement: the kernel may have
	// committed the revoke even though directory durability cannot be confirmed.
	p.write = func(path string, grants map[string]map[string]bool) error {
		if err := savePolicy(path, grants); err != nil {
			return err
		}
		return &policyCommitUncertain{cause: os.ErrPermission}
	}
	if err := p.Revoke("a", "exec"); err == nil {
		t.Fatal("uncertain commit acknowledged")
	}
	if p.Decide("a", "exec").Allow {
		t.Fatal("uncertain revoke left old grant active")
	}
	if err := p.Save(path); err == nil {
		t.Fatal("shutdown overwrote uncertain disk state")
	}
	if err := p.Grant("b", "exec"); err == nil {
		t.Fatal("mutation accepted before recovery")
	}
	recovered := NewPolicy()
	if err := recovered.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	if recovered.Decide("a", "exec").Allow {
		t.Fatal("durably replaced revoke lost at recovery")
	}
}
