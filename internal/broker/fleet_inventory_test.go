package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
)

func fleetInventoryTestService(t *testing.T, names ...string) *Service {
	t.Helper()
	s := NewService(nil)
	t.Cleanup(func() { _ = s.Close(t.Context()) })
	for _, name := range names {
		if err := s.Client().Hosts.Add(transport.Host{Name: name, Addr: name + ".invalid"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ConfigureFleetInventory(filepath.Join(t.TempDir(), "inventory.json")); err != nil {
		t.Fatal(err)
	}
	return s
}

func fleetInventoryTestImport(t *testing.T, s *Service) FleetInventory {
	t.Helper()
	in, err := s.FleetInventoryImport(s.FleetInventorySnapshot().Revision)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func fleetInventoryGrant(t *testing.T, s *Service, owner Owner, id string) {
	t.Helper()
	for _, op := range []string{"fleet.plan", "job_start", "job_status"} {
		if err := s.GrantHost(owner, id, "", op, false); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFleetInventoryImportAliasesAndStableIDs(t *testing.T) {
	s := fleetInventoryTestService(t, "first")
	if err := s.Client().Hosts.Add(transport.Host{Name: "renamed", Addr: "first.invalid"}); err != nil {
		t.Fatal(err)
	}
	in := fleetInventoryTestImport(t, s)
	if len(in.Records) != 1 || !reflect.DeepEqual(in.Records[0].Aliases, []string{"first", "renamed"}) {
		t.Fatalf("aliases did not deduplicate: %+v", in)
	}
	id := in.Records[0].HostID
	if !validFleetHex(id, 32) {
		t.Fatalf("invalid generated HostID %q", id)
	}
	in.Records[0].Aliases = []string{"renamed"}
	in.Records[0].Labels = map[string]string{"owner": "untrusted-label", "env": "test"}
	next, err := s.FleetInventoryUpdate(in.Revision, in.Records)
	if err != nil {
		t.Fatal(err)
	}
	if next.Records[0].HostID != id || len(next.RetiredIDs) != 0 {
		t.Fatal("alias rename changed identity")
	}
	if err := s.FleetTargetCurrent(in.Records[0], "first"); !errors.Is(err, ErrFleetTargetChanged) {
		t.Fatalf("old alias remains usable: %v", err)
	}
	dispatch, err := s.FleetDispatchIdentity(next.Records[0], "renamed")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := s.Client().ProtocolTargetIdentity("renamed")
	if dispatch != want {
		t.Fatal("fleet dispatch digest differs from approved client identity")
	}
	// Returned metadata is a value; callers cannot mutate the authority.
	next.Records[0].Aliases[0] = "changed"
	next.Records[0].Labels["env"] = "changed"
	stored := s.FleetInventorySnapshot()
	if stored.Records[0].Aliases[0] != "renamed" || stored.Records[0].Labels["env"] != "test" {
		t.Fatal("snapshot aliases the authoritative store")
	}
	loaded := NewService(nil)
	t.Cleanup(func() { _ = loaded.Close(t.Context()) })
	if err := loaded.ConfigureFleetInventory(s.inventory.path); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, loaded.FleetInventorySnapshot()) {
		t.Fatal("inventory restart lost identity or metadata")
	}
}

func TestFleetInventoryRetiresReplacedConnections(t *testing.T) {
	s := fleetInventoryTestService(t, "first")
	in := fleetInventoryTestImport(t, s)
	old := in.Records[0]
	if err := s.Client().Hosts.Add(transport.Host{Name: "first", Addr: "replacement.invalid"}); err != nil {
		t.Fatal(err)
	}
	if err := s.FleetTargetCurrent(old, "first"); !errors.Is(err, ErrFleetTargetChanged) {
		t.Fatalf("old identity accepted a replacement: %v", err)
	}
	if _, err := s.FleetInventoryUpdate(in.Revision, in.Records); !errors.Is(err, ErrFleetTargetChanged) {
		t.Fatalf("connection change retained old ID: %v", err)
	}
	next := fleetInventoryTestImport(t, s)
	if len(next.Records) != 1 || next.Records[0].HostID == old.HostID || !reflect.DeepEqual(next.RetiredIDs, []string{old.HostID}) {
		t.Fatalf("replacement must receive a new ID and retire old ID: %+v", next)
	}
	// An alias reused by a replacement cannot inherit host-scoped authority.
	owner := Owner{ClientID: "alice", ProjectID: "a"}
	fleetInventoryGrant(t, s, owner, old.HostID)
	if _, err := s.ResolveFleetTargets(owner, "alias=first"); !errors.Is(err, ErrFleetTargetsNotFound) {
		t.Fatalf("host grant transferred on alias reuse: %v", err)
	}
	next.Records[0].HostID = old.HostID
	if _, err := s.FleetInventoryUpdate(next.Revision, next.Records); err == nil {
		t.Fatal("retired identity reused")
	}
	cleared, err := s.FleetInventoryUpdate(next.Revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	recreated := fleetInventoryTestImport(t, s)
	if recreated.Records[0].HostID == next.Records[0].HostID || len(recreated.RetiredIDs) != len(cleared.RetiredIDs) {
		t.Fatal("delete/recreate reused an identity")
	}
}

func TestFleetInventoryGlobalTrustAndCanonicalConfig(t *testing.T) {
	s := fleetInventoryTestService(t, "global")
	project := transport.Host{Name: "project", Addr: "private.invalid"}
	if _, err := s.Client().Hosts.ApplyHostUpdate(session.HostUpdate{Name: "project", Host: &project, SetScope: true, Scope: session.ScopeProject}); err != nil {
		t.Fatal(err)
	}
	in := fleetInventoryTestImport(t, s)
	if len(in.Records) != 1 || in.Records[0].Aliases[0] != "global" {
		t.Fatalf("project expanded inventory: %+v", in)
	}
	if _, err := s.FleetInventoryUpdate(in.Revision, []FleetHost{{Aliases: []string{"project"}}}); !errors.Is(err, ErrFleetTargetChanged) {
		t.Fatalf("project target accepted: %v", err)
	}
	before, _ := s.FleetTargetIdentity("global")
	s.Client().Hosts.Update("global", func(state *session.State) { state.Env = map[string]string{"SAFE": "changed"} })
	after, _ := s.FleetTargetIdentity("global")
	if before == after || s.FleetTargetCurrent(in.Records[0], "global") == nil {
		t.Fatal("session configuration drift did not invalidate the identity")
	}
	for _, h := range []transport.Host{
		{Name: "ip1", Addr: "user@[2001:db8::1]:22", RemoteDir: "~/.cache/rdev"},
		{Name: "ip2", Addr: "user@2001:db8::1", Port: 22, RemoteDir: ".cache/rdev"},
	} {
		if err := s.Client().Hosts.Add(h); err != nil {
			t.Fatal(err)
		}
	}
	one, _ := s.FleetTargetIdentity("ip1")
	two, _ := s.FleetTargetIdentity("ip2")
	if one != two {
		t.Fatal("equivalent IPv6/namespace configuration did not canonicalize")
	}
}

func TestFleetSelectorDeterminismAndDiscoveryIsolation(t *testing.T) {
	s := fleetInventoryTestService(t, "a", "b", "c")
	in := fleetInventoryTestImport(t, s)
	for i := range in.Records {
		in.Records[i].Labels = map[string]string{"env": "test", "owner": "alice"}
	}
	in, err := s.FleetInventoryUpdate(in.Revision, in.Records)
	if err != nil {
		t.Fatal(err)
	}
	owner := Owner{ClientID: "alice", ProjectID: "project"}
	for _, h := range in.Records[:2] {
		fleetInventoryGrant(t, s, owner, h.HostID)
	}
	for _, selector := range []string{"all", "label:owner=alice&env=test", "id=" + in.Records[1].HostID + "," + in.Records[0].HostID + "," + in.Records[1].HostID} {
		got, err := s.ResolveFleetTargets(owner, selector)
		if err != nil || !reflect.DeepEqual(got, in.Records[:2]) {
			t.Fatalf("%q: got %+v, %v", selector, got, err)
		}
	}
	for _, selector := range []string{"id=" + in.Records[2].HostID, "alias=" + in.Records[2].Aliases[0], "alias=missing", "alias=" + in.Records[0].Aliases[0] + ",missing", "label:env=missing"} {
		if _, err := s.ResolveFleetTargets(owner, selector); !errors.Is(err, ErrFleetTargetsNotFound) {
			t.Fatalf("missing/forbidden selection differs: %q: %v", selector, err)
		}
	}
	// Owner labels do not authorize a different project, missing inner grants,
	// or a host grant using the legacy alias instead of immutable HostID.
	for _, caller := range []Owner{{ClientID: "alice", ProjectID: "other"}, {ClientID: "bob", ProjectID: "project"}} {
		if _, err := s.ResolveFleetTargets(caller, "all"); !errors.Is(err, ErrFleetTargetsNotFound) {
			t.Fatal("owner/project isolation lost")
		}
	}
	aliasOwner := Owner{ClientID: "alias-owner", ProjectID: "project"}
	fleetInventoryGrant(t, s, aliasOwner, in.Records[0].Aliases[0])
	if _, err := s.ResolveFleetTargets(aliasOwner, "all"); !errors.Is(err, ErrFleetTargetsNotFound) {
		t.Fatal("legacy alias grants authorized an immutable Fleet host")
	}
	if err := s.GrantHost(owner, in.Records[0].HostID, "", "job_status", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveFleetTargets(owner, "id="+in.Records[0].HostID); !errors.Is(err, ErrFleetTargetsNotFound) {
		t.Fatal("Fleet capability bypassed specific operation permission")
	}
}

func TestFleetSelectorRejectsEmptyAndMalformed(t *testing.T) {
	s := fleetInventoryTestService(t, "a")
	fleetInventoryTestImport(t, s)
	for _, selector := range []string{"", " ", " all", "all ", "*", "id=", "alias=", "alias=a,", "label:", "label:a=", "label:a=b&a=c", "label:a=b&", "id=not-an-id", "all=1", strings.Repeat("a", fleetMaxSelectorBytes+1)} {
		if _, err := s.ResolveFleetTargets(Owner{ClientID: "a", ProjectID: "p"}, selector); err == nil || errors.Is(err, ErrFleetTargetsNotFound) {
			t.Fatalf("malformed selector was not rejected as syntax: %.100q: %v", selector, err)
		}
	}
}

func TestFleetInventoryCASAndConcurrentSnapshot(t *testing.T) {
	s := fleetInventoryTestService(t, "a", "b")
	in := fleetInventoryTestImport(t, s)
	owner := Owner{ClientID: "a", ProjectID: "p"}
	for _, h := range in.Records {
		fleetInventoryGrant(t, s, owner, h.HostID)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hosts := cloneFleetInventory(in).Records
			for j := range hosts {
				hosts[j].Labels["epoch"] = fmt.Sprint(i)
			}
			if _, err := s.FleetInventoryUpdate(in.Revision, hosts); err == nil {
				successes.Add(1)
			} else if !errors.Is(err, ErrFleetInventoryConflict) {
				t.Errorf("unexpected update error: %v", err)
			}
			got, err := s.ResolveFleetTargets(owner, "all")
			if err != nil || len(got) != 2 || got[0].Labels["epoch"] != got[1].Labels["epoch"] {
				t.Errorf("torn target snapshot: %+v %v", got, err)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 1 || s.FleetInventorySnapshot().Revision != in.Revision+1 {
		t.Fatal("CAS accepted conflicting updates")
	}
	// A previously selected value cannot expand after an administrator update.
	preview, _ := s.ResolveFleetTargets(owner, "all")
	if err := s.Client().Hosts.Add(transport.Host{Name: "c", Addr: "c.invalid"}); err != nil {
		t.Fatal(err)
	}
	fleetInventoryTestImport(t, s)
	if len(preview) != 2 {
		t.Fatal("previous snapshot expanded")
	}
}

func TestFleetInventoryRejectsPartialAndInvalidUpdates(t *testing.T) {
	s := fleetInventoryTestService(t, "a", "b")
	in := fleetInventoryTestImport(t, s)
	tests := map[string]func(*FleetInventory){
		"duplicate-id":    func(v *FleetInventory) { v.Records[1].HostID = v.Records[0].HostID },
		"duplicate-alias": func(v *FleetInventory) { v.Records[0].Aliases = append(v.Records[0].Aliases, v.Records[0].Aliases[0]) },
		"invalid-label":   func(v *FleetInventory) { v.Records[0].Labels["bad key"] = "value" },
		"empty-label":     func(v *FleetInventory) { v.Records[0].Labels["ok"] = "" },
		"case-colliding-labels": func(v *FleetInventory) {
			v.Records[0].Labels["env"], v.Records[0].Labels["Env"] = "a", "b"
		},
		"oversize-label": func(v *FleetInventory) { v.Records[0].Labels["ok"] = strings.Repeat("a", 129) },
		"many-labels": func(v *FleetInventory) {
			for i := 0; i <= FleetMaxLabels; i++ {
				v.Records[0].Labels[fmt.Sprint(i)] = "v"
			}
		},
		"unknown-id":         func(v *FleetInventory) { v.Records[0].HostID = strings.Repeat("f", 32) },
		"mixed-alias-target": func(v *FleetInventory) { v.Records[0].Aliases = append(v.Records[0].Aliases, v.Records[1].Aliases[0]) },
		"second-record-invalid": func(v *FleetInventory) {
			v.Records[0].Labels["valid"] = "update"
			v.Records[1].Aliases = []string{"missing"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := cloneFleetInventory(in)
			mutate(&candidate)
			if _, err := s.FleetInventoryUpdate(in.Revision, candidate.Records); err == nil {
				t.Fatal("invalid update accepted")
			}
			if !reflect.DeepEqual(in, s.FleetInventorySnapshot()) {
				t.Fatal("invalid update partially published")
			}
		})
	}
	if _, err := s.FleetInventoryUpdate(in.Revision, make([]FleetHost, FleetMaxHosts+1)); err == nil {
		t.Fatal("host limit not enforced")
	}
	s.inventory.mu.Lock()
	s.inventory.snapshot.Revision = math.MaxUint64
	s.inventory.mu.Unlock()
	if _, err := s.FleetInventoryUpdate(math.MaxUint64, in.Records); err == nil {
		t.Fatal("revision overflow accepted")
	}
}

func TestFleetInventorySchemaAndPrivateState(t *testing.T) {
	s := fleetInventoryTestService(t, "a")
	in := fleetInventoryTestImport(t, s)
	data, _ := json.Marshal(in)
	for name, raw := range map[string][]byte{
		"future":           []byte(strings.Replace(string(data), `"schema":1`, `"schema":2`, 1)),
		"duplicate-field":  []byte(strings.Replace(string(data), `"schema":1`, `"schema":1,"Schema":1`, 1)),
		"missing-revision": []byte(strings.Replace(string(data), `"revision":2,`, "", 1)),
		"missing-records":  []byte(`{"schema":1,"revision":1,"retired_ids":[]}`),
		"null":             []byte(`{"schema":1,"revision":1,"records":null,"retired_ids":[]}`),
		"unknown-field":    []byte(strings.Replace(string(data), `"schema":1`, `"schema":1,"owner":"admin"`, 1)),
		"truncated":        data[:len(data)-1],
		"trailing":         append(append([]byte{}, data...), []byte(" {}")...),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseFleetInventory(raw); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
	info, err := os.Stat(s.inventory.path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("state not private: %v %v", info, err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(s.inventory.path, link); err != nil {
		t.Fatal(err)
	}
	other := NewService(nil)
	t.Cleanup(func() { _ = other.Close(t.Context()) })
	if err := other.ConfigureFleetInventory(link); err == nil {
		t.Fatal("symlink inventory accepted")
	}
	if err := os.Chmod(s.inventory.path, 0644); err != nil {
		t.Fatal(err)
	}
	if err := other.ConfigureFleetInventory(s.inventory.path); err == nil {
		t.Fatal("public inventory accepted")
	}
}

func TestFleetInventoryDiskFailureDoesNotPublishAndUncertainFailsClosed(t *testing.T) {
	s := fleetInventoryTestService(t, "a")
	in := fleetInventoryTestImport(t, s)
	candidate := cloneFleetInventory(in)
	candidate.Records[0].Labels["env"] = "updated"
	s.inventory.write = func(string, FleetInventory) error { return errors.New("disk full") }
	if _, err := s.FleetInventoryUpdate(in.Revision, candidate.Records); err == nil || !reflect.DeepEqual(in, s.FleetInventorySnapshot()) {
		t.Fatal("failed persistence published state")
	}
	s.inventory.write = func(string, FleetInventory) error {
		return &fleetInventoryCommitUncertain{errors.New("directory fsync failed")}
	}
	if _, err := s.FleetInventoryUpdate(in.Revision, candidate.Records); err == nil {
		t.Fatal("uncertain commit accepted")
	}
	s.inventory.write = saveFleetInventory
	if _, err := s.FleetInventoryUpdate(in.Revision, candidate.Records); err == nil {
		t.Fatal("uncertain commit allowed another writer")
	}
	if _, err := s.FleetDispatchIdentity(in.Records[0], "a"); err == nil {
		t.Fatal("uncertain authority allowed dispatch")
	}
	if !reflect.DeepEqual(in, s.FleetInventorySnapshot()) {
		t.Fatal("last committed snapshot unavailable under storage pressure")
	}
}
