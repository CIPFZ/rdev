package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/synctree"
)

func TestRemoteBrokerSyncManifest(t *testing.T) {
	d, namespace, ssh := newRemoteRuntime(t)
	owner := broker.Owner{ClientID: "manifest-client", ProjectID: "manifest-project"}
	p := broker.NewPolicy()
	if err := p.GrantHost(owner.Key(), "runtime-host", "sync", "sync.push"); err != nil {
		t.Fatal(err)
	}
	if err := p.Save(d.socket + ".policy"); err != nil {
		t.Fatal(err)
	}
	local := t.TempDir()
	path := filepath.Join(local, "large")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", 5<<20)), 0600); err != nil {
		t.Fatal(err)
	}
	remote := "~/" + namespace + "/manifest-target/"
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/manifest-target');os.makedirs(p,exist_ok=True)\n"); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	d.start()
	wire := d.dial(owner, d.token(owner, "5m"), true)
	request := func(dir string) broker.Response {
		return policyRuntimeRequest(t, wire, broker.Request{Owner: owner, Operation: "sync.push", Host: "runtime-host", Sync: &client.SyncOptions{Direction: "push", Local: dir + "/", Remote: remote, DryRun: true, MaxOutputBytes: 1024}})
	}
	before := request(local)
	if !before.OK || before.Sync == nil || !before.Sync.ManifestComplete {
		t.Fatal("source preview failed")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("y"), 4<<20); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	after := request(local)
	if !after.OK || after.Sync == nil || before.Sync.ManifestDigest == after.Sync.ManifestDigest {
		t.Fatal("actual daemon missed a source content change with preserved size and mtime")
	}
	if err := os.Truncate(path, synctree.MaxContentBytes+1); err != nil {
		t.Fatal(err)
	}
	if response := request(local); response.OK || response.Sync != nil {
		t.Fatal("oversized source exceeded the shared content budget")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	for i := range 8200 {
		if err := os.WriteFile(filepath.Join(local, fmt.Sprintf("entry-%05d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if response := request(local); response.OK || response.Sync != nil {
		t.Fatal("shared source exceeded the owner manifest entry budget")
	}
	if out, err := ssh("import os,sys\np=os.path.expanduser('~/'+sys.argv[1]+'/manifest-target');assert os.listdir(p)==[]\n"); err != nil {
		t.Fatalf("manifest preview changed target: %v %s", err, out)
	}
	t.Log("actual daemon/rsync/SSH: full 5 MiB source digest detects same-size/same-mtime mutation; sparse source beyond 8 GiB and 8200-entry source fail bounded shared scan; destination remains empty")
}
