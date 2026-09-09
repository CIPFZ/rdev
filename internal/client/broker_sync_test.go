package client

import (
	"context"
	"errors"
	"github.com/CIPFZ/rdev/internal/transport"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/synctree"
)

func TestSyncPreparedRsyncDeletionScope(t *testing.T) {
	if _, err := exec.LookPath("rsync"); err != nil {
		t.Skip("rsync unavailable")
	}
	for _, prefix := range []bool{false, true} {
		name := "contents"
		if prefix {
			name = "directory"
		}
		t.Run(name, func(t *testing.T) {
			source := filepath.Join(t.TempDir(), "source")
			dest := t.TempDir()
			state := filepath.Join(t.TempDir(), "state")
			if err := os.Mkdir(source, 0700); err != nil {
				t.Fatal(err)
			}
			writeFile := func(path, value string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			writeFile(filepath.Join(source, "keep"), "new")
			writeFile(filepath.Join(source, "excluded"), "excluded source")
			sub := dest
			if prefix {
				sub = filepath.Join(dest, "source")
				if err := os.Mkdir(sub, 0700); err != nil {
					t.Fatal(err)
				}
				writeFile(filepath.Join(dest, "sibling"), "outside deletion scope")
			}
			writeFile(filepath.Join(sub, "obsolete"), "delete")
			writeFile(filepath.Join(sub, "excluded"), "protected")
			if err := os.Mkdir(filepath.Join(sub, "excluded-dir"), 0700); err != nil {
				t.Fatal(err)
			}
			writeFile(filepath.Join(sub, "excluded-dir", "nested"), "protected nested")
			store, err := synctree.NewStore(state)
			if err != nil {
				t.Fatal(err)
			}
			id, _ := synctree.NewID()
			stage, err := store.Capture(t.Context(), "owner", id, source, "preserve")
			if err != nil {
				t.Fatal(err)
			}
			snap, err := synctree.Inspect(t.Context(), dest, synctree.StageLimits)
			if err != nil {
				t.Fatal(err)
			}
			opts := SyncOptions{Delete: true, Exclude: []string{"excluded", "excluded-dir/"}}
			c := &Client{}
			var deletions map[string]bool
			filtered, err := store.Rewrite(t.Context(), "owner", id, func(dir string) error {
				var err error
				deletions, err = c.SyncDeletionPaths(t.Context(), dir, stage, snap, opts, prefix)
				if err != nil {
					return err
				}
				var root *synctree.Entry
				if prefix {
					root = &snap.Manifest.Entries[0]
				}
				return c.FilterSyncStage(t.Context(), dir, opts, true, stage.SourceName, prefix, "", root)
			})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := synctree.BuildScoped(filtered.Manifest, snap, deletions, "overwrite")
			if err != nil {
				t.Fatal(err)
			}
			dataDir, err := store.Directory("owner", id)
			if err != nil {
				t.Fatal(err)
			}
			if err := synctree.Apply(t.Context(), dataDir, dest, plan, synctree.StageLimits); err != nil {
				t.Fatal(err)
			}
			for path, want := range map[string]string{filepath.Join(sub, "keep"): "new", filepath.Join(sub, "excluded"): "protected", filepath.Join(sub, "excluded-dir", "nested"): "protected nested"} {
				got, err := os.ReadFile(path)
				if err != nil || string(got) != want {
					t.Fatalf("unexpected contents %q: %v", path, err)
				}
			}
			if _, err := os.Stat(filepath.Join(sub, "obsolete")); !os.IsNotExist(err) {
				t.Fatal("planned extra file not removed")
			}
			if prefix {
				got, err := os.ReadFile(filepath.Join(dest, "sibling"))
				if err != nil || string(got) != "outside deletion scope" {
					t.Fatal("directory prefix deleted sibling")
				}
			}
		})
	}
}

func TestSyncRawResponseCannotReachGenericFrontend(t *testing.T) {
	c := New(nil)
	defer c.Close()
	input := &proto.Response{OK: true, Sync: &proto.SyncResult{Data: []byte("raw binary credential")}}
	output := c.redactResponse(input)
	if output.Sync != nil || input.Sync == nil {
		t.Fatal("generic response exposed raw sync data or mutated transport response")
	}
}

func TestSyncUsesSharedRetryPolicyAndPreservesRawBytes(t *testing.T) {
	for _, op := range []string{proto.OpSyncInspect, proto.OpSyncCommit} {
		t.Run(op, func(t *testing.T) {
			c := newTestClient()
			defer c.Close()
			if err := c.Hosts.Add(transport.Host{Name: "sync-host", Addr: "sync.invalid"}); err != nil {
				t.Fatal(err)
			}
			target, err := c.ProtocolTargetIdentity("sync-host")
			if err != nil {
				t.Fatal(err)
			}
			const raw = "retained-secret-content"
			if err := c.Secrets.Set(secrets.OutputKey("test-secret"), raw); err != nil {
				t.Fatal(err)
			}
			var conns []*fakeRemoteConn
			var ids []string
			c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
				conn := &fakeRemoteConn{host: h}
				index := len(conns)
				conns = append(conns, conn)
				conn.handler = func(r *proto.Request) (*proto.Response, error) {
					ids = append(ids, r.OperationID)
					if r.ClientID != proto.PrincipalID("owner", "project") || r.ProjectID != "project" || r.DeadlineUnixMilli == 0 {
						t.Fatal("sync lost principal or deadline")
					}
					if index == 1 {
						return nil, errors.New("bulk disconnected")
					}
					return &proto.Response{OK: true, OperationID: r.OperationID, Sync: &proto.SyncResult{Data: []byte(raw)}}, nil
				}
				return conn, nil
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			id, _ := proto.NewOperationID()
			result, err := c.SyncProtocol(ctx, "sync-host", target, "owner", "project", op, id, &proto.SyncParams{})
			if op == proto.OpSyncInspect {
				if err != nil || result == nil || string(result.Data) != raw || len(ids) != 2 {
					t.Fatalf("safe raw sync recovery: %v %+v", err, result)
				}
			} else {
				if err == nil || len(ids) != 1 {
					t.Fatal("commit replayed")
				}
			}
			for _, got := range ids {
				if got != id {
					t.Fatal("stable sync identity replaced")
				}
			}
			if len(conns) < 2 || conns[0].closed || !conns[1].closed {
				t.Fatal("bulk failure damaged base connection or retained broken connection")
			}
		})
	}
}
