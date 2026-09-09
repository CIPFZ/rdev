package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/synctree"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestExecutingSyncRetainsReservationAcrossPlanExpiry(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	if err := s.client.Hosts.Add(transport.Host{Name: "host", Addr: "unused.invalid"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigureSync(filepath.Join(t.TempDir(), "sync")); err != nil {
		t.Fatal(err)
	}
	if err := s.Mutations.ConfigurePersistence(filepath.Join(t.TempDir(), "mutations")); err != nil {
		t.Fatal(err)
	}
	source, destination := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("retained"), 0600); err != nil {
		t.Fatal(err)
	}
	owner := Owner{ClientID: "sync", ProjectID: "expiry"}
	id, _ := synctree.NewID()
	stage, err := s.syncStore.Capture(t.Context(), owner.Key(), id, source, "preserve")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := synctree.Inspect(t.Context(), destination, synctree.StageLimits)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := synctree.Build(stage.Manifest, snapshot, false, "overwrite")
	if err != nil {
		t.Fatal(err)
	}
	opts, err := client.NormalizeSyncOptions(client.SyncOptions{Direction: "pull", Local: destination, Remote: "/source", PlanID: id})
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.client.ProtocolTargetIdentity("host")
	if err != nil {
		t.Fatal(err)
	}
	releasePlan, err := s.Ingress.Hold(owner.Key(), 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	p := &preparedSync{Owner: owner.Key(), Host: "host", Target: target, Options: opts, Destination: destination, Plan: plan, StageID: id, Expires: time.Now().Add(time.Minute), Release: releasePlan}
	key := owner.Key() + "\x00" + id
	s.syncPlans[key] = p
	operationID, _ := proto.NewOperationID()
	req := Request{Owner: owner, Host: "host", Operation: "sync.pull", OperationID: operationID, Sync: &opts}
	approval, err := s.SyncApproval(req, Decision{Digest: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	approval.ApprovalID = strings.Repeat("b", 64)
	entered, unblock := make(chan struct{}), make(chan struct{})
	busy := make(chan error, 1)
	go func() {
		_, err := s.DispatchScheduled(t.Context(), "host", "busy", LaneBulk, func(ctx context.Context) (*proto.Response, error) {
			close(entered)
			select {
			case <-unblock:
				return &proto.Response{OK: true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		busy <- err
	}()
	<-entered
	done := make(chan error, 1)
	go func() { _, _, err := s.ExecuteSync(t.Context(), req, approval); done <- err }()
	deadline := time.Now().Add(5 * time.Second)
	for s.Scheduler.Snapshot(owner.Key()).Queued != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Scheduler.Snapshot(owner.Key()).Queued != 1 {
		select {
		case err := <-done:
			t.Fatal("sync execution exited before queueing", err)
		default:
			t.Fatal("sync execution did not queue")
		}
	}
	s.syncMu.Lock()
	expired := s.expireSyncPlanLocked(key, p.Expires.Add(time.Second))
	s.syncMu.Unlock()
	if expired || s.Ingress.Snapshot(owner.Key()).ObservationBytes != 24<<20 {
		t.Fatal("expiry released live sync reservation")
	}
	close(unblock)
	if err := <-busy; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("sync did not finish")
	}
	if s.Ingress.Snapshot(owner.Key()).ObservationBytes != 0 {
		t.Fatal("completed execution retained reservation")
	}
	if data, err := os.ReadFile(filepath.Join(destination, "file")); err != nil || string(data) != "retained" {
		t.Fatal("queued execution lost retained plan", err)
	}
}
