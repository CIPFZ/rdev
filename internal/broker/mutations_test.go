package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func testMutation(owner, id string) MutationIntent {
	return MutationIntent{Owner: owner, OperationID: id, Operation: proto.OpExec, Host: "h", RequestDigest: strings.Repeat("a", 64), TargetDigest: strings.Repeat("b", 64), PolicyDigest: strings.Repeat("c", 64), ApprovalID: strings.Repeat("d", 64)}
}

func TestConcurrentMutationJobResolutionSharesDurableOutcome(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	if err := s.Client().Hosts.Add(transport.Host{Name: "h", Addr: "u@h"}); err != nil {
		t.Fatal(err)
	}
	m := testMutation("a\x00p", "op_concurrent_resolution")
	m.Operation = proto.OpJobStart
	m.JobID, _ = proto.JobIDForOperation(proto.PrincipalID("a", "p"), m.OperationID)
	m.JobDigest = strings.Repeat("e", 64)
	m.TargetDigest, _ = s.Client().ProtocolTargetIdentity("h")
	if err := s.Mutations.ConfigurePersistence(filepath.Join(t.TempDir(), "intents")); err != nil {
		t.Fatal(err)
	}
	if err := s.Mutations.Prepare(m); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"dispatched", "ambiguous"} {
		if err := s.Mutations.Transition(m.Owner, m.OperationID, state, false); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.Mutations.persist = func(path string, records []MutationIntent) error {
		once.Do(func() { close(entered); <-release })
		return saveMutations(path, records)
	}
	info := &proto.JobInfo{ID: m.JobID, StartOperationID: m.OperationID, StartPrincipalID: proto.PrincipalID("a", "p"), StartDigest: m.JobDigest}
	results := make(chan error, 32)
	go func() { results <- s.ResolveMutationJob("h", m.Owner, info) }()
	<-entered
	for range 31 {
		go func() { results <- s.ResolveMutationJob("h", m.Owner, info) }()
	}
	// Hold publication while other readers capture the same ambiguous state.
	time.Sleep(50 * time.Millisecond)
	close(release)
	for range 32 {
		if err := <-results; err != nil {
			t.Fatal("concurrent recovery rejected the shared successful outcome", err)
		}
	}
	got, err := s.Mutations.Get(m.Owner, m.OperationID)
	if err != nil || got.State != "completed" || !got.RemoteOK {
		t.Fatal("concurrent resolution did not persist outcome")
	}
}

func TestMutationOwnerRetentionNeverEvictsReplayProtection(t *testing.T) {
	r := NewMutationRegistry()
	for i := 0; i < maxOwnerMutationIntents; i++ {
		if err := r.Prepare(testMutation("a\x00p", fmt.Sprintf("op_retained_%08d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Prepare(testMutation("a\x00p", "op_retained_over_limit")); err == nil {
		t.Fatal("owner exceeded identity retention budget")
	}
	if err := r.Prepare(testMutation("b\x00p", "op_retained_00000000")); err != nil {
		t.Fatal("one owner's retention budget blocked another owner", err)
	}
	if err := r.Prepare(testMutation("a\x00p", "op_retained_00000000")); !errors.Is(err, ErrMutationRecorded) {
		t.Fatal("oldest replay identity was evicted", err)
	}
}
func TestMutationDurabilityIsolationAndUncertainRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mutations")
	r := NewMutationRegistry()
	if err := r.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	a := testMutation("a\x00p", "op_mutation_test_1")
	b := testMutation("b\x00p", a.OperationID)
	for _, m := range []MutationIntent{a, b} {
		if err := r.Prepare(m); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Prepare(a); !errors.Is(err, ErrMutationRecorded) {
		t.Fatal(err)
	}
	changed := a
	changed.RequestDigest = strings.Repeat("e", 64)
	if err := r.Prepare(changed); err == nil {
		t.Fatal("operation identity changed payload")
	}
	if _, err := r.Get("other\x00p", a.OperationID); err == nil {
		t.Fatal("other principal read mutation")
	}
	before, _ := os.ReadFile(path)
	entered, release := make(chan struct{}), make(chan struct{})
	r.persist = func(string, []MutationIntent) error {
		close(entered)
		<-release
		return errors.New("pre-rename failure")
	}
	done := make(chan error, 1)
	go func() { done <- r.Transition(a.Owner, a.OperationID, "dispatched", false) }()
	<-entered
	read := make(chan error, 1)
	go func() { _, err := r.Get(b.Owner, b.OperationID); read <- err }()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("disk stall blocked another principal")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("failed dispatch persistence acknowledged")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("failed dispatch changed snapshot")
	}
	r.persist = func(path string, records []MutationIntent) error {
		if err := saveMutations(path, records); err != nil {
			return err
		}
		return &mutationCommitUncertain{errors.New("late directory fsync")}
	}
	if err := r.Transition(a.Owner, a.OperationID, "dispatched", false); err == nil {
		t.Fatal("uncertain dispatch acknowledged")
	}
	if _, err := r.Get(a.Owner, a.OperationID); !errors.Is(err, ErrMutationStorage) {
		t.Fatal("uncertain storage remained active")
	}
	restarted := NewMutationRegistry()
	if err := restarted.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	got, _ := restarted.Get(a.Owner, a.OperationID)
	other, _ := restarted.Get(b.Owner, b.OperationID)
	if got.State != "ambiguous" || other.State != "not_sent" {
		t.Fatalf("restart states %s %s", got.State, other.State)
	}
	if err := restarted.Prepare(a); !errors.Is(err, ErrMutationRecorded) {
		t.Fatal("restart replayed ambiguous operation")
	}
}

func TestMutationStrictPrivateSnapshotPreservesInvalidState(t *testing.T) {
	m := testMutation("a\x00p", "op_mutation_test_1")
	m.State = "prepared"
	m.Updated = time.Now().UTC()
	valid, _ := json.Marshal(mutationSnapshot{Schema: 1, Records: []MutationIntent{m}})
	for name, data := range map[string][]byte{
		"null": []byte("null"), "future": bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":2`), 1),
		"duplicate":       bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":1,"Schema":1`), 1),
		"duplicate_owner": bytes.Replace(valid, []byte(`"owner":`), []byte(`"owner":"wrong","owner":`), 1),
		"unknown":         bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":1,"secret":"forbidden"`), 1),
		"public":          valid, "oversize": bytes.Repeat([]byte("x"), maxMutationBytes+1),
		"deep": []byte(strings.Repeat("[", 64) + "1" + strings.Repeat("]", 64)),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state")
			mode := os.FileMode(0600)
			if name == "public" {
				mode = 0644
			}
			if err := os.WriteFile(path, data, mode); err != nil {
				t.Fatal(err)
			}
			if err := NewMutationRegistry().ConfigurePersistence(path); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(after, data) {
				t.Fatal("invalid snapshot overwritten")
			}
		})
	}
}

func TestMutationPreparationFailureNeverDispatches(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	path := filepath.Join(t.TempDir(), "mutations")
	if err := s.Mutations.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	s.Mutations.persist = func(string, []MutationIntent) error { return errors.New("storage unavailable") }
	s.SetDispatcher(func(context.Context, string, *proto.Request) (*proto.Response, error) {
		t.Fatal("mutation sent before intent durability")
		return nil, nil
	})
	m := testMutation("a\x00p", "op_mutation_test_1")
	_, _, err := s.DispatchMutation(t.Context(), Request{Owner: Owner{ClientID: "a", ProjectID: "p"}, Host: "h", Wire: &proto.Request{Op: proto.OpExec, OperationID: m.OperationID, Exec: &proto.ExecParams{Argv: []string{"false"}}}}, ApprovalPlan{RequestDigest: m.RequestDigest, TargetDigest: m.TargetDigest, PolicyDigest: m.PolicyDigest, ApprovalID: m.ApprovalID})
	if err == nil {
		t.Fatal("preparation failure acknowledged")
	}
}
