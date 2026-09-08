package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/transport"
)

func secretTestService(t *testing.T) (*Service, Owner, Owner) {
	t.Helper()
	s, a, b := approvalTestService(t)
	b = Owner{ClientID: a.ClientID, ProjectID: "other-project"}
	if err := s.ConfigureSecrets(filepath.Join(t.TempDir(), "secrets")); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []Owner{a, b} {
		for _, op := range []string{"secret.set", "secret.delete", "secret.list", "secret.use", proto.OpExec} {
			if err := s.Grant(owner, op); err != nil {
				t.Fatal(err)
			}
		}
	}
	return s, a, b
}
func setTestSecret(t *testing.T, s *Service, owner Owner, host, name, value string) {
	t.Helper()
	p := &SecretParams{Name: name, Value: value}
	target, _ := s.Client().ProtocolTargetIdentity(host)
	digest, err := s.Secrets.Plan(owner.Key(), host, target, "secret.set", p)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Secrets.Apply(owner.Key(), host, target, "secret.set", p, digest); err != nil {
		t.Fatal(err)
	}
}
func TestSecretOwnerHostApprovalAndFrozenSnapshot(t *testing.T) {
	s, a, b := secretTestService(t)
	setTestSecret(t, s, a, "h", "token", "alpha-old-credential")
	setTestSecret(t, s, b, "h", "token", "beta-distinct-credential")
	wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"echo"}, Env: map[string]string{"TOKEN": "secret:token"}}}
	req := Request{Owner: a, Host: "h", Operation: proto.OpExec, Wire: wire}
	approved, err := s.IssueApproval(ApprovalSpec{Owner: a, Host: "h", Operation: wire.Op, Wire: wire})
	if err != nil {
		t.Fatal(err)
	}
	req.Approval = approved.Token
	// Approval cannot move to another project which has an identically named key.
	changed := req
	changed.Owner = b
	if _, err := s.AuthorizeApproval(changed, s.DecideBrokerRequest(changed)); err == nil {
		t.Fatal("approval crossed principal")
	}
	plan, err := s.AuthorizeApproval(req, s.DecideBrokerRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	ApplyApprovedWire(&req, plan)
	setTestSecret(t, s, a, "h", "token", "alpha-rotated-credential")
	if req.Wire.Exec.Env["TOKEN"] != "alpha-old-credential" || wire.Exec.Env["TOKEN"] != "secret:token" {
		t.Fatal("approved snapshot mutated across rotation")
	}
	stale, err := s.IssueApproval(ApprovalSpec{Owner: a, Host: "h", Operation: wire.Op, Wire: wire})
	if err != nil {
		t.Fatal(err)
	}
	setTestSecret(t, s, a, "h", "token", "alpha-third-credential")
	req.Wire, req.Approval = wire, stale.Token
	if _, err := s.AuthorizeApproval(req, s.DecideBrokerRequest(req)); err == nil {
		t.Fatal("old approval selected rotated value")
	}
	req.Host = "other"
	if _, err := s.IssueApproval(ApprovalSpec{Owner: a, Host: req.Host, Operation: wire.Op, Wire: wire}); err == nil {
		t.Fatal("reference fell back across host")
	}
	req.Host = "h"
	if err := s.Revoke(a, "secret.use"); err != nil {
		t.Fatal(err)
	}
	if s.DecideBrokerRequest(req).Allow {
		t.Fatal("exec grant implied secret use")
	}
	if _, err := s.IssueApproval(ApprovalSpec{Owner: a, Host: "h", Operation: wire.Op, Wire: wire}); err == nil {
		t.Fatal("administrator bypassed secret use permission")
	}
	for _, v := range []string{"alpha-old-credential", "alpha-rotated-credential", "alpha-third-credential", "beta-distinct-credential"} {
		if strings.Contains(s.Client().Secrets.Redact(v), v) {
			t.Fatal("prior value no longer protected")
		}
	}
}
func TestSecretDurableDeleteReloadAndIdentityRetirement(t *testing.T) {
	s, a, b := secretTestService(t)
	path := s.Secrets.path
	setTestSecret(t, s, a, "h", "token", "durable-alpha-credential")
	setTestSecret(t, s, b, "h", "token", "durable-beta-credential")
	target, _ := s.Client().ProtocolTargetIdentity("h")
	p := &SecretParams{Name: "token"}
	digest, _ := s.Secrets.Plan(a.Key(), "h", target, "secret.delete", p)
	if err := s.Secrets.Apply(a.Key(), "h", target, "secret.delete", p, digest); err != nil {
		t.Fatal(err)
	}
	redactor := secrets.New()
	r := NewSecretRegistry(redactor)
	if err := r.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	aList, _ := r.List(a.Key(), "h", target)
	bList, _ := r.List(b.Key(), "h", target)
	if len(aList) != 0 || len(bList) != 1 {
		t.Fatal("deletion/reload crossed owner")
	}
	if strings.Contains(redactor.Redact("durable-alpha-credential"), "durable-alpha-credential") {
		t.Fatal("deleted credential lost restart protection")
	}
	if err := s.Client().Hosts.Add(transport.Host{Name: "h", Addr: "changed.invalid"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigureSecrets(path); err != nil {
		t.Fatal(err)
	}
	if list, _ := s.Secrets.List(b.Key(), "h", target); len(list) != 0 {
		t.Fatal("old host authority not durably retired")
	}
	for _, item := range s.Secrets.state.Records {
		if item.Active {
			t.Fatal("old binding active after startup reconciliation")
		}
	}
}
func TestSecretStorageFailureAndMalformedRecoveryFailClosed(t *testing.T) {
	s, a, _ := secretTestService(t)
	setTestSecret(t, s, a, "h", "token", "durable-original")
	path := s.Secrets.path
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p := &SecretParams{Name: "token", Value: "replacement-credential"}
	target, _ := s.Client().ProtocolTargetIdentity("h")
	digest, _ := s.Secrets.Plan(a.Key(), "h", target, "secret.set", p)
	s.Secrets.persist = func(path string, st secretState) error {
		if err := saveSecretState(path, st); err != nil {
			return err
		}
		return errors.New("after rename injected uncertainty")
	}
	if err := s.Secrets.Apply(a.Key(), "h", target, "secret.set", p, digest); err == nil {
		t.Fatal("uncertain commit acknowledged")
	}
	if _, err := s.Secrets.List(a.Key(), "h", target); err == nil {
		t.Fatal("uncertain registry remained usable")
	}
	recovered := NewSecretRegistry(secrets.New())
	if err := recovered.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(recovered.redactor.Redact(p.Value), p.Value) {
		t.Fatal("uncertain durable replacement lacks redaction")
	}
	cases := [][]byte{[]byte("null"), append(append([]byte{}, data...), []byte("{}")...), []byte(`{"schema":1,"Schema":1}`), []byte(`{"unknown":"sensitive-value"}`)}
	for _, bad := range cases {
		testPath := filepath.Join(t.TempDir(), "state")
		if err := os.WriteFile(testPath, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if err := NewSecretRegistry(secrets.New()).ConfigurePersistence(testPath); err == nil {
			t.Fatal("malformed secret state accepted")
		}
		got, _ := os.ReadFile(testPath)
		if string(got) != string(bad) {
			t.Fatal("malformed state overwritten")
		}
	}
}
func TestSecretCapacityAndConcurrentResolution(t *testing.T) {
	s, a, _ := secretTestService(t)
	setTestSecret(t, s, a, "h", "token", "initial-version")
	var wg sync.WaitGroup
	target, _ := s.Client().ProtocolTargetIdentity("h")
	wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Env: map[string]string{"X": "secret:token"}}}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				resolved, _, err := s.Secrets.Resolve(a.Key(), "h", target, wire)
				if err != nil || resolved.Exec.Env["X"] == "secret:token" {
					t.Error("resolution failed during rotation")
				}
			}
		}()
	}
	for i := 0; i < 10; i++ {
		setTestSecret(t, s, a, "h", "token", "next-version-value")
	}
	wg.Wait()
	// Exercise the byte budget with genuine durable snapshots, leaving the old
	// accepted version usable when admission rejects a replacement.
	value := strings.Repeat("v", maxSecretValue)
	p := &SecretParams{Name: "capacity", Value: value}
	rejected := false
	for i := 0; i < 20; i++ {
		digest, err := s.Secrets.Plan(a.Key(), "h", target, "secret.set", p)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Secrets.Apply(a.Key(), "h", target, "secret.set", p, digest); err != nil {
			rejected = true
			break
		}
	}
	if !rejected {
		t.Fatal("owner byte retention unbounded")
	}
	if _, _, err := s.Secrets.Resolve(a.Key(), "h", target, wire); err != nil {
		t.Fatal("capacity rejection removed accepted credential")
	}
}
func TestSecretMutationDurableIdentityAndApprovalBinding(t *testing.T) {
	s, a, _ := secretTestService(t)
	if err := s.Mutations.ConfigurePersistence(filepath.Join(t.TempDir(), "intents")); err != nil {
		t.Fatal(err)
	}
	p := &SecretParams{Name: "token", Value: "approved-secret-value"}
	spec := ApprovalSpec{Owner: a, Host: "h", Operation: "secret.set", Secret: p}
	approval, err := s.IssueApproval(spec)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := proto.NewOperationID()
	req := Request{Owner: a, Host: "h", Operation: spec.Operation, Secret: p, Approval: approval.Token, OperationID: id}
	changed := req
	changed.Secret = &SecretParams{Name: p.Name, Value: "substituted-secret-value"}
	if _, err := s.AuthorizeApproval(changed, s.DecideBrokerRequest(changed)); err == nil {
		t.Fatal("secret payload substitution approved")
	}
	plan, err := s.AuthorizeApproval(req, s.DecideBrokerRequest(req))
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.DispatchSecretMutation(context.Background(), req, plan)
	if err != nil || m.State != "completed" {
		t.Fatal("secret mutation failed")
	}
	if _, err := s.DispatchSecretMutation(context.Background(), req, plan); !errors.Is(err, ErrMutationRecorded) {
		t.Fatal("secret mutation identity reused")
	}
	data, _ := json.Marshal(m)
	if strings.Contains(string(data), p.Value) || strings.Contains(string(data), p.Name) {
		t.Fatal("payload in mutation metadata")
	}
}

func TestSecretSlowPersistencePreservesOtherOwnerReads(t *testing.T) {
	s, a, b := secretTestService(t)
	setTestSecret(t, s, a, "h", "token", "alpha-value")
	setTestSecret(t, s, b, "h", "token", "beta-value")
	target, _ := s.Client().ProtocolTargetIdentity("h")
	p := &SecretParams{Name: "token", Value: "alpha-new-value"}
	digest, _ := s.Secrets.Plan(a.Key(), "h", target, "secret.set", p)
	entered, release := make(chan struct{}), make(chan struct{})
	s.Secrets.persist = func(string, secretState) error {
		close(entered)
		<-release
		return errors.New("injected storage failure")
	}
	done := make(chan error, 1)
	go func() { done <- s.Secrets.Apply(a.Key(), "h", target, "secret.set", p, digest) }()
	defer func() { close(release); <-done }()
	<-entered
	other := make(chan error, 1)
	go func() {
		_, _, err := s.Secrets.Resolve(b.Key(), "h", target, &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Env: map[string]string{"X": "secret:token"}}})
		other <- err
	}()
	select {
	case err := <-other:
		if err != nil {
			t.Fatal("other owner lost committed snapshot")
		}
	case <-time.After(time.Second):
		t.Fatal("secret persistence blocked other owner reads")
	}
}

func TestSecretExpansionBoundAndApprovalCachePrivacy(t *testing.T) {
	s, a, _ := secretTestService(t)
	setTestSecret(t, s, a, "h", "token", strings.Repeat("a", 65536))
	wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Argv: []string{"echo"}, Env: map[string]string{"X": "secret:token"}}}
	approval, err := s.IssueApproval(ApprovalSpec{Owner: a, Host: "h", Operation: wire.Op, Wire: wire})
	if err != nil {
		t.Fatal(err)
	}
	if approval.Plan.resolvedWire != nil || s.approvalByToken[approval.Token].Plan.resolvedWire != nil {
		t.Fatal("approval cache retains plaintext request")
	}
	req := Request{Owner: a, Host: "h", Operation: wire.Op, Wire: wire, Approval: approval.Token}
	plan, err := s.AuthorizeApproval(req, s.DecideBrokerRequest(req))
	if err != nil || plan.ExpandedRequestBytes() < 65536 {
		t.Fatal("expanded request missing ingress charge")
	}
	for i := 0; i < 17; i++ {
		wire.Exec.Env[strconv.Itoa(i)] = "secret:token"
	}
	if _, err := s.IssueApproval(ApprovalSpec{Owner: a, Host: "h", Operation: wire.Op, Wire: wire}); err == nil {
		t.Fatal("repeated references expanded past request bound")
	}
}

func TestSecretEncodedArchiveCapacityDoesNotFailExistingStore(t *testing.T) {
	s, a, _ := secretTestService(t)
	setTestSecret(t, s, a, "h", "token", "retained-old-credential")
	target, _ := s.Client().ProtocolTargetIdentity("h")
	// Control-heavy UTF-8 uses six JSON bytes per value byte. Build an archive
	// just below the serialized limit, with independently valid owner budgets.
	for i := 0; i < 53; i++ {
		owner := Owner{ClientID: "archive-owner", ProjectID: strconv.Itoa(i / 16)}
		s.Secrets.state.Records = append(s.Secrets.state.Records, secretRecord{Owner: owner.Key(), Host: "h", Target: target, Name: strconv.Itoa(i), Version: fmt.Sprintf("%064x", i+1), Value: strings.Repeat(string(rune(1)), 65536)})
	}
	if err := validateSecretState(s.Secrets.state); err != nil {
		t.Fatal("bounded predecessor rejected", err)
	}
	owner := Owner{ClientID: "archive-owner", ProjectID: "3"}
	params := &SecretParams{Name: "next", Value: strings.Repeat(string(rune(1)), 65536)}
	digest, err := s.Secrets.Plan(owner.Key(), "h", target, "secret.set", params)
	if err != nil {
		t.Fatal(err)
	}
	s.Secrets.persist = func(string, secretState) error {
		t.Error("oversize snapshot reached storage")
		return errors.New("unexpected write")
	}
	if err := s.Secrets.Apply(owner.Key(), "h", target, "secret.set", params, digest); err == nil {
		t.Fatal("serialized archive exceeded hard cap")
	}
	wire := &proto.Request{Op: proto.OpExec, Exec: &proto.ExecParams{Env: map[string]string{"X": "secret:token"}}}
	if _, _, err := s.Secrets.Resolve(a.Key(), "h", target, wire); err != nil {
		t.Fatal("size admission failure disabled accepted credential")
	}
}
