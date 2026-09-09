package broker

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestDefaultJobTimeoutDurableDigestAndReplay(t *testing.T) {
	for _, resource := range []*proto.ResourceEnvelope{nil, {}, {WallTimeoutSec: 0, FDs: 128}, {WallTimeoutSec: 1}} {
		s := NewService(nil)
		if err := s.Client().Hosts.Add(transport.Host{Name: "h", Addr: "u@h"}); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "mutations")
		if err := s.Mutations.ConfigurePersistence(path); err != nil {
			t.Fatal(err)
		}
		owner := Owner{ClientID: "a", ProjectID: "p"}
		id, _ := proto.NewOperationID()
		req := Request{Owner: owner, Host: "h", Operation: proto.OpJobStart, Wire: &proto.Request{Op: proto.OpJobStart, ClientID: owner.ClientID, ProjectID: owner.ProjectID, OperationID: id, Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"true"}}, Resources: resource}}}
		original, _ := json.Marshal(req)
		target, _ := s.Client().ProtocolTargetIdentity("h")
		plan := ApprovalPlan{RequestDigest: strings.Repeat("a", 64), TargetDigest: target, PolicyDigest: strings.Repeat("b", 64), ApprovalID: strings.Repeat("c", 64)}
		calls := 0
		s.SetDispatcher(func(_ context.Context, _ string, wire *proto.Request) (*proto.Response, error) {
			calls++
			effective, err := proto.NormalizeTimeouts(wire)
			if err != nil {
				return nil, err
			}
			digest, err := proto.DurableJobDigest(effective)
			if err != nil {
				return nil, err
			}
			principal := proto.PrincipalID(owner.ClientID, owner.ProjectID)
			jobID, _ := proto.JobIDForOperation(principal, id)
			info := &proto.JobInfo{ID: jobID, State: proto.JobRunning, StartOperationID: id, StartPrincipalID: principal, StartDigest: digest}
			return &proto.Response{OK: true, OperationID: id, Terminal: true, Execution: proto.StateCompleted, Job: &proto.JobResult{Info: info}}, nil
		})
		_, intent, err := s.DispatchMutation(t.Context(), req, plan)
		if err != nil || intent == nil || intent.State != "completed" || calls != 1 {
			t.Fatalf("default job outcome %+v calls=%d err=%v", intent, calls, err)
		}
		after, _ := json.Marshal(req)
		if string(original) != string(after) {
			t.Fatal("approval input mutated")
		}
		req.Wire.Replay = true
		if _, _, err := s.DispatchMutation(t.Context(), req, plan); !errors.Is(err, ErrMutationRecorded) || calls != 1 {
			t.Fatalf("replay reexecuted: calls=%d err=%v", calls, err)
		}
		recovered := NewMutationRegistry()
		if err := recovered.ConfigurePersistence(path); err != nil {
			t.Fatal(err)
		}
		stored, err := recovered.Get(owner.Key(), id)
		if err != nil || stored.JobDigest != intent.JobDigest || stored.State != "completed" {
			t.Fatal("durable identity lost", err)
		}
		s.Close(context.Background())
	}
}
