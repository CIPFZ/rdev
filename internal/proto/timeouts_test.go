package proto

import (
	"encoding/json"
	"testing"
)

func TestTimeoutNormalizationPreservesApprovalInput(t *testing.T) {
	for _, req := range []*Request{
		{Op: OpExec, Exec: &ExecParams{Argv: []string{"true"}}},
		{Op: OpJobWait, Job: &JobParams{ID: "job"}},
		{Op: OpJobStart, Job: &JobParams{Spec: &ExecParams{Argv: []string{"true"}}}},
	} {
		before, _ := json.Marshal(req)
		normalized, err := NormalizeTimeouts(req)
		if err != nil {
			t.Fatal(err)
		}
		after, _ := json.Marshal(req)
		if string(before) != string(after) {
			t.Fatal("default resolution mutated approval/dedupe input")
		}
		switch req.Op {
		case OpExec:
			if normalized.Exec.TimeoutSec != 60 {
				t.Fatal(normalized.Exec)
			}
		case OpJobWait:
			if normalized.Job.WaitTimeoutSec != 300 {
				t.Fatal(normalized.Job)
			}
		case OpJobStart:
			if normalized.Job.Resources.WallTimeoutSec != 3600 || normalized.Job.Spec.TimeoutSec != 0 {
				t.Fatal(normalized.Job)
			}
		}
	}
}
