package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/session"
)

func TestProjectApprovalOutputTreatsCommittedWarningAsSuccess(t *testing.T) {
	trust := session.ProjectTrust{Path: "/project/.rdev/hosts.json", Digest: strings.Repeat("a", 64), Approved: true}
	warning := &session.ConfigWriteCommittedError{Cause: errors.New("injected backup cleanup failure")}
	out, err := projectApprovalOutput(trust, warning)
	if err != nil {
		t.Fatalf("committed approval projected as CLI failure: %v", err)
	}
	if !out.Approved || out.Path != trust.Path || out.Digest != trust.Digest || out.Warning == "" {
		t.Fatalf("CLI committed approval output=%+v", out)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var projected map[string]any
	if err := json.Unmarshal(b, &projected); err != nil {
		t.Fatal(err)
	}
	if projected["approved"] != true || projected["warning"] == "" {
		t.Fatalf("CLI JSON projection=%s", b)
	}
}

func TestProjectApprovalOutputKeepsAmbiguousFailure(t *testing.T) {
	ambiguous := &session.ConfigWriteAmbiguousError{
		Cause: errors.New("injected directory fsync failure"), Rollback: errors.New("injected rollback failure"),
	}
	if _, err := projectApprovalOutput(session.ProjectTrust{}, ambiguous); !errors.Is(err, ambiguous) {
		t.Fatalf("ambiguous approval error=%v", err)
	}
}
