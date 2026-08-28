package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
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

func TestCLIExecTruncationNoticePreservesByteAndTerminalMetadata(t *testing.T) {
	stdout, _ := proto.NewTruncation(100, 40)
	stderr, _ := proto.NewTruncation(20, 15)
	notice := execTruncationNotice(&client.ExecResult{ExecResult: &proto.ExecResult{
		OperationID: "op_0123456789abcdef", Terminal: true, Execution: proto.StateCompleted,
		StdoutTruncation: stdout, StderrTruncation: stderr,
	}})
	for _, wanted := range []string{
		"operation_id=op_0123456789abcdef", "terminal=true", "execution_state=completed",
		"stdout_retained=40", "stdout_original=100", "stdout_dropped=60",
		"stderr_retained=15", "stderr_original=20", "stderr_dropped=5",
	} {
		if !strings.Contains(notice, wanted) {
			t.Fatalf("notice %q missing %q", notice, wanted)
		}
	}
}

func TestCLIErrorProjectionPreservesStableEnvelope(t *testing.T) {
	c := client.New(func(string, string) (*transport.AgentBinary, error) { return nil, nil })
	envelope := proto.NewError(proto.CodeObjectNotFound, "op_0123456789abcdef", proto.StateFailed)
	line := cliErrorLine(c, envelope)
	for _, wanted := range []string{
		"code=object.not_found", "category=storage", "retry=never", "retryable=false",
		"execution_state=failed", "operation_id=op_0123456789abcdef", "terminal=true",
		"message=requested object was not found",
	} {
		if !strings.Contains(line, wanted) {
			t.Fatalf("CLI error line %q missing %q", line, wanted)
		}
	}
}

func TestCLISyncTruncationNoticePreservesBothByteLedgers(t *testing.T) {
	stdout, _ := proto.NewTruncation(4096, 128)
	stderr, _ := proto.NewTruncation(512, 64)
	notice := syncTruncationNotice(&client.SyncResult{
		Truncated: true, StdoutTruncation: stdout, StderrTruncation: stderr,
	})
	for _, wanted := range []string{
		"stdout_retained=128", "stdout_original=4096", "stdout_dropped=3968",
		"stderr_retained=64", "stderr_original=512", "stderr_dropped=448",
	} {
		if !strings.Contains(notice, wanted) {
			t.Fatalf("sync notice %q missing %q", notice, wanted)
		}
	}
}

func TestCLISyncLimitValidationUsesStableCodes(t *testing.T) {
	c := client.New(func(string, string) (*transport.AgentBinary, error) { return nil, nil })
	for _, tt := range []struct {
		name string
		raw  string
		code proto.ErrorCode
	}{
		{name: "negative", raw: "-1", code: proto.CodeLimitExceeded},
		{name: "above_hard_cap", raw: "600000", code: proto.CodeLimitExceeded},
		{name: "integer_overflow", raw: "999999999999999999999999", code: proto.CodeInvalidRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := cmdSync(t.Context(), c, []string{"dev", "push", "local", "remote", "-max-output-bytes", tt.raw})
			var envelope *proto.ErrorEnvelope
			if !errors.As(err, &envelope) || envelope.Code != tt.code || envelope.ExecutionState != proto.StateNotSent {
				t.Fatalf("limit %q error = %v", tt.raw, err)
			}
		})
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

func TestPrintJSONRecursivelyRedactsSpecialCharacterSecret(t *testing.T) {
	c := client.New(func(string, string) (*transport.AgentBinary, error) { return nil, nil })
	token := "quote\"slash\\line\n雪界-token"
	if err := c.Secrets.Set(secrets.OutputKey("tok"), token); err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdout
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = original })
	if err := printJSON(c, map[string]any{"nested": []any{map[string]string{"value": "prefix " + token}}}); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	os.Stdout = original
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "雪界") || strings.Contains(string(raw), "quote") || !strings.Contains(string(raw), "redacted:tok") {
		t.Fatalf("CLI JSON boundary leaked or lost placeholder: %s", raw)
	}
}

func TestHostsAddRejectsInvalidDeclarationBeforeAnyMutation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	c := client.New(func(string, string) (*transport.AgentBinary, error) { return nil, nil })
	oldHost := transport.Host{Name: "dev", Addr: "u@old"}
	oldCwd := "~/old"
	if _, err := c.Hosts.ApplyHostUpdate(session.HostUpdate{
		Name: "dev", Host: &oldHost, Scope: session.ScopeGlobal, SetScope: true,
		Cwd: &oldCwd, Secrets: map[string]string{"old": "~/old-token"},
	}); err != nil {
		t.Fatal(err)
	}
	before, err := c.Hosts.Inspect("dev")
	if err != nil {
		t.Fatal(err)
	}
	invalidations := 0
	c.Hosts.SetHostChangeHook(func(string, uint64) { invalidations++ })

	err = cmdHosts(t.Context(), c, []string{
		"add", "dev", "u@new", "-cwd", "~/new", "-secret", "tok=", "-global", "-save",
	})
	if err == nil || !strings.Contains(err.Error(), "nonempty name and path") {
		t.Fatalf("invalid declaration error = %v", err)
	}
	after, err := c.Hosts.Inspect("dev")
	if err != nil {
		t.Fatal(err)
	}
	if after.Host != before.Host || after.Scope != before.Scope || after.Generation != before.Generation ||
		after.State.Cwd != before.State.Cwd || after.State.Secrets["old"] != "~/old-token" {
		t.Fatalf("invalid CLI update changed registry: before=%+v after=%+v", before, after)
	}
	if invalidations != 0 {
		t.Fatalf("invalid CLI update invalidations=%d, want 0", invalidations)
	}
	if _, err := os.Stat(filepath.Join(home, ".rdev", "hosts.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid CLI update touched persistence: %v", err)
	}
}
