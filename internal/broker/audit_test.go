package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAuditLogRotatesAndQueries(t *testing.T) {
	a := NewAuditLog(2)
	now := time.Now()
	a.Append(AuditEvent{At: now.Add(-3 * time.Second), Owner: "o", Operation: "exec", Decision: "deny"})
	a.Append(AuditEvent{At: now.Add(-2 * time.Second), Owner: "o", Operation: "exec", Decision: "allow"})
	a.Append(AuditEvent{At: now, Owner: "o", Operation: "exec", Result: "ok"})
	if len(a.Query(now.Add(-time.Second))) != 1 {
		t.Fatal("query/rotation")
	}
}

func TestAuditLogPersistsAndRotatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	a := NewAuditLog(8)
	if err := a.ConfigureFile(path, 80); err != nil {
		t.Fatal(err)
	}
	a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "exec", Result: "first"})
	a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "exec", Result: "second"})
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatal("audit rotation missing")
	}
}

func TestAuditLogRecoversAfterRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	a := NewAuditLog(32)
	if err := a.ConfigureFile(path, 120); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "exec", Result: "event-with-padding"})
	}
	b := NewAuditLog(32)
	if err := b.ConfigureFile(path, 120); err != nil {
		t.Fatal(err)
	}
	if got := b.QueryOwner(time.Time{}, "owner"); len(got) == 0 {
		t.Fatal("rotated audit history was not recoverable")
	}
}

func TestAuditLogSanitizesFields(t *testing.T) {
	a := NewAuditLog(4)
	a.Append(AuditEvent{At: time.Now(), Owner: "owner\nsecret", Result: "secret=top-secret token=abc"})
	events := a.Query(time.Time{})
	if len(events) != 1 || events[0].Result != "secret=[REDACTED] token=[REDACTED]" || events[0].Owner != "owner secret" {
		t.Fatalf("audit fields were not bounded/sanitized: %+v", events)
	}
}

func TestAuditLogLoadsHistoryAndScopesOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.jsonl")
	first := NewAuditLog(8)
	if err := first.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	first.Append(AuditEvent{At: now, Owner: "a", Operation: "exec"})
	first.Append(AuditEvent{At: now, Owner: "b", Operation: "exec"})
	second := NewAuditLog(8)
	if err := second.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	if got := second.QueryOwner(now.Add(-time.Second), "a"); len(got) != 1 {
		t.Fatalf("history/scope count=%d", len(got))
	}
}
