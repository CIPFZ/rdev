package broker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
	if err := a.ConfigureFile(path, 1024); err != nil {
		t.Fatal(err)
	}
	a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "exec", Result: "first"})
	a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "exec", Result: "second"})
	for i := 0; i < 12; i++ {
		a.Append(AuditEvent{Owner: "owner", Operation: "exec"})
	}
	flushAudit(t, a)
	defer a.Close()
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
	if err := a.ConfigureFile(path, 1024); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "exec", Result: "event-with-padding"})
	}
	flushAudit(t, a)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b := NewAuditLog(32)
	defer b.Close()
	if err := b.ConfigureFile(path, 1024); err != nil {
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
	if len(events) != 1 || events[0].Result != "unknown" || events[0].Owner != AuditOwnerID("owner\nsecret") {
		t.Fatalf("audit fields were not bounded/sanitized: %+v", events)
	}
}

func TestAuditOwnerKeysDoNotCollideAfterSanitization(t *testing.T) {
	a := NewAuditLog(10)
	first := Owner{ClientID: "a b", ProjectID: "c"}
	second := Owner{ClientID: "a", ProjectID: "b c"}
	if strings.ReplaceAll(first.Key(), "\x00", " ") != strings.ReplaceAll(second.Key(), "\x00", " ") {
		t.Fatal("fixture must collide under the old sanitizer")
	}
	a.Append(AuditEvent{Owner: first.Key(), Operation: "exec", Result: "accepted"})
	a.Append(AuditEvent{Owner: second.Key(), Operation: "exec", Result: "completed"})
	for owner, result := range map[Owner]string{first: "accepted", second: "completed"} {
		got := a.QueryOwner(time.Time{}, owner.Key())
		if len(got) != 1 || got[0].Result != result || got[0].At.IsZero() {
			t.Fatalf("owner scope lost: %+v", got)
		}
	}
}

func TestAuditDoesNotPersistArbitraryClientText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	a := NewAuditLog(8)
	if err := a.ConfigureFile(path, 4096); err != nil {
		t.Fatal(err)
	}
	const secret = "raw-bearer-credential-that-has-no-special-prefix"
	a.Append(AuditEvent{Owner: secret, Operation: secret, Decision: secret, Result: secret})
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), secret) {
		t.Fatal("untrusted text leaked into the audit sink")
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
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := NewAuditLog(8)
	defer second.Close()
	if err := second.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	if got := second.QueryOwner(now.Add(-time.Second), "a"); len(got) != 1 {
		t.Fatalf("history/scope count=%d", len(got))
	}
}

func TestAuditLogRestartRestoresRotatedSegment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.jsonl")
	a := NewAuditLog(32)
	if err := a.ConfigureFile(path, 1024); err != nil {
		t.Fatal(err)
	}
	a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "before", Result: "padding-xxxxxxxxxxxxxxxxxxxxxxxx"})
	a.Append(AuditEvent{At: time.Now(), Owner: "owner", Operation: "after", Result: "padding-xxxxxxxxxxxxxxxxxxxxxxxx"})
	for i := 0; i < 12; i++ {
		a.Append(AuditEvent{Owner: "owner", Operation: "exec"})
	}
	flushAudit(t, a)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b := NewAuditLog(32)
	defer b.Close()
	if err := b.ConfigureFile(path, 1024); err != nil {
		t.Fatal(err)
	}
	events := b.QueryOwner(time.Time{}, "owner")
	if len(events) < 2 {
		t.Fatalf("restart lost rotated history: got %d events", len(events))
	}
}

func flushAudit(t *testing.T, a *AuditLog) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}
