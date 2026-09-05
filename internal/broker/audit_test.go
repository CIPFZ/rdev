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
