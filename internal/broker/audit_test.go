package broker

import (
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
