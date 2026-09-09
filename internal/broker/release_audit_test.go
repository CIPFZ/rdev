package broker

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestReleaseAuditBoundsFieldsAndKeepsOwnerDecision(t *testing.T) {
	log := NewAuditLog(8)
	owner := Owner{ClientID: "release", ProjectID: "test"}
	good := &ReleaseAudit{Version: "1.2.3", Digest: strings.Repeat("a", 64), Channel: "stable", Result: "committed"}
	log.Append(AuditEvent{Owner: owner.Key(), Operation: "ping", Decision: "release_policy", Result: "committed", Release: good})
	got := log.QueryOwner(time.Time{}, owner.Key())
	if len(got) != 1 || got[0].Decision != "release_policy" || got[0].Result != "committed" || got[0].Release == nil || *got[0].Release != *good {
		t.Fatalf("release correlation lost: %+v", got)
	}
	bad := &ReleaseAudit{Version: "SECRET_PAYLOAD", Digest: "SECRET_PAYLOAD", Channel: "SECRET_PAYLOAD", Result: "SECRET_PAYLOAD"}
	log.Append(AuditEvent{Owner: owner.Key(), Release: bad})
	raw, _ := json.Marshal(log.QueryOwner(time.Time{}, owner.Key()))
	if strings.Contains(string(raw), "SECRET_PAYLOAD") {
		t.Fatal("arbitrary release diagnostics leaked")
	}
	if rows := log.QueryOwner(time.Time{}, (Owner{ClientID: "release", ProjectID: "other"}).Key()); len(rows) != 0 {
		t.Fatal("release audit crossed owner")
	}
}
