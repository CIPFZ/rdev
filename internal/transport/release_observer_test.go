package transport

import (
	"context"
	"errors"
	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/proto"
	"testing"
)

func TestReleaseRejectionAuditBindsActualCandidateWithoutSSH(t *testing.T) {
	var events []ReleaseEvent
	ctx := WithReleaseObserver(context.Background(), func(e ReleaseEvent) { events = append(events, e) })
	data := []byte("candidate-only-fixture")
	bin := &AgentBinary{Data: data, SHA256: artifact.Hash(data), Authorize: func(context.Context, Host) (artifact.Decision, error) {
		return artifact.Decision{}, proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent)
	}}
	err := (&Conn{}).ensureAgent(ctx, bin, "")
	if err == nil || len(events) != 1 || events[0].Result != "rejected" || events[0].Decision.Digest != artifact.Hash(data) {
		t.Fatalf("missing bounded release decision: %+v %v", events, err)
	}
	for _, test := range []struct {
		err  error
		want string
	}{{nil, "ready"}, {errors.New("raw secret diagnostics"), "rejected"}, {&AgentInstallAmbiguousError{}, "ambiguous"}, {&AgentInstallCommittedError{}, "committed"}} {
		observeRelease(ctx, artifact.Decision{}, test.err)
		if events[len(events)-1].Result != test.want {
			t.Fatal("installation outcome lost")
		}
	}
}
