package transport

import (
	"context"
	"errors"
	"github.com/CIPFZ/rdev/internal/artifact"
)

type releaseObserverKey struct{}
type ReleaseEvent struct {
	Decision artifact.Decision
	Result   string
}

// WithReleaseObserver records actual admission/install attempts only. The
// callback receives no raw peer text, command, path, policy content or key.
func WithReleaseObserver(ctx context.Context, observe func(ReleaseEvent)) context.Context {
	return context.WithValue(ctx, releaseObserverKey{}, observe)
}
func observeRelease(ctx context.Context, d artifact.Decision, err error) {
	fn, _ := ctx.Value(releaseObserverKey{}).(func(ReleaseEvent))
	if fn == nil {
		return
	}
	result := "ready"
	if err != nil {
		result = "rejected"
		var ambiguous *AgentInstallAmbiguousError
		var committed *AgentInstallCommittedError
		if errors.As(err, &ambiguous) {
			result = "ambiguous"
		} else if errors.As(err, &committed) {
			result = "committed"
		}
	}
	fn(ReleaseEvent{d, result})
}

func HasReleaseObserver(ctx context.Context) bool {
	fn, _ := ctx.Value(releaseObserverKey{}).(func(ReleaseEvent))
	return fn != nil
}
