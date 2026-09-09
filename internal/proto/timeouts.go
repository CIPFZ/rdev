package proto

// Public execution budgets. Zero is always a bounded default, never infinity.
// Connection/request contexts may end earlier; a wait budget never kills a job.
const (
	DefaultExecTimeoutSeconds    = 60
	DefaultJobWaitSeconds        = 300
	DefaultJobWallTimeoutSeconds = 3600
	MaxTimeoutSeconds            = 3600
)

func ResolveTimeout(seconds, fallback int) (int, error) {
	if seconds < 0 {
		return 0, NewError(CodeInvalidRequest, "", StateNotSent)
	}
	if seconds > MaxTimeoutSeconds {
		return 0, NewError(CodeLimitExceeded, "", StateNotSent)
	}
	if seconds == 0 {
		seconds = fallback
	}
	return seconds, nil
}

// NormalizeTimeouts returns a private request copy, retaining the caller's
// digest-bound representation. Explicit effective values also enforce defaults
// when talking to a compatible older agent whose zero once meant unbounded.
func NormalizeTimeouts(request *Request) (*Request, error) {
	if request == nil {
		return nil, NewError(CodeInvalidRequest, "", StateNotSent)
	}
	out := *request
	switch out.Op {
	case OpExec:
		if out.Exec == nil {
			return nil, NewError(CodeInvalidRequest, "", StateNotSent)
		}
		p := *out.Exec
		n, err := ResolveTimeout(p.TimeoutSec, DefaultExecTimeoutSeconds)
		if err != nil {
			return nil, err
		}
		p.TimeoutSec = n
		out.Exec = &p
	case OpJobWait:
		if out.Job == nil {
			return nil, NewError(CodeInvalidRequest, "", StateNotSent)
		}
		p := *out.Job
		n, err := ResolveTimeout(p.WaitTimeoutSec, DefaultJobWaitSeconds)
		if err != nil {
			return nil, err
		}
		p.WaitTimeoutSec = n
		out.Job = &p
	case OpJobStart:
		if out.Job == nil {
			return nil, NewError(CodeInvalidRequest, "", StateNotSent)
		}
		p := *out.Job
		r := ResourceEnvelope{}
		if p.Resources != nil {
			r = *p.Resources
		}
		n, err := ResolveTimeout(r.WallTimeoutSec, DefaultJobWallTimeoutSeconds)
		if err != nil {
			return nil, err
		}
		r.WallTimeoutSec = n
		p.Resources = &r
		out.Job = &p
	}
	return &out, nil
}
