package agentinstall

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/state"
)

type Record struct {
	SchemaVersion  int               `json:"schema_version"`
	Phase          string            `json:"phase"`
	Candidate      artifact.Decision `json:"candidate"`
	PreviousDigest string            `json:"previous_digest,omitempty"`
	OwnerPID       int               `json:"owner_pid"`
	StartedAt      time.Time         `json:"started_at"`
}
type Error struct {
	State string
	Stage string
	Cause error
}

func (e *Error) Error() string {
	return fmt.Sprintf("agent transaction %s at %s: %v", e.State, e.Stage, e.Cause)
}
func (e *Error) Unwrap() error { return e.Cause }

func validateRecord(r Record) error {
	if r.SchemaVersion != 1 || r.OwnerPID <= 0 || r.StartedAt.IsZero() || !validDigest(r.Candidate.Digest) || (r.PreviousDigest != "" && !validDigest(r.PreviousDigest)) {
		return errors.New("invalid upgrade record")
	}
	switch r.Phase {
	case "prepared", "verified", "switching", "committed", "rolled_back":
		return nil
	}
	return errors.New("unknown upgrade phase")
}

func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'f') && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func Health(ctx context.Context, binary, root string) error { return health(ctx, binary, root, true) }

func health(ctx context.Context, binary, root string, candidate bool) error {
	if e := state.CheckCompatible(root); e != nil {
		return e
	}
	report, e := state.Inspect(root)
	if e != nil {
		return e
	}
	for _, f := range report.Findings {
		if f.Kind != "manifest_missing" {
			return errors.New("state readiness inspection failed")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-state", root)
	input, e := cmd.StdinPipe()
	if e != nil {
		return e
	}
	output, e := cmd.StdoutPipe()
	if e != nil {
		return e
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	defer func() { input.Close(); cancel(); _ = cmd.Wait() }()
	req := proto.Request{ID: "rdev-upgrade-health", Op: proto.OpPing, Hello: &proto.HelloParams{MinVersion: proto.TypedProtocolVersion, MaxVersion: proto.Version, Features: proto.SupportedFeatures()}}
	if e = json.NewEncoder(input).Encode(req); e != nil {
		return e
	}
	line, e := bufio.NewReader(io.LimitReader(output, 64<<10)).ReadBytes('\n')
	if e != nil {
		return e
	}
	var resp proto.Response
	if e = json.Unmarshal(line, &resp); e != nil {
		return e
	}
	if resp.ID != req.ID || !resp.OK || resp.Ping == nil || resp.Ping.OS != runtime.GOOS || resp.Ping.Arch != runtime.GOARCH {
		return errors.New("candidate health identity mismatch")
	}
	p := resp.Ping
	min := p.MinVersion
	if min == 0 {
		min = p.Version
	}
	if _, ok := proto.NegotiateVersion(proto.ProtocolRange{Min: proto.TypedProtocolVersion, Max: proto.Version}, proto.ProtocolRange{Min: min, Max: p.Version}); !ok {
		return errors.New("candidate protocol incompatible")
	}
	required := proto.SupportedFeatures()
	if !candidate {
		required = nil
	} // Old agents need a valid handshake, not future features.
	for _, feature := range required {
		found := false
		for _, f := range p.Features {
			if f == feature {
				found = true
			}
		}
		if !found {
			return errors.New("candidate required feature missing")
		}
	}
	return nil
}

func Marker(err error) string {
	var e *Error
	if errors.As(err, &e) {
		switch e.State {
		case "committed":
			return "RDEV_AGENT_INSTALL_COMMITTED:" + e.Stage
		case "ambiguous":
			return "RDEV_AGENT_INSTALL_AMBIGUOUS:" + e.Stage
		}
	}
	if e != nil {
		return "RDEV_AGENT_INSTALL_NOT_SENT:" + e.Stage
	}
	return "RDEV_AGENT_INSTALL_NOT_SENT:arguments"
}

func DecodeDecision(raw string) (artifact.Decision, error) {
	var d artifact.Decision
	e := artifact.Decode([]byte(raw), &d)
	if e == nil && (!validDigest(d.Digest) || strings.ContainsAny(d.Channel, "\r\n")) {
		e = errors.New("invalid install decision")
	}
	return d, e
}
