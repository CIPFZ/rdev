package client

import (
	"context"
	"errors"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestTimeoutDefaultsAndBoundsBeforeConnection(t *testing.T) {
	for _, n := range []int{-1, 3601} {
		c := newTestClient()
		c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
			t.Fatal("invalid timeout dialed SSH")
			return nil, nil
		}
		if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}, TimeoutSec: n}); err == nil {
			t.Fatal("invalid exec timeout accepted")
		}
		if _, err := c.JobWait(t.Context(), JobWaitOptions{Host: "dev", ID: "job", TimeoutSec: n}); err == nil {
			t.Fatal("invalid wait timeout accepted")
		}
		if _, err := c.JobStart(t.Context(), JobStartOptions{Host: "dev", Argv: []string{"true"}, Resources: &proto.ResourceEnvelope{WallTimeoutSec: n}}); err == nil {
			t.Fatal("invalid job wall accepted")
		}
		c.Close()
	}
	for _, n := range []int{0, 1, 3600} {
		c := newTestClient()
		c.Hosts.Add(transport.Host{Name: "dev", Addr: "user@host"})
		conn := &fakeRemoteConn{host: transport.Host{Name: "dev", Addr: "user@host"}}
		c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) { return conn, nil }
		conn.handler = func(req *proto.Request) (*proto.Response, error) {
			expected := n
			var got int
			r := &proto.Response{OK: true, OperationID: req.OperationID, Type: proto.EventFinal, Seq: 1, Terminal: true, Execution: proto.StateCompleted}
			switch req.Op {
			case proto.OpExec:
				if n == 0 {
					expected = 60
				}
				got = req.Exec.TimeoutSec
				r.Exec = &proto.ExecResult{}
			case proto.OpJobWait:
				if n == 0 {
					expected = 300
				}
				got = req.Job.WaitTimeoutSec
				r.Job = &proto.JobResult{Info: &proto.JobInfo{ID: "job", State: proto.JobRunning}}
			case proto.OpJobStart:
				if n == 0 {
					expected = 3600
				}
				got = req.Job.Resources.WallTimeoutSec
				r.Job = &proto.JobResult{Info: &proto.JobInfo{}}
				if req.Job.Spec.TimeoutSec != 0 {
					t.Fatal("foreground default applied to detached job spec")
				}
			default:
				t.Fatalf("unexpected op %s", req.Op)
			}
			if got != expected {
				t.Fatalf("%s wire timeout=%d want=%d", req.Op, got, expected)
			}
			return r, nil
		}
		if _, err := c.Exec(t.Context(), ExecOptions{Host: "dev", Argv: []string{"true"}, TimeoutSec: n}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.JobWait(t.Context(), JobWaitOptions{Host: "dev", ID: "job", TimeoutSec: n}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.JobStart(t.Context(), JobStartOptions{Host: "dev", Argv: []string{"true"}, Resources: &proto.ResourceEnvelope{WallTimeoutSec: n}}); err != nil {
			t.Fatal(err)
		}
		c.Close()
	}
}

type oldResourcePeer struct {
	*fakeRemoteConn
	version int
}

func (c *oldResourcePeer) NegotiatedVersion() int { return c.version }
func (c *oldResourcePeer) SupportsFeature(f proto.Feature) bool {
	return f != proto.FeatureJobResourceEnvelope && proto.IsKnownFeature(f)
}
func TestJobStartRejectsPeerWithoutEnforcedEnvelope(t *testing.T) {
	for _, version := range []int{2, 3} {
		c := newTestClient()
		c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@host"})
		raw := &fakeRemoteConn{host: transport.Host{Name: "dev", Addr: "u@host"}}
		c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
			return &oldResourcePeer{raw, version}, nil
		}
		_, err := c.JobStart(t.Context(), JobStartOptions{Host: "dev", Argv: []string{"true"}})
		var e *proto.ErrorEnvelope
		if !errors.As(err, &e) || e.Code != proto.CodeUnsupportedFeature || e.ExecutionState != proto.StateNotSent {
			t.Fatalf("v%d unbounded peer accepted: %v", version, err)
		}
		if len(raw.ops) != 0 {
			t.Fatalf("v%d job was sent: %v", version, raw.ops)
		}
		if _, err := c.Ping(t.Context(), "dev"); err != nil {
			t.Fatalf("v%d common operation lost: %v", version, err)
		}
		c.Close()
	}
}

func TestCapabilityFeaturesComeFromActualNegotiation(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	c.Hosts.Add(transport.Host{Name: "dev", Addr: "u@host"})
	raw := &fakeRemoteConn{host: transport.Host{Name: "dev", Addr: "u@host"}}
	raw.handler = func(req *proto.Request) (*proto.Response, error) {
		return &proto.Response{OK: true, OperationID: req.OperationID, Terminal: true, Execution: proto.StateCompleted, Capability: &proto.CapabilityResult{ProbeVersion: "1", Features: proto.SupportedFeatures()}}, nil
	}
	c.dial = func(context.Context, transport.Host, AgentLookup) (remoteConnection, error) {
		return &oldResourcePeer{raw, 3}, nil
	}
	cap, err := c.CapabilityProbe(t.Context(), "dev", true)
	if err != nil {
		t.Fatal(err)
	}
	for _, feature := range cap.Features {
		if feature == proto.FeatureJobResourceEnvelope {
			t.Fatal("payload overrode negotiated feature refusal")
		}
	}
	// Returned snapshots cannot modify a cached discovery result.
	if len(cap.Features) == 0 {
		t.Fatal("missing negotiated common features")
	}
	cap.Features[0] = proto.FeatureJobResourceEnvelope
	again, err := c.CapabilityProbe(t.Context(), "dev", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, feature := range again.Features {
		if feature == proto.FeatureJobResourceEnvelope {
			t.Fatal("caller mutated cached negotiated features")
		}
	}
}
