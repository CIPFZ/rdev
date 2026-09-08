package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/secrets"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestPrincipalSecretReaderKeepsRawValueAndExactIdentity(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	if err := c.Hosts.Add(transport.Host{Name: "h", Addr: "remote.invalid"}); err != nil {
		t.Fatal(err)
	}
	const value = "previously-registered-credential"
	if err := c.Secrets.Set(secrets.OutputKey("existing"), value); err != nil {
		t.Fatal(err)
	}
	var callers []string
	dials := 0
	fail := false
	c.dial = func(_ context.Context, h transport.Host, _ AgentLookup) (remoteConnection, error) {
		dials++
		return &fakeRemoteConn{host: h, handler: func(req *proto.Request) (*proto.Response, error) {
			if req.Op != proto.OpReadFile || req.Read.Limit != maxSecretFileBytes+1 || proto.ValidateOperationID(req.OperationID) != nil {
				t.Fatal("secret source read lost bounds or operation identity")
			}
			callers = append(callers, req.ClientID)
			if fail {
				return nil, errors.New("unknown-prospective-secret")
			}
			return &proto.Response{OK: true, Read: &proto.ReadResult{Content: value, Size: int64(len(value)), EOF: true}}, nil
		}}, nil
	}
	target, _ := c.ProtocolTargetIdentity("h")
	for _, project := range []string{"alpha", "beta"} {
		got, err := c.ReadPrincipalSecret(t.Context(), "h", target, "same-client", project, "secret-path")
		if err != nil || got != value {
			t.Fatal("raw repeat import was redacted before binding")
		}
	}
	if len(callers) != 2 || callers[0] != proto.PrincipalID("same-client", "alpha") || callers[1] != proto.PrincipalID("same-client", "beta") || dials != 2 {
		t.Fatal("source requests lost project identity or shared bulk pool")
	}
	if _, err := c.ReadPrincipalSecret(t.Context(), "h", "changed-target", "same-client", "alpha", "secret-path"); err == nil || len(callers) != 2 {
		t.Fatal("wrong target reached source")
	}
	fail = true
	if _, err := c.ReadPrincipalSecret(t.Context(), "h", target, "same-client", "alpha", "secret-path"); err == nil || strings.Contains(err.Error(), "unknown-prospective-secret") {
		t.Fatal("source failure exposed prospective credential")
	}
}
