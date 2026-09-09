package compat

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
)

func TestPublishedPolicyCompatibilityUsesActualReader(t *testing.T) {
	var policyFormat Format
	for _, f := range Current().Formats {
		if f.Name == "broker_policy" {
			policyFormat = f
		}
	}
	if policyFormat.Versioning == "" || policyFormat.Current != 0 {
		t.Fatal("policy must be described as unversioned")
	}
	path := filepath.Join(t.TempDir(), "policy")
	owner := broker.Owner{ClientID: "client", ProjectID: "project"}
	valid, err := json.Marshal(map[string]map[string]bool{owner.Key(): {"future.operation": true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, valid, 0600); err != nil {
		t.Fatal(err)
	}
	p := broker.NewPolicy()
	if err := p.Load(path); err != nil {
		t.Fatalf("legacy unknown grant strings must remain readable: %v", err)
	}
	if !p.Decide(owner.Key(), "future.operation").Allow {
		t.Fatal("reader discarded stored grant")
	}
	if err := broker.ValidateRoute(broker.Request{Owner: owner, Operation: "future.operation"}); err == nil {
		t.Fatal("unknown grant made an unsupported route executable")
	}
	for _, invalid := range []string{`null`, `{"o":null}`, `{"o":{"exec":null}}`, `{"o":{"exec":1}}`, `{"o":{"exec":true,"exec":false}}`, `{"o":{"exec":true},"o":{"exec":false}}`} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if err := p.Load(path); err == nil {
			t.Fatalf("policy reader accepted %s", invalid)
		}
		if !p.Decide(owner.Key(), "future.operation").Allow {
			t.Fatal("invalid policy replaced prior state")
		}
	}
}

func TestPublishedProtocolRangesNegotiateAndReject(t *testing.T) {
	c := Current()
	a := c.Protocols[0].Range
	for _, tc := range []struct {
		peer    proto.ProtocolRange
		version int
		ok      bool
	}{
		{proto.ProtocolRange{Min: 2, Max: 3}, 3, true},
		{proto.ProtocolRange{Min: 2, Max: 2}, 2, true},
		{proto.ProtocolRange{Min: 3, Max: 4}, 3, true},
		{proto.ProtocolRange{Min: 1, Max: 1}, 0, false},
		{proto.ProtocolRange{Min: 4, Max: 4}, 0, false},
		{proto.ProtocolRange{Min: 0, Max: 3}, 0, false},
	} {
		got, ok := proto.NegotiateVersion(a, tc.peer)
		if got != tc.version || ok != tc.ok {
			t.Fatalf("published agent range %+v against %+v = %d/%v", a, tc.peer, got, ok)
		}
	}
	b := c.Protocols[1].Range
	local := proto.BrokerHello{MinVersion: b.Min, Version: b.Max}
	for _, peer := range []proto.BrokerHello{{MinVersion: 1, Version: 1}, {MinVersion: 1, Version: 2}} {
		if err := proto.ValidateBrokerHello(local, peer); err != nil {
			t.Fatal(err)
		}
	}
	for _, peer := range []proto.BrokerHello{{MinVersion: 0, Version: 0}, {MinVersion: 2, Version: 2}, {MinVersion: 2, Version: 1}} {
		if err := proto.ValidateBrokerHello(local, peer); err == nil {
			t.Fatal("accepted unsupported peer", peer)
		}
	}
	shared := c.Protocols[2].Range
	if _, ok := proto.NegotiateVersion(shared, proto.ProtocolRange{Min: 2, Max: 2}); ok {
		t.Fatal("published shared range admits identity-less legacy peer")
	}
	if version, ok := proto.NegotiateVersion(shared, proto.ProtocolRange{Min: 2, Max: 3}); !ok || version != 3 {
		t.Fatal("published shared range rejects compatible peer")
	}
}

func TestPublishedErrorContractIsEnforced(t *testing.T) {
	c := Current()
	for _, d := range c.Errors.Codes {
		e := proto.NewError(d.Code, "", proto.StateNotSent)
		if err := e.Validate(); err != nil {
			t.Fatal(err)
		}
		e.Retryable = !e.Retryable
		if err := e.Validate(); err == nil {
			t.Fatal("accepted changed retry semantics", d.Code)
		}
	}
	e := proto.NewError(proto.CodeInternalFailure, "", proto.StateNotSent)
	e.Code = "future.error"
	if err := e.Validate(); err == nil {
		t.Fatal("accepted unknown error code")
	}
	data, err := json.Marshal(c)
	if err != nil || len(data) == 0 {
		t.Fatal(err)
	}
}
