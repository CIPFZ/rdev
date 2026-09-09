package compat

import (
	"encoding/json"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

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
