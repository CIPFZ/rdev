package mcpsrv

import (
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/support"
)

func TestSupportSDKBothModesAndSharedBoundaries(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	shared, err := NewBroker("/must-not-connect", broker.Owner{ClientID: "sdk", ProjectID: "p"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		shared bool
	}{{"standalone", false}, {"broker", true}} {
		t.Run(tt.name, func(t *testing.T) {
			srv := New(c)
			if tt.shared {
				srv = shared
			}
			cs := connectServer(t, srv)
			var out support.Discovery
			if isErr, text := callTool(t, cs, "rdev_support", map[string]any{}, &out); isErr {
				t.Fatal(text)
			}
			if out.Mode != tt.name || out.RuntimeStatus != "not_requested" || out.SchemaVersion != support.SchemaVersion {
				t.Fatalf("discovery=%+v", out)
			}
			if isErr, _ := callTool(t, cs, "rdev_support", map[string]any{"refresh": true}, nil); !isErr {
				t.Fatal("refresh without target accepted")
			}
			if isErr, _ := callTool(t, cs, "rdev_state", map[string]any{"host": "host", "action": "not-an-action"}, nil); !isErr {
				t.Fatal("invalid state action accepted")
			}
		})
	}
}

func TestSessionSDKIPv6PortCanonicalizationIsAtomic(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	cs := connect(t, c)
	var out SessionOut
	if isErr, text := callTool(t, cs, "rdev_session", map[string]any{"host": "v6", "addr": "alice@[2001:db8::1]:2222"}, &out); isErr {
		t.Fatal(text)
	}
	if len(out.Hosts) != 1 || out.Hosts[0].Addr != "alice@2001:db8::1" || out.Hosts[0].Port != 2222 {
		t.Fatalf("SDK host destination=%+v", out.Hosts)
	}
	if isErr, _ := callTool(t, cs, "rdev_session", map[string]any{"host": "v6", "addr": "bob@[::1]:2222", "port": 2222}, nil); !isErr {
		t.Fatal("SDK accepted embedded and separate port")
	}
	resolved, err := c.Hosts.Resolve("v6")
	if err != nil || resolved.Host.Addr != "alice@2001:db8::1" || resolved.Host.Port != 2222 {
		t.Fatal("invalid SDK address update changed host identity")
	}
}
