package mcpsrv

import (
	"reflect"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/compat"
)

// Exercise the official SDK schema and structured result on both servers,
// without credentials or a live socket: compatibility is local build metadata.
func TestCompatSDKBothModes(t *testing.T) {
	c := newTestClient()
	defer c.Close()
	shared, err := NewBroker("/must-not-connect", broker.Owner{ClientID: "compat", ProjectID: "project"})
	if err != nil {
		t.Fatal(err)
	}
	for _, sharedMode := range []bool{false, true} {
		srv := New(c)
		if sharedMode {
			srv = shared
		}
		cs := connectServer(t, srv)
		var out compat.Contract
		if isErr, text := callTool(t, cs, "rdev_compat", map[string]any{}, &out); isErr {
			t.Fatal(text)
		}
		if !reflect.DeepEqual(out, compat.Current()) {
			t.Fatalf("SDK compatibility diverged (shared=%v)", sharedMode)
		}
	}
}
