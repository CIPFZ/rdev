package mcpsrv

import (
	"encoding/json"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestTimeoutErrorsHaveSameSDKEnvelopeInBothModes(t *testing.T) {
	for _, shared := range []bool{false, true} {
		srv := New(newTestClient())
		if shared {
			var err error
			srv, err = NewBroker("/missing-socket", broker.Owner{ClientID: "a", ProjectID: "p"})
			if err != nil {
				t.Fatal(err)
			}
		}
		cs := connectServer(t, srv)
		for _, n := range []int{-1, 3601} {
			for _, tool := range []string{"rdev_exec", "rdev_job_start", "rdev_job_wait"} {
				args := map[string]any{"host": "missing-host"}
				switch tool {
				case "rdev_exec":
					args["argv"] = []string{"true"}
					args["timeout_sec"] = n
				case "rdev_job_start":
					args["argv"] = []string{"true"}
					args["resources"] = map[string]any{"wall_timeout_sec": n}
				case "rdev_job_wait":
					args["id"] = "job"
					args["timeout_sec"] = n
				}
				r, err := cs.CallTool(t.Context(), &mcp.CallToolParams{Name: tool, Arguments: args})
				if err != nil {
					t.Fatalf("shared=%t %s protocol error: %v", shared, tool, err)
				}
				data, _ := json.Marshal(r.StructuredContent)
				var envelope proto.ErrorEnvelope
				if json.Unmarshal(data, &envelope) != nil || !r.IsError {
					t.Fatalf("lost typed envelope: %s", data)
				}
				want := proto.CodeInvalidRequest
				if n > 3600 {
					want = proto.CodeLimitExceeded
				}
				if envelope.Code != want || envelope.ExecutionState != proto.StateNotSent || envelope.Retryable {
					t.Fatalf("shared=%t %s timeout=%d: %s", shared, tool, n, data)
				}
			}
		}
	}
}
