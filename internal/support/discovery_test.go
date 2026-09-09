package support

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestDiscoveryDoesNotPromoteProbeToCertification(t *testing.T) {
	d := Discover("broker")
	d.SetRuntime(&proto.CapabilityResult{ProbeVersion: "1", OS: "darwin", Arch: "arm64", Cgroup: true, Rlimit: true, Profile: &proto.ExecutionProfile{Path: "sensitive-path", Home: "private-home"}})
	if d.Runtime.Platform.Validation != "build_only" {
		t.Fatalf("Darwin runtime claim=%+v", d.Runtime.Platform)
	}
	if d.Runtime.Controls[0].Status != "unsupported" {
		t.Fatal("detected cgroup incorrectly advertised as CPU enforcement")
	}
	data, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "sensitive-path") || strings.Contains(string(data), "private-home") {
		t.Fatal("support exposed the execution profile")
	}
	for _, frontend := range d.Frontends {
		if frontend.Name == "host_session_edit" && (frontend.Status != "unsupported" || frontend.Alternative == "") {
			t.Fatal("missing shared frontend boundary")
		}
	}
	for _, p := range d.Local {
		if p.OS == "linux" && p.Arch == "amd64" && p.Validation != "runtime_verified" {
			t.Fatalf("Linux baseline stale=%+v", p)
		}
		if p.OS == "darwin" && p.Validation == "runtime_verified" {
			t.Fatal("deferred macOS marked runtime verified")
		}
	}
}
