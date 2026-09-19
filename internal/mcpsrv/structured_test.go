package mcpsrv

import (
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestParseGitStatus(t *testing.T) {
	out := parseGitStatus(&proto.ExecResult{ExitCode: 0, Stdout: "## main...origin/main [ahead 2, behind 1]\n M cmd/rdev/main.go\n?? new.txt\n"})
	if out.Branch != "main" || out.Ahead != 2 || out.Behind != 1 || len(out.Entries) != 2 || out.Clean {
		t.Fatalf("unexpected git status: %+v", out)
	}
}

func TestListenerPort(t *testing.T) {
	for input, want := range map[string]int{"0.0.0.0:8080": 8080, "[::]:443": 443, "*:22": 22, "LISTEN": 0} {
		if got := listenerPort(input); got != want {
			t.Errorf("listenerPort(%q)=%d, want %d", input, got, want)
		}
	}
}
