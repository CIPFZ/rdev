package main

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/broker"
)

type fleetBrokenReader struct{ emitted bool }

func (r *fleetBrokenReader) Read(p []byte) (int, error) {
	if !r.emitted {
		r.emitted = true
		return copy(p, `{ "selector": "all", "operation": "job_start" }`), nil
	}
	return 0, errors.New("input interrupted")
}

func TestFleetInputMustBeCompleteAndUnambiguous(t *testing.T) {
	for _, input := range []string{`null`, `{"selector":"all","selector":"alias=a"}`, `{"selector":"all","unknown":"value"}`, `{"job":{"env":{"A":"a","A":"b"}}}`, `{} {}`} {
		var out broker.FleetSpec
		err := readFleetJSON(strings.NewReader(input), 16384, &out)

		if err == nil {
			t.Fatalf("accepted input %s", input)
		}
	}
	for _, reader := range []io.Reader{&fleetBrokenReader{}, strings.NewReader(strings.Repeat(" ", 16385))} {
		var out broker.FleetSpec
		if err := readFleetJSON(reader, 16384, &out); err == nil {
			t.Fatal("accepted incomplete or oversized input")
		}
	}
}

func TestFleetStrictCLIAndStandaloneProcess(t *testing.T) {
	for _, args := range [][]string{
		{"fleet", "status", "plan", "-limit", "33"}, {"fleet", "results", "plan", "-offset", "-1"},
		{"fleet", "execute", "plan", "-digest", "a", "-digest", "b"}, {"fleet", "plan", "-file", "x", "extra"},
		{"fleet", "retry", "plan"}, {"fleet", "inventory-import", "-revision", "18446744073709551616"},
	} {
		if err := validateCLI(args); err == nil {
			t.Fatalf("accepted %q", args)
		}
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		args     []string
		shared   bool
		want     string
		badStdin bool
	}{
		{[]string{"fleet", "list"}, false, "requires RDEV_BROKER_SOCKET", false},
		{[]string{"fleet", "plan", "-file=-"}, true, "read Fleet input:", true},
		{[]string{"fleet", "status", "plan", "-limit", "0"}, true, "-limit must be 1..32", false},
	} {
		cmd := exec.Command(binary, append([]string{"-test.run=^TestCLIContractProcessHelper$", "--"}, tc.args...)...)
		cmd.Dir = t.TempDir()
		cmd.Env = append(os.Environ(), "RDEV_CONTRACT_HELPER=1", "RDEV_BROKER_SOCKET=", "RDEV_PRINCIPAL_TOKEN=")
		if tc.shared {
			cmd.Env = append(cmd.Env, "RDEV_BROKER_SOCKET=/must-not-connect")
		}
		if tc.badStdin {
			dir, err := os.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer dir.Close()
			cmd.Stdin = dir
		}
		output, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(output), tc.want) {
			t.Fatalf("%q: %s, %v", tc.args, output, err)
		}
	}
}
