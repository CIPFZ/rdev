package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestCLIContractProcessHelper(t *testing.T) {
	if os.Getenv("RDEV_CONTRACT_HELPER") == "" {
		return
	}
	for i, a := range os.Args {
		if a == "--" {
			os.Args = append([]string{"rdev"}, os.Args[i+1:]...)
			main()
			os.Exit(0)
		}
	}
	os.Exit(99)
}
func TestCLIProcessRejectsStdinAndPreservesErrorCodes(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, shared := range []bool{false, true} {
		for _, test := range []struct {
			args       []string
			inputError bool
			want       string
		}{
			{[]string{"sync", "host", "push", "a", "b", "-max-output-bytes", "-1"}, false, "code=resource.limit_exceeded"},
			{[]string{"exec", "host", "-timeout", "-1", "--", "true"}, false, "code=request.invalid"},
			{[]string{"exec", "host", "--unknown=secret-marker-value", "--", "true"}, false, "unknown flag -unknown"},
			{[]string{"write", "host", "target"}, true, "read stdin:"},
		} {
			args := append([]string{"-test.run=^TestCLIContractProcessHelper$", "--"}, test.args...)
			cmd := exec.Command(binary, args...)
			cmd.Dir = t.TempDir()
			cmd.Env = append(os.Environ(), "RDEV_CONTRACT_HELPER=1", "HOME="+t.TempDir(), "RDEV_BROKER_SOCKET=", "RDEV_PRINCIPAL_TOKEN=")
			if shared {
				cmd.Env = append(cmd.Env, "RDEV_BROKER_SOCKET=/does-not-exist")
			}
			if test.inputError {
				dir, err := os.Open(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer dir.Close()
				cmd.Stdin = dir
			}
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), test.want) || strings.Contains(string(output), "secret-marker-value") {
				t.Fatalf("shared=%t args=%q output=%s err=%v", shared, test.args, output, err)
			}
		}
	}
}
