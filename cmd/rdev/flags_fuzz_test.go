package main

import (
	"reflect"
	"strings"
	"testing"
)

func FuzzCLIArguments(f *testing.F) {
	for _, s := range []string{"exec\x00host\x00--\x00printf\x00--timeout=-1", "sync\x00host\x00push\x00--\x00-leading\x00/tmp/out", "job\x00wait\x00host\x00id\x00--timeout=999999999999999999999", "fleet\x00execute\x00id\x00--digest=x\x00--digest=y"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 64<<10 {
			t.Skip()
		}
		args := strings.Split(input, "\x00")
		before := append([]string(nil), args...)
		_ = validateCLI(args)
		if !reflect.DeepEqual(args, before) {
			t.Fatal("validation mutated argv")
		}
		// Every arbitrary argument, including option-shaped data, stays byte
		// exact after the exec delimiter.
		wire := append([]string{"host", "--"}, args...)
		_, argv, err := splitArgv(wire)
		if err != nil || !reflect.DeepEqual(argv, args) {
			t.Fatal("exec argv forwarding changed bytes")
		}
	})
}
