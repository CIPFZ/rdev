package main

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/transport"
)

func TestStrictCLIInputs(t *testing.T) {
	for _, args := range [][]string{
		{"exec", "dev", "-typo", "1", "--", "true"}, {"exec", "dev", "-timeout", "oops", "--", "true"},
		{"exec", "dev", "-timeout", "9999999999999999999999", "--", "true"}, {"exec", "dev", "-timeout", "-1", "--", "true"},
		{"exec", "dev", "-timeout", "3601", "--", "true"}, {"exec", "dev", "-timeout", "infinite", "--", "true"},
		{"exec", "dev", "-timeout", "1", "--timeout", "1", "--", "true"}, {"exec", "dev", "-no-login", "--no-login", "--", "true"},
		{"exec", "dev", "-cwd", "-timeout", "1", "--", "true"}, {"exec", "dev", "extra", "--", "true"},
		{"exec", "dev", "-cwd", "--", "true"}, {"write", "dev", "p", "-mode", "1000"}, {"write", "dev", "p", "-mode", "999"},
		{"read", "dev", "p", "-offset", "-1"}, {"read", "dev", "p", "-limit", "999999999"}, {"read", "dev", "p", "extra"},
		{"hosts", "add", "dev", "h", "-port", "0"}, {"hosts", "add", "dev", "h", "-port", "65536"},
		{"hosts", "add", "dev", "h", "-env", "K=1", "-env", "K=2"}, {"hosts", "add", "dev", "h", "-secret", "K=p", "-secret", "K=p"},
		{"sync", "dev", "push", "a", "b", "-dry-run", "-prepare"}, {"sync", "dev", "push", "a", "b", "-confirm-delete"},
		{"sync", "dev", "push", "-leading", "b"}, {"job", "rm", "dev", "id", "-keep-last", "1"},
		{"job", "rm", "dev", "-older-than", "9223372037"}, {"job", "wait", "dev", "id", "-timeout", "-1"},
		{"job", "events", "dev", "id", "-after", "1"}, {"job", "start", "dev", "-wall-timeout", "3601", "--", "true"},
		{"job", "start", "dev", "-timeout", "1", "--", "true"}, {"job", "logs", "dev", "id", "-tail", "1001"},
		{"serve", "-unknown"}, {"ping", "dev", "extra"}, {"version", "extra"}, {"support", "-typo"}, {"help", "-typo"},
		{"hosts", "list", "extra"}, {"hosts", "trust", "extra"}, {"secrets", "list", "extra"}, {"job", "status", "dev", "id", "-unknown"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			if err := validateCLI(args); err == nil {
				t.Fatalf("accepted invalid invocation %q", args)
			}
		})
	}
	for _, args := range [][]string{
		{"exec", "dev", "--timeout=0", "--", "printf", "--", "-a", "雪", ""},
		{"exec", "-cwd=-leading", "dev", "--", "true"}, {"job", "start", "dev", "-wall-timeout", "0", "--", "true"},
		{"sync", "dev", "push", "-exclude", "one", "-exclude", "two", "--", "-leading local 雪", "remote"},
		{"hosts", "add", "dev", "h", "-env", "A=1", "-env", "B=2", "-secret", "s=p"},
		{"job", "rm", "dev", "-older-than", "60", "-keep-last", "2"}, {"read", "dev", "--", "-file"},
	} {
		if err := validateCLI(args); err != nil {
			t.Fatalf("valid invocation %q: %v", args, err)
		}
	}
}
func TestDelimiterPreservesOperands(t *testing.T) {
	argv := []string{"printf", "--", "-leading", "", "space here", "雪", "$(touch sentinel)"}
	_, got, err := splitArgv(append([]string{"dev", "--"}, argv...))
	if err != nil || !reflect.DeepEqual(got, argv) {
		t.Fatalf("argv changed %q %v", got, err)
	}
	opts, err := parseSyncOptions([]string{"dev", "push", "-exclude", "one", "-exclude", "two", "--", "-leading local 雪", "remote space 雪"})
	if err != nil || opts.Local != "-leading local 雪" || opts.Remote != "remote space 雪" || !reflect.DeepEqual(opts.Exclude, []string{"one", "two"}) {
		t.Fatalf("sync operands changed %+v %v", opts, err)
	}
}

type faultInput struct {
	data string
	err  error
}

func (r *faultInput) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, r.err
}
func TestStdinFailureCannotDispatchPartialWrite(t *testing.T) {
	injected := errors.New("injected read failure")
	c := client.New(func(string, string) (*transport.AgentBinary, error) {
		t.Fatal("attempted SSH after failed stdin")
		return nil, nil
	})
	defer c.Close()
	for _, data := range []string{"", "partial business content"} {
		for _, shared := range []bool{false, true} {
			input := &faultInput{data, injected}
			var err error
			if shared {
				err = brokerWriteInput(t.Context(), []string{"host", "target"}, input)
			} else {
				err = cmdWriteInput(t.Context(), c, []string{"host", "target"}, input)
			}
			if !errors.Is(err, injected) {
				t.Fatalf("shared=%t data=%q: %v", shared, data, err)
			}
		}
		got, err := readAllInput(&faultInput{data, injected})
		if got != "" || !errors.Is(err, injected) {
			t.Fatalf("partial input returned %q %v", got, err)
		}
	}
	for _, data := range []string{"", "complete"} {
		got, err := readAllInput(&faultInput{data, io.EOF})
		if got != data || err != nil {
			t.Fatalf("EOF %q %v", got, err)
		}
	}
	for _, extra := range []int{0, 1} {
		data := strings.Repeat("x", int(proto.AbsoluteRequestFrameBytes)+extra)
		got, err := readAllInput(strings.NewReader(data))
		if extra == 0 {
			if err != nil || got != data {
				t.Fatal("exact input cap rejected")
			}
		} else if err == nil || got != "" {
			t.Fatal("oversized input returned content")
		}
	}
}
