package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/CIPFZ/rdev/internal/proto"
)

// Keep the existing interspersed -flag/--flag syntax. Each command declares its
// accepted flags and arity; values are validated before config, stdin or I/O.
type flagSpec struct {
	kind string
	max  int64
}
type commandSpec struct {
	min, max int
	flags    map[string]flagSpec
}
type flagSet struct {
	vals    map[string]string
	bools   map[string]bool
	repeat  map[string][]string
	numbers map[string]int
	pos     []string
}

func schema(name string) commandSpec {
	s := commandSpec{flags: map[string]flagSpec{}}
	add := func(kind string, max int64, names ...string) {
		for _, n := range names {
			s.flags[n] = flagSpec{kind, max}
		}
	}
	switch name {
	case "fleet.plan", "fleet.inventory-update":
		add("string", 0, "file")
	case "fleet.approve", "fleet.execute":
		s.min, s.max = 1, 1
		add("string", 0, "digest")
		if name == "fleet.approve" {
			add("int", 600, "ttl")
		} else {
			add("string", 0, "approval")
		}
	case "fleet.status", "fleet.results":
		s.min, s.max = 1, 1
		add("int", 32, "limit")
		add("int", math.MaxInt32, "offset")
	case "fleet.list":
		add("int", 32, "limit")
		add("int", math.MaxInt32, "offset")
	case "fleet.pause", "fleet.resume", "fleet.cancel", "fleet.reconcile":
		s.min, s.max = 1, 1
	case "fleet.retry":
		s.min, s.max = 2, 129
	case "fleet.inventory-import":
		add("uint", 0, "revision")
	case "fleet.inventory-list":
	case "exec", "job.start":
		s.min, s.max = 1, 1
		add("string", 0, "cwd")
		add("keyvalue", 0, "env")
		add("bool", 0, "no-login")
		if name == "exec" {
			add("int", proto.MaxTimeoutSeconds, "timeout")
		} else {
			add("string", 0, "label")
			add("int", proto.MaxTimeoutSeconds, "wall-timeout")
			add("int", math.MaxInt32, "fds", "job-count")
		}
	case "job.list", "ls":
		s.min, s.max = 1, 1
		if name == "ls" {
			s.max = 2
		}
		add("int", 10000, "limit")
	case "read":
		s.min, s.max = 2, 2
		add("int", proto.AbsoluteReadBytes, "limit")
		add("int", math.MaxInt64-proto.AbsoluteReadBytes, "offset")
	case "write":
		s.min, s.max = 2, 2
		add("mode", 0777, "mode")
		add("bool", 0, "append")
	case "job.status":
		s.min, s.max = 2, 2
	case "job.logs":
		s.min, s.max = 2, 2
		add("string", 0, "grep")
		add("stream", 0, "stream")
		add("int", 1000, "tail")
	case "job.stop":
		s.min, s.max = 2, 2
		add("signal", 0, "signal")
		add("int", proto.MaxTimeoutSeconds, "grace")
	case "job.wait":
		s.min, s.max = 2, 65
		add("bool", 0, "any")
		add("int", proto.MaxTimeoutSeconds, "timeout")
		add("int", 1000, "tail")
	case "job.rm":
		s.min, s.max = 1, 2
		add("int", math.MaxInt64/1_000_000_000, "older-than")
		add("int", math.MaxInt32, "keep-last")
	case "job.events":
		s.min, s.max = 2, 2
		add("string", 0, "stream")
		add("uint", 0, "after")
		add("int", 64, "limit")
	case "sync":
		s.min, s.max = 4, 4
		add("bool", 0, "dry-run", "delete", "confirm-delete", "prepare")
		add("repeat", 0, "exclude")
		add("string", 0, "plan")
		add("symlink", 0, "symlink-policy")
		add("conflict", 0, "conflict-policy")
		add("int", proto.AbsoluteOutputBytes, "max-output-bytes")
	case "hosts.add":
		s.min, s.max = 2, 2
		add("bool", 0, "save", "global", "no-login", "force-agent-upload")
		add("port", 65535, "port")
		add("string", 0, "cwd", "remote-dir")
		add("keyvalue", 0, "env", "secret")
	case "capability.flags":
		add("bool", 0, "refresh")
	case "capability", "support":
		s.min, s.max = 1, 1
		if name == "support" {
			s.min = 0
		}
		add("bool", 0, "refresh")
	case "state.flags":
		add("bool", 0, "dry-run")
	case "state.inspect", "state.migrate", "state.repair":
		s.min, s.max = 1, 1
		if name != "state.inspect" {
			add("bool", 0, "dry-run")
		}
	case "env.inspect":
		s.min, s.max = 1, 1
		add("bool", 0, "refresh")
	case "secrets.set-from-file":
		s.min, s.max = 2, 2
		add("string", 0, "host")
	case "secrets.check":
		s.min, s.max = 2, 2
		add("string", 0, "path")
	case "ping", "hosts.approve-project", "mutation.status", "secret.list":
		s.min, s.max = 1, 1
	case "secret.set", "secret.delete":
		s.min, s.max = 2, 2
	case "secret.set_from_file":
		s.min, s.max = 3, 3
	case "broker.status":
		add("bool", 0, "pool")
	case "serve", "version", "-version", "--version", "help", "-h", "--help", "compat", "hosts", "hosts.list", "hosts.trust", "secrets.list":
	default:
		s.min, s.max = -1, -1
	}
	return s
}

func parseFlags(args []string, command string) (*flagSet, error) {
	spec := schema(command)
	if spec.min < 0 {
		return nil, fmt.Errorf("unknown command %q", command)
	}
	f := &flagSet{vals: map[string]string{}, bools: map[string]bool{}, repeat: map[string][]string{}, numbers: map[string]int{}}
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			f.pos = append(f.pos, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(a, "-") || a == "-" {
			f.pos = append(f.pos, a)
			continue
		}
		key := strings.TrimPrefix(strings.TrimPrefix(a, "-"), "-")
		key, value, inline := strings.Cut(key, "=")
		rule, ok := spec.flags[key]
		if !ok {
			return nil, fmt.Errorf("unknown flag -%s for %s", key, command)
		}
		repeat := rule.kind == "repeat" || rule.kind == "keyvalue"
		if seen[key] && !repeat {
			return nil, fmt.Errorf("duplicate flag -%s", key)
		}
		seen[key] = true
		if rule.kind == "bool" {
			if inline {
				return nil, fmt.Errorf("flag -%s does not take a value", key)
			}
			f.bools[key] = true
			continue
		}
		if !inline {
			if i+1 >= len(args) || args[i+1] == "--" {
				return nil, fmt.Errorf("flag -%s needs a value", key)
			}
			value = args[i+1]
			// Negative numeric values reach range validation. Option-shaped strings
			// require -key=value so a forgotten value cannot swallow another flag.
			if strings.HasPrefix(value, "-") && rule.kind != "int" && rule.kind != "port" && rule.kind != "uint" {
				return nil, fmt.Errorf("flag -%s needs a value (use -%s=value for leading-dash values)", key, key)
			}
			i++
		}
		if value == "" {
			return nil, fmt.Errorf("flag -%s needs a nonempty value", key)
		}
		switch rule.kind {
		case "int", "port", "mode":
			base := 10
			if rule.kind == "mode" {
				base = 8
			}
			n, err := strconv.ParseInt(value, base, 64)
			if err != nil {
				return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
			}
			min := int64(0)
			if rule.kind == "port" {
				min = 1
			}
			if n < 0 && (key == "timeout" || key == "wall-timeout") {
				return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
			}
			if n < min || n > rule.max || int64(int(n)) != n {
				return nil, proto.NewError(proto.CodeLimitExceeded, "", proto.StateNotSent)
			}
			f.numbers[key] = int(n)
		case "uint":
			if _, err := strconv.ParseUint(value, 10, 64); err != nil {
				return nil, fmt.Errorf("invalid -%s integer", key)
			}
		case "keyvalue":
			k, _, ok := strings.Cut(value, "=")
			if !ok || k == "" {
				return nil, fmt.Errorf("-%s expects NAME=VALUE", key)
			}
			for _, old := range f.repeat[key] {
				oldKey, _, _ := strings.Cut(old, "=")
				if k == oldKey {
					return nil, fmt.Errorf("duplicate -%s key", key)
				}
			}
		case "stream":
			if value != "stdout" && value != "stderr" {
				return nil, fmt.Errorf("invalid -stream")
			}
		case "signal":
			if value != "TERM" && value != "KILL" && value != "INT" && value != "HUP" {
				return nil, fmt.Errorf("invalid -signal")
			}
		case "symlink":
			if value != "preserve" && value != "follow" && value != "skip" {
				return nil, fmt.Errorf("invalid -symlink-policy")
			}
		case "conflict":
			if value != "overwrite" && value != "skip" && value != "fail" {
				return nil, fmt.Errorf("invalid -conflict-policy")
			}
		}
		if repeat {
			f.repeat[key] = append(f.repeat[key], value)
		} else {
			f.vals[key] = value
		}
	}
	if len(f.pos) < spec.min || len(f.pos) > spec.max {
		return nil, fmt.Errorf("invalid operands for %s; see rdev help", command)
	}
	if command == "sync" {
		if f.pos[1] != "push" && f.pos[1] != "pull" {
			return nil, fmt.Errorf("sync direction must be push or pull")
		}
		modes := 0
		for _, k := range []string{"dry-run", "prepare", "plan"} {
			if seen[k] {
				modes++
			}
		}
		if modes > 1 || f.bools["confirm-delete"] && !f.bools["delete"] {
			return nil, fmt.Errorf("conflicting sync flags")
		}
	}
	if command == "job.rm" && len(f.pos) == 2 && (seen["older-than"] || seen["keep-last"]) {
		return nil, fmt.Errorf("job ID conflicts with sweep filters")
	}
	if command == "job.events" && seen["after"] && !seen["stream"] {
		return nil, fmt.Errorf("-after requires -stream")
	}
	return f, nil
}
func (f *flagSet) str(k string) string { return f.vals[k] }
func (f *flagSet) num(k string) int    { return f.numbers[k] }

func validateCLI(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("command required")
	}
	name, rest := args[0], args[1:]
	switch name {
	case "job", "hosts", "state", "env", "secrets", "secret", "mutation", "broker", "fleet":
		if len(rest) > 0 {
			name += "." + rest[0]
			rest = rest[1:]
		}
	}
	if name == "exec" || name == "job.start" || name == "secrets.check" {
		flags, argv, err := splitArgv(rest)
		if err != nil {
			return err
		}
		if len(argv) == 0 {
			return fmt.Errorf("no command after --")
		}
		rest = flags
	}
	_, err := parseFlags(rest, name)
	return err
}

func (f *flagSet) env() map[string]string {
	if len(f.repeat["env"]) == 0 {
		return nil
	}
	out := map[string]string{}
	for _, kv := range f.repeat["env"] {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}
	return out
}
