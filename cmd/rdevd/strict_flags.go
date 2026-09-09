package main

import (
	"flag"
	"fmt"
	"strings"
)

// parseSingleFlags retains flag's value syntax and validation while rejecting
// duplicate single-value options and operands before commands have side effects.
// A string beginning with '-' must use -key=-value to avoid swallowing a flag
// when its predecessor is missing a value.
func parseSingleFlags(fs *flag.FlagSet, args []string) error {
	seen := make(map[string]bool)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			if i != len(args)-1 {
				return fmt.Errorf("unexpected positional arguments")
			}
			break
		}
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			return fmt.Errorf("unexpected positional arguments")
		}
		name := strings.TrimPrefix(arg, "-")
		name = strings.TrimPrefix(name, "-")
		name, _, inline := strings.Cut(name, "=")
		f := fs.Lookup(name)
		if f == nil {
			// Preserve the standard help behavior, including flag.ErrHelp.
			if name == "help" || name == "h" {
				return fs.Parse(args)
			}
			return fmt.Errorf("flag provided but not defined: -%s", name)
		}
		if seen[name] {
			return fmt.Errorf("flag -%s may only be supplied once", name)
		}
		seen[name] = true
		isBool, ok := f.Value.(interface{ IsBoolFlag() bool })
		if inline || ok && isBool.IsBoolFlag() {
			continue
		}
		if i+1 == len(args) || args[i+1] == "--" {
			return fmt.Errorf("flag -%s needs a value", name)
		}
		if getter, ok := f.Value.(flag.Getter); ok {
			if _, stringValue := getter.Get().(string); stringValue && strings.HasPrefix(args[i+1], "-") {
				return fmt.Errorf("flag -%s needs a value (use -%s=VALUE for a leading dash)", name, name)
			}
		}
		i++
	}
	return fs.Parse(args)
}
