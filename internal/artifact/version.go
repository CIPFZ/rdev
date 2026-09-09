package artifact

import (
	"errors"
	"strconv"
	"strings"
)

// CompareVersions orders the deliberately small release grammar: numeric
// major.minor.patch, then dev.N < beta.N < stable. Commit times never order releases.
func CompareVersions(a, b string) (int, error) {
	parse := func(s string) ([5]uint64, error) {
		var n [5]uint64
		m := versionPattern.FindStringSubmatch(s)
		if m == nil {
			return n, errors.New("invalid release version")
		}
		for i := 0; i < 3; i++ {
			v, e := strconv.ParseUint(m[i+1], 10, 32)
			if e != nil {
				return n, e
			}
			n[i] = v
		}
		n[3] = 2
		if m[5] != "" {
			if m[5] == "dev" {
				n[3] = 0
			} else {
				n[3] = 1
			}
			v, e := strconv.ParseUint(m[6], 10, 32)
			if e != nil {
				return n, e
			}
			n[4] = v
		}
		return n, nil
	}
	x, e := parse(a)
	if e != nil {
		return 0, e
	}
	y, e := parse(b)
	if e != nil {
		return 0, e
	}
	for i := range x {
		if x[i] < y[i] {
			return -1, nil
		}
		if x[i] > y[i] {
			return 1, nil
		}
	}
	return 0, nil
}

// TargetKey names configured connection identity, excluding local aliases.
func TargetKey(addr string, port int, namespace string) string {
	return Hash([]byte(strings.Join([]string{addr, strconv.Itoa(port), namespace}, "\x00")))
}
