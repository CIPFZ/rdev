//go:build linux

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// processIdentity returns Linux's monotonic process start-time tick count
// from /proc/<pid>/stat. It remains unique for a PID across exit/reuse even
// when wall-clock time moves backwards.
func processIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid")
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	line := string(b)
	close := strings.LastIndex(line, ")")
	if close < 0 || close+2 >= len(line) {
		return "", fmt.Errorf("malformed process stat")
	}
	fields := strings.Fields(line[close+2:]) // state is field 3
	if len(fields) <= 19 {
		return "", fmt.Errorf("malformed process stat")
	}
	if _, err := strconv.ParseUint(fields[19], 10, 64); err != nil {
		return "", err
	}
	return fields[19], nil
}
