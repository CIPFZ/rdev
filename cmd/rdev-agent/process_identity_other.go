//go:build !linux && !darwin && !windows

package main

import "fmt"

func processIdentity(int) (string, error) {
	return "", fmt.Errorf("process identity is unsupported on this platform")
}
