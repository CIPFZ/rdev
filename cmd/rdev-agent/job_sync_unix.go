//go:build !windows

package main

import (
	"os"
)

func syncJobDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	err = d.Sync()
	closeErr := d.Close()
	if err == nil {
		err = closeErr
	}
	return err
}
