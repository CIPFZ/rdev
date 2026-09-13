package main

import (
	"os"
	"testing"
)

func TestReadBootstrapPasswordRejectsPipeWithoutReading(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if _, err := readBootstrapPassword(r, os.Stdout); err == nil {
		t.Fatal("pipe input unexpectedly accepted")
	}
}
