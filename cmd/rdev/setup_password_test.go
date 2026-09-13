package main

import (
	"os"
	"testing"
)

func TestReadBootstrapPasswordFD(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := w.WriteString("synthetic-agent-secret\n"); err != nil {
		t.Fatal(err)
	}
	w.Close()
	got, err := readBootstrapPasswordFD(int(r.Fd()))
	if err != nil || got != "synthetic-agent-secret" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestReadBootstrapPasswordFDRejectsOversize(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, _ = w.Write(make([]byte, 4097))
	w.Close()
	if _, err := readBootstrapPasswordFD(int(r.Fd())); err == nil {
		t.Fatal("accepted oversized password payload")
	}
}
