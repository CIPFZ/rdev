package main

import (
	"os"
	"syscall"
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
	fd, err := dupHigh(int(r.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	got, err := readBootstrapPasswordFD(fd)
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
	fd, err := dupHigh(int(r.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if _, err := readBootstrapPasswordFD(fd); err == nil {
		t.Fatal("accepted oversized password payload")
	}
}

func dupHigh(src int) (int, error) {
	return syscall.Dup2(src, minAgentPasswordFD+1)
}

func TestReadBootstrapPasswordFDRejectsRuntimeDescriptors(t *testing.T) {
	if _, err := readBootstrapPasswordFD(3); err == nil {
		t.Fatal("accepted a low descriptor reserved by the Go runtime")
	}
}
