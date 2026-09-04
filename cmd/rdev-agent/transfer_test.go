package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestTransferStagesResumesAndCommitsAtomically(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	data := []byte("new content across chunks")
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	id := "op_0123456789abcdef"
	first := base64.StdEncoding.EncodeToString(data[:8])
	r, err := doTransferChunk(&proto.WriteParams{Path: target, TransferID: id, Offset: 0, TotalSize: int64(len(data)), Digest: digest, Content: first, ContentB64: true})
	if err != nil || r.Offset != 8 || r.Committed {
		t.Fatalf("first chunk = %#v, err=%v", r, err)
	}
	got, _ := os.ReadFile(target)
	if string(got) != "old" {
		t.Fatalf("destination changed before commit: %q", got)
	}
	second := base64.StdEncoding.EncodeToString(data[8:])
	r, err = doTransferChunk(&proto.WriteParams{Path: target, TransferID: id, Offset: 8, TotalSize: int64(len(data)), Digest: digest, Content: second, ContentB64: true, Final: true})
	if err != nil || !r.Committed {
		t.Fatalf("final chunk = %#v, err=%v", r, err)
	}
	got, _ = os.ReadFile(target)
	if string(got) != string(data) {
		t.Fatalf("committed data = %q", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ".rdev-transfer-"+id+".part")); !os.IsNotExist(err) {
		t.Fatalf("staging file remains: %v", err)
	}
}

func TestTransferRejectsDigestMismatchWithoutPublishing(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out")
	_ = os.WriteFile(target, []byte("old"), 0o600)
	_, err := doTransferChunk(&proto.WriteParams{Path: target, TransferID: "op_0123456789abcdef", Offset: 0, TotalSize: 3, Digest: hex.EncodeToString(make([]byte, 32)), Content: "bad", Final: true})
	if err == nil {
		t.Fatal("digest mismatch accepted")
	}
	got, _ := os.ReadFile(target)
	if string(got) != "old" {
		t.Fatalf("destination changed: %q", got)
	}
}
