package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"os"

	"github.com/CIPFZ/rdev/internal/fileedit"
	"github.com/CIPFZ/rdev/internal/proto"
)

func editError(code proto.ErrorCode) error { return proto.NewError(code, "", proto.StateFailed) }

// snapshotBytes bounds reads and rejects in-place changes observed during the
// read. Digest and returned slice always derive from these same exact bytes.
func snapshotBytes(f *os.File) ([]byte, os.FileInfo, error) {
	before, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, nil, editError(proto.CodeInvalidRequest)
	}
	if before.Size() > fileedit.MaxBytes {
		return nil, nil, editError(proto.CodeLimitExceeded)
	}
	b, err := io.ReadAll(io.LimitReader(f, fileedit.MaxBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if len(b) > fileedit.MaxBytes {
		return nil, nil, editError(proto.CodeLimitExceeded)
	}
	after, err := f.Stat()
	if err != nil {
		return nil, nil, err
	}
	if before.Size() != after.Size() || int64(len(b)) != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, nil, editError(proto.CodeEditConflict)
	}
	return b, after, nil
}

func readSnapshot(p *proto.ReadParams, f *os.File, limit int64) (*proto.ReadResult, error) {
	b, _, err := snapshotBytes(f)
	if err != nil {
		return nil, err
	}
	start := min(p.Offset, int64(len(b)))
	end := min(start+limit, int64(len(b)))
	truncation, _ := proto.NewTruncation(int64(len(b))-start, end-start)
	r := &proto.ReadResult{Content: string(b[start:end]), Size: int64(len(b)), EOF: end == int64(len(b)), Truncation: truncation, Digest: fileedit.Digest(b)}
	if !isJSONSafeText(b) {
		r.Content = base64.StdEncoding.EncodeToString(b[start:end])
		r.ContentB64 = true
	}
	return r, nil
}

func editResult(before, after []byte) *proto.EditResult {
	return &proto.EditResult{OldDigest: fileedit.Digest(before), NewDigest: fileedit.Digest(after), BytesBefore: len(before), BytesAfter: len(after), Changed: !bytes.Equal(before, after), Committed: true}
}
