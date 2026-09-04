package main

// Resumable file transfer primitives. The wire still uses write_file so older
// agents reject the optional fields safely; a transfer is staged beside the
// destination and published with rename only after a complete digest check.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/CIPFZ/rdev/internal/proto"
)

const maxTransferBytes int64 = 1 << 30

var transferLocks sync.Map

type transferMeta struct {
	Path      string `json:"path"`
	TotalSize int64  `json:"total_size"`
	Digest    string `json:"digest"`
	Mode      uint32 `json:"mode,omitempty"`
}

func doTransferChunk(p *proto.WriteParams) (*proto.WriteResult, error) {
	if p == nil || p.TransferID == "" || proto.ValidateOperationID(p.TransferID) != nil {
		return nil, invalidRequestError("invalid transfer id")
	}
	if p.Path == "" || p.Offset < 0 || p.TotalSize < 0 || p.TotalSize > maxTransferBytes || p.Offset > p.TotalSize {
		return nil, invalidRequestError("invalid transfer range")
	}
	if p.Append {
		return nil, invalidRequestError("transfer cannot append")
	}
	if len(p.Digest) != 64 {
		return nil, invalidRequestError("transfer digest required")
	}
	if _, err := hex.DecodeString(p.Digest); err != nil {
		return nil, invalidRequestError("invalid transfer digest")
	}
	data := []byte(p.Content)
	if p.ContentB64 {
		var err error
		data, err = decodeTransferB64(p.Content)
		if err != nil {
			return nil, invalidRequestError("invalid base64 content")
		}
	}
	if int64(len(data)) > p.TotalSize-p.Offset {
		return nil, invalidRequestError("transfer chunk exceeds declared size")
	}
	path := expandHome(p.Path)
	if !filepath.IsAbs(path) {
		path, _ = filepath.Abs(path)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if st, err := os.Lstat(dir); err != nil || st.Mode()&os.ModeSymlink != 0 || !st.IsDir() {
		return nil, invalidRequestError("transfer parent must be a directory")
	}
	if st, err := os.Lstat(path); err == nil && (st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular()) {
		return nil, invalidRequestError("transfer target must be a regular file")
	}
	base := filepath.Join(dir, ".rdev-transfer-"+p.TransferID)
	part, metaPath := base+".part", base+".json"
	mu := transferLock(path)
	mu.Lock()
	defer mu.Unlock()
	meta := transferMeta{Path: path, TotalSize: p.TotalSize, Digest: p.Digest, Mode: p.Mode}
	resumed := false
	if b, err := os.ReadFile(metaPath); err == nil {
		if json.Unmarshal(b, &meta) != nil || meta.Path != path || meta.TotalSize != p.TotalSize || meta.Digest != p.Digest {
			return nil, invalidRequestError("transfer metadata conflict")
		}
		resumed = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if p.Offset == 0 && !resumed {
		if err := writeTransferMeta(metaPath, meta); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() != p.Offset {
		if resumed && p.Offset == 0 && len(data) == 0 && !p.Final {
			_ = f.Close()
			return &proto.WriteResult{Path: path, Offset: st.Size(), Resumed: true}, nil
		}
		_ = f.Close()
		return nil, invalidRequestError("transfer offset does not match staged size")
	}
	if _, err = f.Seek(p.Offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	offset := p.Offset + int64(len(data))
	if !p.Final {
		return &proto.WriteResult{Path: path, BytesWritten: len(data), Offset: offset, Resumed: resumed}, nil
	}
	if offset != p.TotalSize {
		return nil, invalidRequestError("final transfer size mismatch")
	}
	if err := verifyTransferDigest(part, p.TotalSize, p.Digest); err != nil {
		return nil, err
	}
	mode := os.FileMode(p.Mode)
	if mode == 0 {
		mode = 0o600
	}
	if mode&^0o7777 != 0 {
		return nil, invalidRequestError("invalid transfer mode")
	}
	if err := os.Chmod(part, mode); err != nil {
		return nil, err
	}
	if err := os.Rename(part, path); err != nil {
		return nil, err
	}
	_ = os.Remove(metaPath)
	df, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	syncErr := df.Sync()
	closeErr = df.Close()
	if syncErr != nil {
		return nil, syncErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return &proto.WriteResult{Path: path, BytesWritten: len(data), Offset: offset, Committed: true, Resumed: resumed}, nil
}

func transferLock(path string) *sync.Mutex {
	v, _ := transferLocks.LoadOrStore(path, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func decodeTransferB64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

func writeTransferMeta(path string, m transferMeta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".rdev-transfer-meta-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(b)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func verifyTransferDigest(path string, size int64, want string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyN(h, f, size)
	if err != nil && !(errors.Is(err, io.EOF) && n == size) {
		return err
	}
	if n != size {
		return fmt.Errorf("transfer staged size mismatch")
	}
	var extra [1]byte
	if n, _ := f.Read(extra[:]); n != 0 {
		return fmt.Errorf("transfer staged size mismatch")
	}
	if hex.EncodeToString(h.Sum(nil)) != want {
		return invalidRequestError("transfer digest mismatch")
	}
	return nil
}
