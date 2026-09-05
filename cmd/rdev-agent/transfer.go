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
	"golang.org/x/sys/unix"
)

const maxTransferBytes int64 = 1 << 30

// Keep each request bounded even when a caller declares a large transfer.
// This protects the JSON/base64 decoder and the staging writer from a single
// request consuming the entire transfer quota.
const maxTransferChunkBytes int64 = 1 << 20

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
	if int64(len(data)) > maxTransferChunkBytes {
		return nil, limitExceededError("transfer chunk exceeds limit")
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
	flock, err := acquireTransferLock(base + ".lock")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = unix.Flock(int(flock.Fd()), unix.LOCK_UN)
		_ = flock.Close()
	}()
	meta := transferMeta{Path: path, TotalSize: p.TotalSize, Digest: p.Digest, Mode: p.Mode}
	resumed := false
	b, metaErr := readTransferMeta(metaPath)
	if metaErr == nil {
		if json.Unmarshal(b, &meta) != nil || meta.Path != path || meta.TotalSize != p.TotalSize || meta.Digest != p.Digest || meta.Mode != p.Mode {
			return nil, invalidRequestError("transfer metadata conflict")
		}
		resumed = true
	} else if !errors.Is(metaErr, os.ErrNotExist) {
		return nil, invalidRequestError("transfer metadata is not a private regular file")
	}
	if p.Offset == 0 && !resumed {
		if err := writeTransferMeta(metaPath, meta); err != nil {
			return nil, err
		}
	}
	f, err := openTransferArtifact(part, unix.O_WRONLY|unix.O_CREAT)
	if err != nil {
		if errors.Is(err, os.ErrExist) || !errors.Is(err, os.ErrNotExist) {
			// Keep the protocol error stable and avoid exposing a platform-specific
			// ELOOP/path detail when an artifact is replaced by a symlink.
			return nil, invalidRequestError("transfer staging is not a private regular file")
		}
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

// acquireTransferLock extends the in-process mutex with an inter-process lock.
// A transfer ID can be retried by two agent processes after a reconnect; both
// must not append to the same staging inode concurrently. O_NOFOLLOW keeps a
// pre-planted lock symlink from redirecting the open outside the destination.
func acquireTransferLock(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, errors.New("failed to create transfer lock")
	}
	st, statErr := f.Stat()
	if statErr != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		_ = f.Close()
		if statErr != nil {
			return nil, statErr
		}
		return nil, errors.New("transfer lock is not a private regular file")
	}
	if err := unix.Flock(fd, unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func decodeTransferB64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

func validateTransferArtifact(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return errors.New("transfer artifact is not private regular file")
	}
	return nil
}

// openTransferArtifact opens a staging/metadata artifact without following a
// symlink. The lstat-before-open check alone is insufficient because another
// process can swap the inode between those operations.
func openTransferArtifact(path string, flags int) (*os.File, error) {
	fd, err := unix.Open(path, flags|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, errors.New("failed to open transfer artifact")
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		_ = f.Close()
		return nil, errors.New("transfer artifact is not a private regular file")
	}
	return f, nil
}

func readTransferMeta(path string) ([]byte, error) {
	f, err := openTransferArtifact(path, unix.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// Metadata is generated by us and should remain tiny. Bound the read so a
	// hostile regular file cannot turn a retry into an unbounded allocation.
	b, err := io.ReadAll(io.LimitReader(f, 64<<10))
	if err != nil {
		return nil, err
	}
	if len(b) == 64<<10 {
		return nil, errors.New("transfer metadata too large")
	}
	return b, nil
}

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
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = d.Sync()
	closeErr := d.Close()
	if err != nil {
		return err
	}
	return closeErr
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
