package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/CIPFZ/rdev/internal/proto"
)

type jobStartIdentity struct {
	Schema      int    `json:"schema"`
	JobID       string `json:"job_id"`
	OperationID string `json:"operation_id"`
	PrincipalID string `json:"principal_id"`
	Digest      string `json:"digest"`
}

func jobStartRequest(req *proto.Request, state string) (*proto.JobResult, error) {
	if !req.Job.DurableStart {
		return jobStart(req.Job, state)
	}
	id, err := proto.JobIDForOperation(req.ClientID, req.OperationID)
	if err != nil {
		return nil, err
	}
	digest, err := proto.DurableJobDigest(req)
	if err != nil {
		return nil, err
	}
	identity := &jobStartIdentity{Schema: 1, JobID: id, OperationID: req.OperationID, PrincipalID: req.ClientID, Digest: digest}
	return jobStartWithIdentity(req.Job, state, identity, req.Replay)
}
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

// reserveJobStart runs under the cross-process admission lock. Tombstones are
// never removed by job_rm/GC, so a missing job cannot turn an old operation ID
// into a new execution after remote cache eviction, agent restart or deletion.
func reserveJobStart(state string, want jobStartIdentity, allowCreate bool) (bool, error) {
	root := filepath.Join(state, "job-start-intents")
	if err := secureDir(root, 0700); err != nil {
		return false, err
	}
	if err := syncJobDirectory(state); err != nil {
		return false, err
	}
	sum := sha256.Sum256([]byte(want.PrincipalID))
	prefix := hex.EncodeToString(sum[:]) + "."
	path := filepath.Join(root, prefix+want.JobID+".json")
	if _, err := os.Lstat(path); err == nil {
		if err := secureRecordFile(path); err != nil {
			return false, err
		}
		f, err := os.Open(path)
		if err != nil {
			return false, err
		}
		data, err := io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		if err != nil {
			return false, err
		}
		if len(data) > 4096 {
			return false, processStateError("job start identity record too large")
		}
		var got jobStartIdentity
		if decodeStartIdentity(data, &got) != nil || got != want {
			return false, proto.NewError(proto.CodeOperationIDConflict, want.OperationID, proto.StateNotSent)
		}
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, err
	}
	if !allowCreate {
		return false, proto.NewError(proto.CodeAmbiguousOutcome, want.OperationID, proto.StatePossiblyExecuted)
	}
	d, err := os.Open(root)
	if err != nil {
		return false, err
	}
	defer d.Close()
	total, owner := 0, 0
	for {
		entries, err := d.ReadDir(256)
		for _, entry := range entries {
			total++
			if strings.HasPrefix(entry.Name(), prefix) {
				owner++
			}
		}
		if total >= 8192 || owner >= 1024 {
			return false, limitExceededError("durable job identity retention limit reached")
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, err
		}
	}
	data, _ := json.Marshal(want)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return false, err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	if err := syncJobDirectory(root); err != nil {
		return false, err
	}
	return false, nil
}

func decodeStartIdentity(data []byte, out *jobStartIdentity) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("invalid durable start record")
	}
	seen := make(map[string]bool)
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return errors.New("duplicate durable start field")
		}
		seen[key] = true
		switch key {
		case "schema", "job_id", "operation_id", "principal_id", "digest":
		default:
			return errors.New("unknown durable start field")
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return err
		}
		if bytes.Equal(raw, []byte("null")) {
			return errors.New("null durable start field")
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF || len(seen) != 5 {
		return errors.New("incomplete durable start record")
	}
	return json.Unmarshal(data, out)
}
func recoveredJobStart(dir string, want *jobStartIdentity) (*proto.JobResult, error) {
	meta, err := readMeta(dir)
	if err != nil {
		return nil, proto.NewError(proto.CodeAmbiguousOutcome, want.OperationID, proto.StatePossiblyExecuted)
	}
	if meta.ID != want.JobID || meta.StartOperationID != want.OperationID || meta.StartPrincipalID != want.PrincipalID || meta.StartDigest != want.Digest {
		return nil, proto.NewError(proto.CodeOperationIDConflict, want.OperationID, proto.StateNotSent)
	}
	return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
}

var errDurableStartNeedsIdentity = errors.New("durable start requires request identity")
