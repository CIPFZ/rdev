package broker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// One small durable marker precedes audit admission. A crash can lose the
// asynchronous queue even if every surviving segment ends in a complete line.
// Incomplete is sticky: a later clean exit cannot repair an earlier unknown gap.
type auditSeal struct {
	ActiveSHA256  string `json:"active_sha256"`
	RotatedSHA256 string `json:"rotated_sha256"`
}

func sealAuditBytes(active, rotated []byte) *auditSeal {
	a, b := sha256.Sum256(active), sha256.Sum256(rotated)
	return &auditSeal{ActiveSHA256: hex.EncodeToString(a[:]), RotatedSHA256: hex.EncodeToString(b[:])}
}

func readAuditSeal(path string, maxBytes int64) (*auditSeal, error) {
	active, err := readAuditSegment(path, maxBytes)
	if err != nil {
		return nil, err
	}
	rotated, err := readAuditSegment(path+".1", maxBytes)
	if err != nil {
		return nil, err
	}
	return sealAuditBytes(active, rotated), nil
}

type auditContinuity struct {
	Seal              *auditSeal `json:"seal,omitempty"`
	Schema            int        `json:"schema"`
	Active            bool       `json:"active"`
	Incomplete        bool       `json:"incomplete"`
	UncleanRecoveries uint64     `json:"unclean_recoveries"`
}

func readAuditContinuity(path string) (auditContinuity, bool, error) {
	data, err := ReadPrivateFile(path, 1024)
	if os.IsNotExist(err) {
		return auditContinuity{Schema: 1}, false, nil
	}
	if err != nil {
		return auditContinuity{}, false, err
	}
	invalid := errors.New("invalid audit continuity state")
	dec := json.NewDecoder(bytes.NewReader(data))
	if rejectDuplicateJSON(dec) != nil {
		return auditContinuity{}, false, invalid
	}
	if _, err := dec.Token(); err != io.EOF {
		return auditContinuity{}, false, invalid
	}
	var state auditContinuity
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if dec.Decode(&state) != nil || state.Schema != 1 || state.UncleanRecoveries > 1<<63-1 {
		return auditContinuity{}, false, invalid
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || (len(fields) != 4 && len(fields) != 5) {
		return auditContinuity{}, false, invalid
	}
	for _, name := range []string{"active", "incomplete"} {
		value := bytes.TrimSpace(fields[name])
		if !bytes.Equal(value, []byte("true")) && !bytes.Equal(value, []byte("false")) {
			return auditContinuity{}, false, invalid
		}
	}
	if bytes.Equal(bytes.TrimSpace(fields["unclean_recoveries"]), []byte("null")) {
		return auditContinuity{}, false, invalid
	}
	if state.Seal != nil && (!validDigest(state.Seal.ActiveSHA256) || !validDigest(state.Seal.RotatedSHA256)) {
		return auditContinuity{}, false, invalid
	}
	if !state.Active && state.Seal == nil {
		return auditContinuity{}, false, invalid
	}
	if state.UncleanRecoveries > 0 && !state.Incomplete {
		return auditContinuity{}, false, invalid
	}
	return state, true, nil
}

func saveAuditContinuity(path string, state auditContinuity) error {
	if _, err := ReadPrivateFile(path, 1024); err != nil && !os.IsNotExist(err) {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".rdev-audit-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	return errors.Join(err, dir.Close())
}
