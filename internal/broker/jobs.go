package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/CIPFZ/rdev/internal/proto"
)

const maxJobRegistryBytes = 4 << 20
const maxJobRegistryRecords = 8192

type JobRef struct{ ID, Owner, Host string }
type jobKey struct{ Host, ID string }
type jobSnapshot struct {
	Schema int      `json:"schema"`
	Jobs   []JobRef `json:"jobs"`
}

var ErrJobRegistryUnavailable = errors.New("job registry durability uncertain; restart required")

type jobCommitUncertain struct{ cause error }

func (e *jobCommitUncertain) Error() string { return ErrJobRegistryUnavailable.Error() }
func (e *jobCommitUncertain) Unwrap() error { return e.cause }

// JobRegistry separates serialized durable updates from read-only snapshots.
// Readers never wait for filesystem I/O. A failed pre-rename write leaves the
// active ownership map unchanged; an uncertain post-rename error fails closed
// until an explicit load of the authoritative on-disk state.
type JobRegistry struct {
	updateMu sync.Mutex
	mu       sync.RWMutex
	jobs     map[jobKey]JobRef
	path     string
	failed   error
	persist  func(string, []JobRef) error
}

func NewJobRegistry() *JobRegistry {
	return &JobRegistry{jobs: make(map[jobKey]JobRef), persist: saveJobs}
}
func validateJobRef(j JobRef) error {
	client, project, ok := strings.Cut(j.Owner, "\x00")
	if !ok || (Owner{ClientID: client, ProjectID: project}).Validate() != nil {
		return fmt.Errorf("invalid persisted job owner")
	}
	if j.Host == "" || len(j.Host) > 512 || strings.ContainsAny(j.Host, "\x00\r\n") {
		return fmt.Errorf("invalid persisted job host")
	}
	if j.ID == "" || len(j.ID) > 128 || j.ID == "." || j.ID == ".." {
		return fmt.Errorf("invalid persisted job id")
	}
	for _, c := range j.ID {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return fmt.Errorf("invalid persisted job id")
		}
	}
	return nil
}
func sortedJobs(jobs map[jobKey]JobRef) []JobRef {
	out := make([]JobRef, 0, len(jobs))
	for _, ref := range jobs {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].ID < out[j].ID
	})
	return out
}
func (r *JobRegistry) healthy() error { r.mu.RLock(); defer r.mu.RUnlock(); return r.failed }
func (r *JobRegistry) update(change func(map[jobKey]JobRef) error) error {
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.mu.RLock()
	if r.failed != nil {
		r.mu.RUnlock()
		return ErrJobRegistryUnavailable
	}
	next := make(map[jobKey]JobRef, len(r.jobs))
	for key, ref := range r.jobs {
		next[key] = ref
	}
	path := r.path
	r.mu.RUnlock()
	if err := change(next); err != nil {
		return err
	}
	if len(next) > maxJobRegistryRecords {
		return fmt.Errorf("job registry record limit exceeded")
	}
	if path != "" {
		if err := r.persistSnapshot(path, sortedJobs(next)); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.jobs = next
	r.mu.Unlock()
	return nil
}
func (r *JobRegistry) Put(ref JobRef) error {
	if err := validateJobRef(ref); err != nil {
		return err
	}
	return r.update(func(next map[jobKey]JobRef) error {
		key := jobKey{ref.Host, ref.ID}
		if old, ok := next[key]; ok && old.Owner != ref.Owner {
			return fmt.Errorf("job owner conflict")
		}
		next[key] = ref
		return nil
	})
}
func (r *JobRegistry) GetHost(host, id string) (JobRef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.failed != nil {
		return JobRef{}, false
	}
	ref, ok := r.jobs[jobKey{host, id}]
	return ref, ok
}
func (r *JobRegistry) RemoveOwned(host, owner string, ids ...string) error {
	return r.update(func(next map[jobKey]JobRef) error {
		for _, id := range ids {
			if ref, ok := next[jobKey{host, id}]; ok && ref.Owner != owner {
				return fmt.Errorf("job owner mismatch")
			}
		}
		for _, id := range ids {
			delete(next, jobKey{host, id})
		}
		return nil
	})
}
func (r *JobRegistry) Snapshot() []JobRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sortedJobs(r.jobs)
}
func (r *JobRegistry) Save(path string) error {
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	r.mu.RLock()
	if r.failed != nil {
		r.mu.RUnlock()
		return ErrJobRegistryUnavailable
	}
	snapshot := sortedJobs(r.jobs)
	r.mu.RUnlock()
	return r.persistSnapshot(path, snapshot)
}
func (r *JobRegistry) persistSnapshot(path string, snapshot []JobRef) error {
	err := r.persist(path, snapshot)
	var uncertain *jobCommitUncertain
	if errors.As(err, &uncertain) {
		r.mu.Lock()
		r.failed = ErrJobRegistryUnavailable
		r.mu.Unlock()
	}
	return err
}
func (r *JobRegistry) OwnedIDs(host, owner string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var ids []string
	if r.failed == nil {
		for key, ref := range r.jobs {
			if key.Host == host && ref.Owner == owner {
				ids = append(ids, key.ID)
			}
		}
	}
	sort.Strings(ids)
	return ids
}
func saveJobs(path string, jobs []JobRef) error {
	data, err := json.Marshal(jobSnapshot{Schema: 1, Jobs: jobs})
	if err != nil {
		return err
	}
	if len(data) > maxJobRegistryBytes {
		return fmt.Errorf("job registry size limit exceeded")
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".rdev-jobs-*")
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
	d, err := os.Open(dir)
	if err != nil {
		return &jobCommitUncertain{err}
	}
	err = d.Sync()
	closeErr := d.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return &jobCommitUncertain{err}
	}
	return nil
}

// decodeJobArray is strict even for legacy schema-less snapshots. Duplicate
// field names and references cannot silently select a different owner.
func decodeJobArray(dec *json.Decoder) (map[jobKey]JobRef, error) {
	t, err := dec.Token()
	if err != nil || t != json.Delim('[') {
		return nil, fmt.Errorf("jobs requires an array")
	}
	out := make(map[jobKey]JobRef)
	for dec.More() {
		if len(out) >= maxJobRegistryRecords {
			return nil, fmt.Errorf("job registry record limit exceeded")
		}
		t, err = dec.Token()
		if err != nil || t != json.Delim('{') {
			return nil, fmt.Errorf("job reference requires an object")
		}
		fields := make(map[string]string, 3)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok || (name != "ID" && name != "Owner" && name != "Host") {
				return nil, fmt.Errorf("unknown job reference field")
			}
			if _, exists := fields[name]; exists {
				return nil, fmt.Errorf("duplicate job reference field")
			}
			val, err := dec.Token()
			if err != nil {
				return nil, err
			}
			value, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("job reference requires strings")
			}
			fields[name] = value
		}
		if t, err = dec.Token(); err != nil || t != json.Delim('}') {
			return nil, fmt.Errorf("invalid job reference")
		}
		ref := JobRef{ID: fields["ID"], Owner: fields["Owner"], Host: fields["Host"]}
		if err := validateJobRef(ref); err != nil {
			return nil, err
		}
		key := jobKey{ref.Host, ref.ID}
		if _, exists := out[key]; exists {
			return nil, fmt.Errorf("duplicate job reference")
		}
		out[key] = ref
	}
	if t, err = dec.Token(); err != nil || t != json.Delim(']') {
		return nil, fmt.Errorf("invalid job array")
	}
	return out, nil
}
func parseJobs(data []byte) (map[jobKey]JobRef, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, fmt.Errorf("empty job registry")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	var jobs map[jobKey]JobRef
	var err error
	if data[0] == '[' {
		jobs, err = decodeJobArray(dec)
	} else {
		t, err := dec.Token()
		if err != nil || t != json.Delim('{') {
			return nil, fmt.Errorf("job registry requires a versioned object or legacy array")
		}
		seen := make(map[string]bool)
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return nil, fmt.Errorf("duplicate job snapshot field")
			}
			seen[name] = true
			switch name {
			case "schema":
				var schema int
				if err := dec.Decode(&schema); err != nil || schema != 1 {
					return nil, fmt.Errorf("unsupported job registry schema")
				}
			case "jobs":
				jobs, err = decodeJobArray(dec)
				if err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("unknown job snapshot field")
			}
		}
		if !seen["schema"] || !seen["jobs"] {
			return nil, fmt.Errorf("incomplete job snapshot")
		}
		if t, err = dec.Token(); err != nil || t != json.Delim('}') {
			return nil, fmt.Errorf("invalid job snapshot")
		}
	}
	if err != nil {
		return nil, err
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("job registry requires one document")
	}
	return jobs, nil
}
func (r *JobRegistry) load(path string, configure bool) error {
	r.updateMu.Lock()
	defer r.updateMu.Unlock()
	data, err := ReadPrivateFile(path, maxJobRegistryBytes)
	var next map[jobKey]JobRef
	if err != nil {
		if !configure || !os.IsNotExist(err) {
			return err
		}
		next = make(map[jobKey]JobRef)
	} else {
		next, err = parseJobs(data)
		if err != nil {
			return err
		}
	}
	if configure {
		if err := r.persistSnapshot(path, sortedJobs(next)); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.jobs = next
	r.failed = nil
	if configure {
		r.path = path
	}
	r.mu.Unlock()
	return nil
}
func (r *JobRegistry) Load(path string) error                 { return r.load(path, false) }
func (r *JobRegistry) ConfigurePersistence(path string) error { return r.load(path, true) }

// BindRequest captures the authorized list scope before admission and remote
// pagination. The caller's parameters are immutable, including across retries.
func (r *JobRegistry) BindRequest(host, owner string, req *proto.Request) (*proto.Request, error) {
	if err := r.ValidateRequest(host, owner, req); err != nil {
		return nil, err
	}
	if req.Op != proto.OpJobList {
		return req, nil
	}
	bound := *req
	params := proto.JobParams{}
	if req.Job != nil {
		params = *req.Job
	}
	params.FilterIDs = true
	params.IDs = append([]string(nil), params.IDs...)
	if len(params.IDs) == 0 {
		params.IDs = r.OwnedIDs(host, owner)
	}
	bound.Job = &params
	return &bound, nil
}

// ValidateRequest checks references before remote dispatch. Remote job storage is
// shared by Unix user and does not itself implement broker principal isolation.
func (r *JobRegistry) ValidateRequest(host, owner string, req *proto.Request) error {
	if err := r.healthy(); err != nil {
		return err
	}
	if req.Job == nil {
		return nil
	}
	if len(req.Job.IDs) > maxJobRegistryRecords {
		return fmt.Errorf("job ID limit exceeded")
	}
	if req.Op == proto.OpJobRm && req.Job.ID == "" {
		return fmt.Errorf("broker job_rm requires an explicit owned job ID")
	}
	ids := append([]string{}, req.Job.IDs...)
	if req.Job.ID != "" {
		ids = append(ids, req.Job.ID)
	}
	for _, id := range ids {
		ref, ok := r.GetHost(host, id)
		if !ok || ref.Owner != owner {
			return fmt.Errorf("job owner mismatch or unknown job")
		}
	}
	return nil
}

// RecordResponse commits remote mutation outcomes before they are acknowledged.
// It never retries a remote mutation, including after a local disk failure.
func (r *JobRegistry) RecordResponse(host, owner string, req *proto.Request, resp *proto.Response) error {
	if resp == nil || !resp.OK {
		return nil
	}
	if resp.Job == nil {
		if strings.HasPrefix(req.Op, "job_") {
			return fmt.Errorf("remote job response missing result")
		}
		return nil
	}
	if err := r.healthy(); err != nil {
		return err
	}
	switch req.Op {
	case proto.OpJobStart:
		if resp.Job.Info == nil || resp.Job.Info.ID == "" {
			return fmt.Errorf("job_start response missing job identity")
		}
		if req.Job != nil && req.Job.DurableStart {
			principal := proto.PrincipalID(req.ClientID, req.ProjectID)
			id, err := proto.JobIDForOperation(principal, req.OperationID)
			if err != nil {
				return err
			}
			digest, err := proto.DurableJobDigest(req)
			if err != nil {
				return err
			}
			info := resp.Job.Info
			if info.ID != id || info.StartOperationID != req.OperationID || info.StartPrincipalID != principal || info.StartDigest != digest {
				return errors.New("remote job start identity mismatch")
			}
		}
		return r.Put(JobRef{ID: resp.Job.Info.ID, Host: host, Owner: owner})
	case proto.OpJobRm:
		if req.Job == nil || req.Job.ID == "" {
			return fmt.Errorf("job removal target missing")
		}
		ids := append(append([]string(nil), resp.Job.Removed...), resp.Job.Missing...)
		for _, id := range ids {
			if id != req.Job.ID {
				return fmt.Errorf("job removal response changed target")
			}
		}
		return r.RemoveOwned(host, owner, ids...)
	case proto.OpJobWait, proto.OpJobStatus, proto.OpJobStop, proto.OpJobLogs:
		if req.Job == nil {
			return fmt.Errorf("job response has no requested scope")
		}
		selected := make(map[string]bool, len(req.Job.IDs)+1)
		selected[req.Job.ID] = req.Job.ID != ""
		for _, id := range req.Job.IDs {
			selected[id] = true
		}
		validate := func(id string) error {
			ref, ok := r.GetHost(host, id)
			if !selected[id] || !ok || ref.Owner != owner {
				return fmt.Errorf("job response changed owner or target")
			}
			return nil
		}
		if resp.Job.Info != nil {
			if err := validate(resp.Job.Info.ID); err != nil {
				return err
			}
		}
		for _, waited := range resp.Job.Waited {
			if waited == nil {
				return fmt.Errorf("invalid waited job")
			}
			if err := validate(waited.ID); err != nil {
				return err
			}
			if waited.Info != nil && waited.Info.ID != waited.ID {
				return fmt.Errorf("waited job changed identity")
			}
		}
	case proto.OpJobList:
		if req.Job == nil {
			return fmt.Errorf("job list parameters missing")
		}
		originalLength := len(resp.Job.List)
		originalTotal := resp.Job.Total
		selected := make(map[string]bool, len(req.Job.IDs))
		for _, id := range req.Job.IDs {
			selected[id] = true
		}
		out := resp.Job.List[:0]
		for _, info := range resp.Job.List {
			if info != nil {
				ref, ok := r.GetHost(host, info.ID)
				if ok && ref.Owner == owner && (!req.Job.FilterIDs || selected[info.ID]) {
					out = append(out, info)
				}
			}
		}
		if req.Job.FilterIDs && len(out) != originalLength {
			return fmt.Errorf("scoped job list contained an unowned reference")
		}
		resp.Job.List = out
		resp.Job.Total = len(out)
		// Only scoped lists have trustworthy totals/truncation; BindRequest binds
		// that remote filter before dispatch. Never expose an unscoped hint.
		if !req.Job.FilterIDs {
			resp.Job.Truncated = false
		} else {
			resp.Job.Total = originalTotal
		}
	}
	return nil
}
