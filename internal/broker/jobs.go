package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/CIPFZ/rdev/internal/proto"
)

type JobRef struct{ ID, Owner, Host string }
type jobKey struct{ Host, ID string }

// JobRegistry retains detached job ownership independently of live transports.
// Host and ID identify the remote object; Owner is immutable authorization data.
type JobRegistry struct {
	mu   sync.RWMutex
	jobs map[jobKey]JobRef
	path string
}

func NewJobRegistry() *JobRegistry { return &JobRegistry{jobs: make(map[jobKey]JobRef)} }
func (r *JobRegistry) Put(job JobRef) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := jobKey{job.Host, job.ID}
	if previous, ok := r.jobs[key]; ok && previous.Owner != job.Owner {
		return fmt.Errorf("job owner conflict")
	}
	r.jobs[key] = job
	return r.persistLocked()
}

// Remove is a legacy unscoped helper. Runtime callers must use RemoveOwned.
func (r *JobRegistry) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.jobs {
		if key.ID == id {
			delete(r.jobs, key)
		}
	}
}

// Get fails closed when an ID occurs on multiple hosts.
func (r *JobRegistry) Get(id string) (JobRef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out JobRef
	found := false
	for key, ref := range r.jobs {
		if key.ID == id {
			if found {
				return JobRef{}, false
			}
			out, found = ref, true
		}
	}
	return out, found
}
func (r *JobRegistry) GetHost(host, id string) (JobRef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ref, ok := r.jobs[jobKey{host, id}]
	return ref, ok
}
func (r *JobRegistry) RemoveOwned(host, owner string, ids ...string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		key := jobKey{host, id}
		if ref, ok := r.jobs[key]; ok && ref.Owner == owner {
			delete(r.jobs, key)
		}
	}
	return r.persistLocked()
}
func (r *JobRegistry) snapshotLocked() []JobRef {
	out := make([]JobRef, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Host != out[j].Host {
			return out[i].Host < out[j].Host
		}
		return out[i].ID < out[j].ID
	})
	return out
}
func (r *JobRegistry) Snapshot() []JobRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotLocked()
}

// ConfigurePersistence loads ownership before requests are admitted. Invalid
// records fail startup rather than silently erasing authorization state.
func (r *JobRegistry) ConfigurePersistence(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.loadLocked(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	r.path = path
	return r.persistLocked()
}
func (r *JobRegistry) persistLocked() error {
	if r.path == "" {
		return nil
	}
	return r.saveLocked(r.path)
}
func (r *JobRegistry) Save(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.saveLocked(path)
}
func (r *JobRegistry) saveLocked(path string) error {
	data, err := json.Marshal(r.snapshotLocked())
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".rdev-jobs-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
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
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func (r *JobRegistry) Load(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.loadLocked(path)
}
func (r *JobRegistry) loadLocked(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var jobs []JobRef
	if err = json.Unmarshal(data, &jobs); err != nil {
		return err
	}
	loaded := make(map[jobKey]JobRef, len(jobs))
	for _, j := range jobs {
		if j.ID == "" || j.Host == "" || j.Owner == "" {
			return fmt.Errorf("invalid persisted job reference")
		}
		key := jobKey{j.Host, j.ID}
		if old, ok := loaded[key]; ok && old.Owner != j.Owner {
			return fmt.Errorf("conflicting persisted job ownership")
		}
		loaded[key] = j
	}
	r.jobs = loaded
	return nil
}

// ValidateRequest checks references before remote dispatch. Remote job storage is
// shared by Unix user and does not itself implement broker principal isolation.
func (r *JobRegistry) ValidateRequest(host, owner string, req *proto.Request) error {
	if req.Job == nil {
		return nil
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
	if resp == nil || !resp.OK || resp.Job == nil {
		return nil
	}
	switch req.Op {
	case proto.OpJobStart:
		if resp.Job.Info == nil || resp.Job.Info.ID == "" {
			return fmt.Errorf("job_start response missing job identity")
		}
		return r.Put(JobRef{ID: resp.Job.Info.ID, Host: host, Owner: owner})
	case proto.OpJobRm:
		return r.RemoveOwned(host, owner, resp.Job.Removed...)
	case proto.OpJobList:
		out := resp.Job.List[:0]
		for _, info := range resp.Job.List {
			if info != nil {
				ref, ok := r.GetHost(host, info.ID)
				if ok && ref.Owner == owner {
					out = append(out, info)
				}
			}
		}
		resp.Job.List = out
		resp.Job.Total = len(out)
	}
	return nil
}
