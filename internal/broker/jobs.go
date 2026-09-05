package broker

import (
	"encoding/json"
	"os"
	"sync"
)

type JobRef struct{ ID, Owner, Host string }

// JobRegistry retains detached job ownership independently of live transports.
type JobRegistry struct {
	mu   sync.RWMutex
	jobs map[string]JobRef
}

func NewJobRegistry() *JobRegistry      { return &JobRegistry{jobs: make(map[string]JobRef)} }
func (r *JobRegistry) Put(job JobRef)   { r.mu.Lock(); r.jobs[job.ID] = job; r.mu.Unlock() }
func (r *JobRegistry) Remove(id string) { r.mu.Lock(); delete(r.jobs, id); r.mu.Unlock() }
func (r *JobRegistry) Get(id string) (JobRef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	j, ok := r.jobs[id]
	return j, ok
}
func (r *JobRegistry) Snapshot() []JobRef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]JobRef, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, j)
	}
	return out
}

func (r *JobRegistry) Save(path string) error {
	data, err := json.Marshal(r.Snapshot())
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
func (r *JobRegistry) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var jobs []JobRef
	if err := json.Unmarshal(data, &jobs); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, j := range jobs {
		if j.ID != "" {
			r.jobs[j.ID] = j
		}
	}
	return nil
}
