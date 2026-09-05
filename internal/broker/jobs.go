package broker

import "sync"

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
