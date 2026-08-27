// Package observe defines the stable, low-cardinality observability seams used
// while rdev's full status/doctor subsystem is built out.
package observe

import (
	"crypto/sha256"
	"fmt"
	"sync"
)

const SchemaVersion = 1

type SecurityReason string

const (
	ReasonProjectUntrusted SecurityReason = "project_untrusted"
	ReasonConfigInvalid    SecurityReason = "config_invalid"
	ReasonConfigSymlink    SecurityReason = "config_symlink"
	ReasonDestination      SecurityReason = "destination_invalid"
	ReasonRemoteDir        SecurityReason = "remote_dir_invalid"
)

var securityReasons = [...]SecurityReason{
	ReasonProjectUntrusted,
	ReasonConfigInvalid,
	ReasonConfigSymlink,
	ReasonDestination,
	ReasonRemoteDir,
}

type Event struct {
	Name       string         `json:"name"`
	Level      string         `json:"level"`
	Reason     SecurityReason `json:"reason,omitempty"`
	TargetHash string         `json:"target_hash,omitempty"`
}

type Sink interface {
	Log(Event)
}

type Snapshot struct {
	SchemaVersion    int               `json:"schema_version"`
	SecurityRejects  map[string]uint64 `json:"security_rejects"`
	ProjectApprovals uint64            `json:"project_approvals"`
}

// Registry accepts only enumerated reasons. Target identity is hashed for logs
// and never becomes a metric label, so arbitrary projects cannot grow series.
type Registry struct {
	mu        sync.RWMutex
	rejects   map[SecurityReason]uint64
	approvals uint64
	sink      Sink
}

func New(sink Sink) *Registry {
	return &Registry{rejects: make(map[SecurityReason]uint64), sink: sink}
}

func validReason(reason SecurityReason) bool {
	for _, candidate := range securityReasons {
		if reason == candidate {
			return true
		}
	}
	return false
}

func hashTarget(target string) string {
	sum := sha256.Sum256([]byte(target))
	return fmt.Sprintf("%x", sum[:8])
}

func (r *Registry) Reject(reason SecurityReason, target string) {
	if r == nil || !validReason(reason) {
		return
	}
	r.mu.Lock()
	r.rejects[reason]++
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{
			Name: "security.config_rejected", Level: "warn", Reason: reason,
			TargetHash: hashTarget(target),
		})
	}
}

func (r *Registry) ProjectApproved(target string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.approvals++
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{
			Name: "security.project_approved", Level: "info",
			TargetHash: hashTarget(target),
		})
	}
}

func (r *Registry) Snapshot() Snapshot {
	out := Snapshot{SchemaVersion: SchemaVersion, SecurityRejects: make(map[string]uint64)}
	if r == nil {
		return out
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, reason := range securityReasons {
		out.SecurityRejects[string(reason)] = r.rejects[reason]
	}
	out.ProjectApprovals = r.approvals
	return out
}
