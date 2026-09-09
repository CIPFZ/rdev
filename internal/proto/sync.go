package proto

import "github.com/CIPFZ/rdev/internal/synctree"

const (
	OpSyncInspect = "sync_inspect"
	OpSyncStage   = "sync_stage"
	OpSyncCommit  = "sync_commit"
)

// SyncParams is broker-internal. Public callers use approved sync.push/pull;
// no capability grant enables direct access to these staging primitives.
type SyncParams struct {
	Action    string             `json:"action,omitempty"`
	ID        string             `json:"id,omitempty"`
	Path      string             `json:"path,omitempty"`
	Policy    string             `json:"policy,omitempty"`
	Index     int                `json:"index,omitempty"`
	Offset    int64              `json:"offset,omitempty"`
	Data      []byte             `json:"data,omitempty"`
	Manifest  *synctree.Manifest `json:"manifest,omitempty"`
	Plan      *synctree.Plan     `json:"plan,omitempty"`
	OutcomeID string             `json:"outcome_id,omitempty"`
}
type SyncResult struct {
	Snapshot *synctree.Snapshot `json:"snapshot,omitempty"`
	Stage    *synctree.Stage    `json:"stage,omitempty"`
	Outcome  *synctree.Outcome  `json:"outcome,omitempty"`
	Data     []byte             `json:"data,omitempty"`
}
