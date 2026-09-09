package broker

import (
	"github.com/CIPFZ/rdev/internal/proto"
	"time"
)

const FleetSchemaVersion = 1
const FleetMaxTargets = 128
const FleetLargeThreshold = 20
const FleetMaxPlans = 64
const FleetMaxOwnerPlans = 8
const FleetMaxBytes = 16 << 20
const FleetMaxSpecBytes = 16 << 10

// FleetSpec is the only execution model accepted by every Fleet entry point.
// The first version supports durable job_start without secrets or inherited env.
type FleetSpec struct {
	Selector  string           `json:"selector"`
	Operation string           `json:"operation"`
	Job       *proto.JobParams `json:"job"`
	Rollout   FleetRollout     `json:"rollout"`
}
type FleetRollout struct {
	Strategy             string   `json:"strategy"`
	MaxParallel          int      `json:"max_parallel"`
	Canary               int      `json:"canary,omitempty"`
	WaveSize             int      `json:"wave_size,omitempty"`
	PauseBetweenWavesSec int      `json:"pause_between_waves_sec,omitempty"`
	MaxFailures          *int     `json:"max_failures,omitempty"`
	MaxFailureRatio      *float64 `json:"max_failure_ratio,omitempty"`
	OnThreshold          string   `json:"on_threshold,omitempty"`
}
type FleetRequest struct {
	Spec       *FleetSpec      `json:"spec,omitempty"`
	PlanID     string          `json:"plan_id,omitempty"`
	Digest     string          `json:"digest,omitempty"`
	HostIDs    []string        `json:"host_ids,omitempty"`
	Offset     int             `json:"offset,omitempty"`
	Limit      int             `json:"limit,omitempty"`
	TTLSeconds int             `json:"ttl_seconds,omitempty"`
	Inventory  *FleetInventory `json:"inventory,omitempty"`
	Revision   uint64          `json:"revision,omitempty"`
}
type FleetPlan struct {
	RiskReasons       []string       `json:"risk_reasons"`
	PlanID            string         `json:"plan_id"`
	ParentPlanID      string         `json:"parent_plan_id,omitempty"`
	Owner             Owner          `json:"owner"`
	Spec              FleetSpec      `json:"spec"`
	Digest            string         `json:"digest"`
	State             string         `json:"state"`
	Reason            string         `json:"reason,omitempty"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         time.Time      `json:"updated_at"`
	Runs              []HostRun      `json:"runs"`
	WaveEnd           int            `json:"wave_end"`
	NextWaveAt        time.Time      `json:"next_wave_at,omitempty"`
	ApprovalRef       string         `json:"approval_ref,omitempty"`
	ApprovalPolicy    string         `json:"approval_policy,omitempty"`
	ApprovalExpiresAt time.Time      `json:"approval_expires_at,omitempty"`
	ApprovalConsumed  bool           `json:"approval_consumed,omitempty"`
	Counts            map[string]int `json:"counts"`
	Total             int            `json:"total"`
	NextOffset        int            `json:"next_offset,omitempty"`
	ExitStatus        int            `json:"exit_status"`
}
type HostRun struct {
	Host           FleetHost `json:"host"`
	Alias          string    `json:"alias"`
	DispatchDigest string    `json:"dispatch_digest"`
	State          string    `json:"state"`
	Attempt        int       `json:"attempt"`
	OperationID    string    `json:"operation_id"`
	JobID          string    `json:"job_id,omitempty"`
	JobDigest      string    `json:"job_digest,omitempty"`
	ExitCode       *int      `json:"exit_code,omitempty"`
	Reason         string    `json:"reason,omitempty"`
	RetryPlanID    string    `json:"retry_plan_id,omitempty"`
	ApprovalRef    string    `json:"approval_ref,omitempty"`
	PolicyDigest   string    `json:"policy_digest,omitempty"`
}
type FleetPage struct {
	Plans      []FleetPlan `json:"plans"`
	NextOffset int         `json:"next_offset,omitempty"`
}
