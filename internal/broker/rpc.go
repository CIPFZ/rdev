package broker

import (
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/support"
	"time"
)

type Request struct {
	Sync            *client.SyncOptions `json:"sync,omitempty"`
	Secret          *SecretParams       `json:"secret,omitempty"`
	OperationID     string              `json:"operation_id,omitempty"`
	JobEvents       *JobEventQuery      `json:"job_events,omitempty"`
	MutationID      string              `json:"mutation_id,omitempty"`
	ID              string              `json:"id"`
	Owner           Owner               `json:"owner"`
	Operation       string              `json:"operation"`
	Host            string              `json:"host,omitempty"`
	Wire            *proto.Request      `json:"wire,omitempty"`
	Target          string              `json:"target,omitempty"`
	ApprovalSpec    *ApprovalSpec       `json:"approval_spec,omitempty"`
	Approval        string              `json:"approval,omitempty"`
	Risk            bool                `json:"risk,omitempty"`
	Capability      string              `json:"capability,omitempty"`
	GrantOwner      Owner               `json:"grant_owner,omitempty"`
	GrantOperation  string              `json:"grant_operation,omitempty"`
	GrantHost       string              `json:"grant_host,omitempty"`
	GrantCapability string              `json:"grant_capability,omitempty"`
	Revoke          bool                `json:"revoke,omitempty"`
	Since           time.Time           `json:"since,omitempty"`
}
type Response struct {
	Support         *support.Discovery `json:"support,omitempty"`
	Sync            *client.SyncResult `json:"sync,omitempty"`
	Secrets         []SecretDescriptor `json:"secrets,omitempty"`
	RequestRef      string             `json:"request_ref,omitempty"`
	Pool            *PoolHealth        `json:"pool,omitempty"`
	Ingress         *IngressSnapshot   `json:"ingress,omitempty"`
	History         *JobEventPage      `json:"history,omitempty"`
	Mutation        *MutationIntent    `json:"mutation,omitempty"`
	SharedWaits     *SharedWaitStatus  `json:"shared_waits,omitempty"`
	AuditHealth     *AuditSinkStatus   `json:"audit_health,omitempty"`
	Scheduler       *SchedulerSnapshot `json:"scheduler,omitempty"`
	ID              string             `json:"id"`
	OK              bool               `json:"ok"`
	Error           string             `json:"error,omitempty"`
	Wire            *proto.Response    `json:"wire,omitempty"`
	Audit           []AuditEvent       `json:"audit,omitempty"`
	Approval        *Approval          `json:"approval,omitempty"`
	PolicyDigest    string             `json:"policy_digest,omitempty"`
	AuditIncomplete bool               `json:"audit_incomplete,omitempty"`
}
