package broker

import (
	"github.com/CIPFZ/rdev/internal/proto"
	"time"
)

type Request struct {
	ID              string         `json:"id"`
	Owner           Owner          `json:"owner"`
	Operation       string         `json:"operation"`
	Host            string         `json:"host,omitempty"`
	Wire            *proto.Request `json:"wire,omitempty"`
	Target          string         `json:"target,omitempty"`
	ApprovalSpec    *ApprovalSpec  `json:"approval_spec,omitempty"`
	Approval        string         `json:"approval,omitempty"`
	Risk            bool           `json:"risk,omitempty"`
	Capability      string         `json:"capability,omitempty"`
	GrantOwner      Owner          `json:"grant_owner,omitempty"`
	GrantOperation  string         `json:"grant_operation,omitempty"`
	GrantHost       string         `json:"grant_host,omitempty"`
	GrantCapability string         `json:"grant_capability,omitempty"`
	Revoke          bool           `json:"revoke,omitempty"`
	Since           time.Time      `json:"since,omitempty"`
}
type Response struct {
	ID              string          `json:"id"`
	OK              bool            `json:"ok"`
	Error           string          `json:"error,omitempty"`
	Wire            *proto.Response `json:"wire,omitempty"`
	Audit           []AuditEvent    `json:"audit,omitempty"`
	Approval        *Approval       `json:"approval,omitempty"`
	PolicyDigest    string          `json:"policy_digest,omitempty"`
	AuditIncomplete bool            `json:"audit_incomplete,omitempty"`
}
