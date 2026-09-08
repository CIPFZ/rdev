package broker

import "errors"

// StatusSnapshot contains only the authenticated owner's resource use. Global
// pool identities and audit health have separate authorization boundaries.
type StatusSnapshot struct {
	PolicyDigest string            `json:"policy_digest"`
	Ingress      IngressSnapshot   `json:"ingress"`
	Scheduler    SchedulerSnapshot `json:"scheduler"`
	SharedWaits  SharedWaitStatus  `json:"shared_waits"`
}

func ProjectStatus(r Response) (StatusSnapshot, error) {
	if !r.OK {
		if r.Error != "" {
			return StatusSnapshot{}, errors.New(r.Error)
		}
		return StatusSnapshot{}, errors.New("broker status denied")
	}
	if r.Ingress == nil || r.Scheduler == nil || r.SharedWaits == nil || len(r.PolicyDigest) != 64 {
		return StatusSnapshot{}, errors.New("broker returned incomplete status")
	}
	return StatusSnapshot{PolicyDigest: r.PolicyDigest, Ingress: *r.Ingress, Scheduler: *r.Scheduler, SharedWaits: *r.SharedWaits}, nil
}

// ProjectPoolHealth projects only an explicitly authorized administrative reply.
func ProjectPoolHealth(r Response) (PoolHealth, error) {
	if !r.OK {
		return PoolHealth{}, errors.New("broker pool health denied")
	}
	if r.Pool == nil {
		return PoolHealth{}, errors.New("broker returned no pool health")
	}
	return *r.Pool, nil
}
