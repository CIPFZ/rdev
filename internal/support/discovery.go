package support

import "github.com/CIPFZ/rdev/internal/proto"

// Boundary describes front-end availability, independently of authorization.
type Boundary struct {
	Name        string `json:"name"`
	Status      string `json:"status"`
	Scope       string `json:"scope,omitempty"`
	Alternative string `json:"alternative,omitempty"`
}

type Permission struct {
	Operation        string `json:"operation"`
	Capability       string `json:"capability"`
	Scope            string `json:"scope"`
	Callable         bool   `json:"callable"`
	Allowed          bool   `json:"allowed"`
	ApprovalRequired bool   `json:"approval_required"`
}

type Runtime struct {
	ProbeVersion      string          `json:"probe_version"`
	ProbedAt          string          `json:"probed_at"`
	Platform          Platform        `json:"platform"`
	CgroupDetected    bool            `json:"cgroup_detected"`
	RlimitDetected    bool            `json:"rlimit_detected"`
	WallTimeoutMaxSec int             `json:"wall_timeout_max_sec"`
	Features          []proto.Feature `json:"features,omitempty"`
	Controls          []Boundary      `json:"controls"`
}

// Discovery retains Snapshot's top-level fields for existing support clients.
// Runtime never includes the probe's profile (paths, environment or host data).
type Discovery struct {
	Matrix
	Mode          string       `json:"mode"`
	Frontends     []Boundary   `json:"frontends"`
	RuntimeStatus string       `json:"runtime_status"`
	Runtime       *Runtime     `json:"runtime,omitempty"`
	Permissions   []Permission `json:"permissions,omitempty"`
	PolicyDigest  string       `json:"policy_digest,omitempty"`
}

func Discover(mode string) Discovery {
	out := Discovery{Matrix: Snapshot(), Mode: mode, RuntimeStatus: "not_requested"}
	out.Frontends = []Boundary{
		{Name: "exec_files_jobs_sync", Status: "supported"},
		{Name: "state", Status: "supported", Scope: "host-wide administration; shared mode requires explicit state operation grants (exact-host recommended); migration/repair require approval"},
		{Name: "host_session_edit", Status: "supported"},
		{Name: "declarative_secrets", Status: "supported"},
	}
	if mode == "broker" {
		out.Frontends[2] = Boundary{Name: "host_session_edit", Status: "unsupported", Alternative: "administrator edits private host registry and restarts rdevd; clients supply cwd/env per request"}
		out.Frontends[3] = Boundary{Name: "declarative_secrets", Status: "unsupported", Alternative: "use principal-owned secret set or set_from_file, then secret:name with a secret.use grant"}
	}
	return out
}

func (d *Discovery) SetRuntime(probe *proto.CapabilityResult) {
	if probe == nil {
		return
	}
	p := Platform{OS: probe.OS, Arch: probe.Arch, Tier: "unsupported", Status: "outside the build and runtime support matrix", Validation: "unverified"}
	for _, platform := range d.Remote {
		if platform.OS == probe.OS && platform.Arch == probe.Arch {
			p = platform
			break
		}
	}
	r := &Runtime{Features: append([]proto.Feature(nil), probe.Features...), ProbeVersion: probe.ProbeVersion, ProbedAt: probe.ProbedAt, Platform: p, CgroupDetected: probe.Cgroup, RlimitDetected: probe.Rlimit, WallTimeoutMaxSec: probe.Resources.WallTimeoutSec}
	// A detected cgroup hierarchy does not mean rdev can enforce tree budgets.
	// Probe v1 agents reject these resource envelopes even on Linux cgroup v2.
	for _, name := range []string{"cpu", "memory", "pids"} {
		r.Controls = append(r.Controls, Boundary{Name: name, Status: "unsupported"})
	}
	fdStatus := "unsupported"
	if probe.Rlimit {
		fdStatus = "supported"
	}
	r.Controls = append(r.Controls, Boundary{Name: "file_descriptors", Status: fdStatus, Scope: "up to the target process hard limit"}, Boundary{Name: "wall_timeout", Status: "supported"}, Boundary{Name: "job_count", Status: "supported"})
	if probe.ProbeVersion != "1" {
		for i := range r.Controls {
			r.Controls[i].Status = "unknown"
		}
	}
	jobStart := Boundary{Name: "job_start", Status: "supported"}
	operation, _ := proto.LookupOperation(proto.OpJobStart)
	required := append([]proto.Feature(nil), operation.RequiredFeatures...)
	if d.Mode == "broker" {
		required = append(required, proto.FeatureDurableJobStart)
	}
	available := map[proto.Feature]bool{}
	for _, feature := range probe.Features {
		available[feature] = true
	}
	for _, feature := range required {
		if !available[feature] {
			jobStart.Status = "unsupported"
			jobStart.Alternative = "upgrade the agent to negotiate all required job_start features, including job_resource_envelope"
			break
		}
	}
	r.Controls = append(r.Controls, jobStart)
	d.Runtime, d.RuntimeStatus = r, "probed"
}
