// Package support exposes the machine-readable support baseline. Runtime tiers
// are intentionally separate from build coverage so cross-compilation cannot be
// mistaken for certification.
package support

const SchemaVersion = 1

type Platform struct {
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	Tier       string `json:"tier"`
	TargetTier string `json:"target_tier"`
	Status     string `json:"status"`
	Validation string `json:"validation"`
}

type Matrix struct {
	SchemaVersion           int        `json:"schema_version"`
	ProductionCertification string     `json:"production_certification"`
	Local                   []Platform `json:"local"`
	Remote                  []Platform `json:"remote"`
	RequiredSSHFeatures     []string   `json:"required_ssh_features"`
	RequiredLocalTools      []string   `json:"required_local_tools"`
	NonGoals                []string   `json:"non_goals"`
	Capabilities            []Boundary `json:"capabilities"`
}

func Snapshot() Matrix {
	return Matrix{
		SchemaVersion:           SchemaVersion,
		ProductionCertification: "pending: Tier 1 runtime, trusted official signing, hosted CI, complete compatibility/scale and continuous 24h evidence required",
		Local: []Platform{
			{OS: "darwin", Arch: "arm64", Tier: "tier1", TargetTier: "tier1", Status: "historical development baseline; shared runtime unverified and deferred; fd-native config ACL checks require cgo", Validation: "historical_only"},
			{OS: "darwin", Arch: "amd64", Tier: "build", TargetTier: "tier1", Status: "cross-build only", Validation: "build_only"},
			{OS: "linux", Arch: "amd64", Tier: "tier1", TargetTier: "tier1", Status: "Linux standalone and shared broker real-SSH runtime verified", Validation: "runtime_verified"},
			{OS: "linux", Arch: "arm64", Tier: "build", TargetTier: "tier1", Status: "cross-build only", Validation: "build_only"},
		},
		Remote: []Platform{
			{OS: "windows", Arch: "amd64", Tier: "experimental", TargetTier: "tier1", Status: "native remote agent implemented; Windows runtime and SSH certification pending", Validation: "build_only"},
			{OS: "linux", Arch: "amd64", Tier: "tier1", TargetTier: "tier1", Status: "Ubuntu standalone and shared bootstrap, exec, file, sync, cancellation and jobs runtime verified", Validation: "runtime_verified"},
			{OS: "linux", Arch: "arm64", Tier: "build", TargetTier: "tier1", Status: "agent cross-build only", Validation: "build_only"},
			{OS: "darwin", Arch: "amd64", Tier: "build", TargetTier: "tier1", Status: "agent cross-build only", Validation: "build_only"},
			{OS: "darwin", Arch: "arm64", Tier: "build", TargetTier: "tier1", Status: "agent cross-build only", Validation: "build_only"},
		},
		RequiredSSHFeatures: []string{"BatchMode", "ControlMaster", "ControlPath", "ControlPersist", "OpenSSH SSHSIG (-Y sign/verify) for release trust"},
		RequiredLocalTools:  []string{"ssh", "rsync for sync operations"},
		Capabilities: []Boundary{
			{Name: "ssh_alias_ipv4_ipv6_proxyjump", Status: "supported"},
			{Name: "complex_proxycommand", Status: "experimental", Scope: "uses user OpenSSH configuration; no complete runtime certification"},
			{Name: "regular_files_and_symlink_policy", Status: "supported"},
			{Name: "file_mode_mtime", Status: "best_effort", Scope: "subject to target filesystem and user permissions"},
			{Name: "interactive_pty", Status: "unsupported"},
			{Name: "native_windows", Status: "experimental", Scope: "remote Windows amd64 agent only; native Windows controller remains unsupported; see docs/phase9-acceptance.md"},
			{Name: "complete_acl_xattr_owner_fidelity", Status: "unsupported"},
			{Name: "remote_to_remote_sync", Status: "unsupported", Alternative: "stage through a local directory"},
		},
		NonGoals: []string{
			"native Windows controller runtime", "interactive PTY or TUI forwarding", "port forwarding",
			"native Windows controller config owner, mode, ACL, or POSIX no-follow guarantees",
			"full ACL, xattr, owner, or sparse-file fidelity", "multi-tenant remote sandboxing",
		},
	}
}
