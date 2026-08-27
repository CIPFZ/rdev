// Package support exposes the machine-readable support baseline. Runtime tiers
// are intentionally separate from build coverage so cross-compilation cannot be
// mistaken for certification.
package support

const SchemaVersion = 1

type Platform struct {
	OS     string `json:"os"`
	Arch   string `json:"arch"`
	Tier   string `json:"tier"`
	Status string `json:"status"`
}

type Matrix struct {
	SchemaVersion       int        `json:"schema_version"`
	Local               []Platform `json:"local"`
	Remote              []Platform `json:"remote"`
	RequiredSSHFeatures []string   `json:"required_ssh_features"`
	RequiredLocalTools  []string   `json:"required_local_tools"`
	NonGoals            []string   `json:"non_goals"`
}

func Snapshot() Matrix {
	return Matrix{
		SchemaVersion: SchemaVersion,
		Local: []Platform{
			{OS: "darwin", Arch: "arm64", Tier: "tier1", Status: "development/test baseline; fd-native config ACL checks require cgo, otherwise fail closed"},
			{OS: "darwin", Arch: "amd64", Tier: "build", Status: "cross-build only"},
			{OS: "linux", Arch: "amd64", Tier: "build", Status: "cross-build only"},
			{OS: "linux", Arch: "arm64", Tier: "build", Status: "cross-build only"},
		},
		Remote: []Platform{
			{OS: "linux", Arch: "amd64", Tier: "tier1", Status: "Ubuntu real-SSH bootstrap, exec, file, and rsync baseline"},
			{OS: "linux", Arch: "arm64", Tier: "build", Status: "agent cross-build only"},
			{OS: "darwin", Arch: "amd64", Tier: "build", Status: "agent cross-build only"},
			{OS: "darwin", Arch: "arm64", Tier: "build", Status: "agent cross-build only"},
		},
		RequiredSSHFeatures: []string{"BatchMode", "ControlMaster", "ControlPath", "ControlPersist"},
		RequiredLocalTools:  []string{"ssh", "rsync for sync operations"},
		NonGoals: []string{
			"native Windows runtime", "interactive PTY or TUI forwarding", "port forwarding",
			"native Windows config owner, mode, ACL, or POSIX no-follow guarantees",
			"full ACL, xattr, owner, or sparse-file fidelity", "multi-tenant remote sandboxing",
		},
	}
}
