package artifact

import (
	"errors"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/state"
)

type CompatibilityFormat struct {
	Name       string   `json:"name"`
	Current    int      `json:"current,omitempty"`
	Minimum    int      `json:"minimum,omitempty"`
	Versioning string   `json:"versioning"`
	Legacy     string   `json:"legacy"`
	Migration  string   `json:"migration"`
	Unknown    string   `json:"unknown"`
	Fields     []string `json:"fields,omitempty"`
}

type CompatibilityProtocol struct {
	Name        string              `json:"name"`
	Encoding    string              `json:"encoding"`
	Range       proto.ProtocolRange `json:"range"`
	Features    []proto.Feature     `json:"features,omitempty"`
	Previous    string              `json:"previous"`
	Unsupported string              `json:"unsupported"`
}

type CompatibilityErrors struct {
	Version int                     `json:"version"`
	Codes   []proto.ErrorDescriptor `json:"codes"`
	Unknown string                  `json:"unknown"`
	Changes string                  `json:"changes"`
}

type Compatibility struct {
	SchemaVersion   int                     `json:"schema_version"`
	Release         string                  `json:"release"`
	Protocols       []CompatibilityProtocol `json:"protocols"`
	Formats         []CompatibilityFormat   `json:"formats"`
	Errors          CompatibilityErrors     `json:"errors"`
	Upgrade         string                  `json:"upgrade"`
	BreakingChanges []string                `json:"breaking_changes"`
	Timeouts        map[string]int          `json:"timeout_seconds"`
}

// DecodeCompatibility is shared with compat's generated machine contract. A
// signature does not make a future or malformed schema meaningful to this reader.
func DecodeCompatibility(raw []byte, version string) error {
	var c Compatibility
	if err := Decode(raw, &c); err != nil {
		return err
	}
	if c.SchemaVersion != 1 || c.Release != version || c.Errors.Version != proto.ErrorContractVersion || len(c.Protocols) != 3 || len(c.Formats) == 0 || len(c.Formats) > 64 {
		return errors.New("unsupported compatibility contract")
	}
	names := map[string]bool{}
	for _, p := range c.Protocols {
		if names[p.Name] || (p.Name != "client_agent" && p.Name != "client_broker" && p.Name != "broker_agent") || p.Range.Validate() != nil {
			return errors.New("invalid compatibility protocol")
		}
		names[p.Name] = true
		features := map[proto.Feature]bool{}
		for _, f := range p.Features {
			if features[f] || !proto.IsKnownFeature(f) {
				return errors.New("unknown or duplicate compatibility feature")
			}
			features[f] = true
		}
	}
	names = map[string]bool{}
	for _, f := range c.Formats {
		if f.Name == "" || names[f.Name] || f.Current < 0 || f.Minimum < 0 || f.Minimum > f.Current {
			return errors.New("invalid state format contract")
		}
		names[f.Name] = true
		if f.Name == "agent_state" && f.Current != state.CurrentSchemaVersion {
			return errors.New("unsupported agent state reader contract")
		}
	}
	if !names["agent_state"] {
		return errors.New("missing agent state reader contract")
	}
	known := map[proto.ErrorCode]proto.ErrorDescriptor{}
	for _, d := range proto.ErrorDescriptors() {
		known[d.Code] = d
	}
	seen := map[proto.ErrorCode]bool{}
	for _, d := range c.Errors.Codes {
		if seen[d.Code] || known[d.Code] != d {
			return errors.New("unknown compatibility error contract")
		}
		seen[d.Code] = true
	}
	if len(seen) == 0 {
		return errors.New("missing error contract")
	}
	defaults := map[string]int{"exec_default": proto.DefaultExecTimeoutSeconds, "job_wait_default": proto.DefaultJobWaitSeconds, "new_job_wall_default": proto.DefaultJobWallTimeoutSeconds, "hard_maximum": proto.MaxTimeoutSeconds}
	if len(c.Timeouts) != len(defaults) {
		return errors.New("unknown timeout contract")
	}
	for k, v := range defaults {
		if c.Timeouts[k] != v {
			return errors.New("unsupported timeout contract")
		}
	}
	return nil
}
