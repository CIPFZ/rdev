// Package compat projects the validators and wire registries used by this build.
// It contains no configured hosts, principals, paths or secret values.
package compat

import (
	"reflect"
	"sort"
	"strings"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/buildinfo"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/state"
)

const SchemaVersion = 1

type Format struct {
	Name       string   `json:"name"`
	Current    int      `json:"current,omitempty"`
	Minimum    int      `json:"minimum,omitempty"`
	Versioning string   `json:"versioning"`
	Legacy     string   `json:"legacy"`
	Migration  string   `json:"migration"`
	Unknown    string   `json:"unknown"`
	Fields     []string `json:"fields,omitempty"`
}

type Protocol struct {
	Name        string              `json:"name"`
	Encoding    string              `json:"encoding"`
	Range       proto.ProtocolRange `json:"range"`
	Features    []proto.Feature     `json:"features,omitempty"`
	Previous    string              `json:"previous"`
	Unsupported string              `json:"unsupported"`
}

type ErrorContract struct {
	Version int                     `json:"version"`
	Codes   []proto.ErrorDescriptor `json:"codes"`
	Unknown string                  `json:"unknown"`
	Changes string                  `json:"changes"`
}

type Contract struct {
	SchemaVersion   int            `json:"schema_version"`
	Release         string         `json:"release"`
	Protocols       []Protocol     `json:"protocols"`
	Formats         []Format       `json:"formats"`
	Errors          ErrorContract  `json:"errors"`
	Upgrade         string         `json:"upgrade"`
	BreakingChanges []string       `json:"breaking_changes"`
	Timeouts        map[string]int `json:"timeout_seconds"`
}

// Current reads versions from the exact constants used by readers, writers and
// negotiation. Field inventories are derived from the actual config structs.
func Current() Contract {
	return Contract{
		SchemaVersion: SchemaVersion, Release: buildinfo.Version,
		Timeouts: map[string]int{"exec_default": proto.DefaultExecTimeoutSeconds, "job_wait_default": proto.DefaultJobWaitSeconds, "new_job_wall_default": proto.DefaultJobWallTimeoutSeconds, "hard_maximum": proto.MaxTimeoutSeconds},
		Protocols: []Protocol{
			{Name: "client_agent", Encoding: "ID-correlated NDJSON (not JSON-RPC)", Range: proto.CurrentHello().ProtocolRange(), Features: proto.SupportedFeatures(), Previous: "standalone protocol 2 unary fallback for supported operations; no v3 cancellation, streaming or deduplication guarantees; new job_start requires job_resource_envelope and rejects older peers that cannot enforce runtime budgets", Unsupported: "no intersecting range: reject before business requests; legacy missing min_version means exactly the advertised version; v3 missing required operation features is rejected"},
			{Name: "client_broker", Encoding: "hello followed by ID-correlated NDJSON (not JSON-RPC)", Range: proto.ProtocolRange{Min: proto.BrokerMinVersion, Max: proto.BrokerProtocolVersion}, Previous: "no protocol 0 compatibility; earlier releases with protocol 1 may connect but unsupported routes fail closed", Unsupported: "invalid/disjoint hello range: reject before authentication/dispatch; no private SSH fallback"},
			{Name: "broker_agent", Encoding: "ID-correlated NDJSON (not JSON-RPC)", Range: proto.ProtocolRange{Min: proto.TypedProtocolVersion, Max: proto.Version}, Features: proto.SupportedFeatures(), Previous: "protocol 2 explicitly rejected for shared requests because it cannot preserve principal identity; existing protocol 3 agents may serve only operations whose required features they advertise", Unsupported: "shared requests reject a legacy peer before business dispatch; new job_start requires job_resource_envelope"},
		},
		Formats: []Format{
			{Name: "host_config", Versioning: "unversioned JSON shape", Legacy: "existing hosts array remains readable and writable", Migration: "none; use only fields understood by every participating build", Unknown: "standalone ignores unknown JSON fields; administrator hosts-file rejects them; no future version support is implied", Fields: fields(reflect.TypeOf(session.HostConfigShape()), "")},
			{Name: "broker_config", Versioning: "unversioned JSON shape", Legacy: "existing omitted fields retain defaults", Migration: "none; new fields require a reader that recognizes them", Unknown: "reject unknown fields and trailing documents", Fields: fields(reflect.TypeOf(broker.Config{}), "")},
			{Name: "broker_policy", Versioning: "unversioned JSON owner-key to grant-key boolean map", Legacy: "existing operation-wide, capability and exact-host grants remain readable", Migration: "none; preserve exact owner keys and grant scope", Unknown: "duplicate keys, null and non-boolean grants are rejected; unknown grant strings are retained as data, but unsupported executable routes fail closed"},
			{Name: "project_trust", Current: session.TrustSchemaVersion, Minimum: session.TrustSchemaVersion, Versioning: "exact version", Legacy: "missing file creates an empty current store; missing/zero version is rejected", Migration: "none; retain project path and digest binding", Unknown: "different version rejected"},
			{Name: "agent_state", Current: state.CurrentSchemaVersion, Versioning: "schema_version", Legacy: "missing record schema_version is legacy 0; explicit zero is corrupt", Migration: "explicit state migrate from legacy 0 to current, forward only, with backup; state repair/quarantine after review; no automatic downgrade", Unknown: "future manifest fails migration closed; future/corrupt records are reported invalid and not silently migrated"},
			{Name: "fleet_inventory", Current: broker.FleetInventorySchemaVersion, Minimum: broker.FleetInventorySchemaVersion, Versioning: "exact schema with CAS revision", Legacy: "explicit administrator import from trusted hosts configuration; no project inventory", Migration: "inventory import/update through broker; HostIDs and retired identity tombstones remain durable", Unknown: "future/corrupt schema, unknown fields and partial updates rejected", Fields: fields(reflect.TypeOf(broker.FleetInventory{}), "")},
			persistent("broker_fleet", broker.FleetSchemaVersion, "none; immutable target snapshots and attempt identities; submitted mutations are reconciled, never automatically replayed"),
			persistent("broker_jobs", broker.JobRegistrySchemaVersion, "legacy JSON array is readable; next write creates current envelope"),
			persistent("broker_mutations", broker.MutationSchemaVersion, "none; restart preserves ambiguous outcomes without replay"),
			persistent("broker_job_events", broker.JobEventSchemaVersion, "none"),
			persistent("broker_secrets", broker.SecretSchemaVersion, "none; absent archive with dependent state fails closed"),
			persistent("broker_audit_continuity", broker.AuditContinuitySchemaVersion, "none"),
			{Name: "broker_audit", Current: broker.AuditSchemaVersion, Versioning: "schema", Legacy: "legacy owner display identities remain admin-readable on disk but are omitted from principal queries", Migration: "no lossless owner migration; omission marker is returned", Unknown: "non-current records never attributed to a principal"},
		},
		Errors:          ErrorContract{Version: proto.ErrorContractVersion, Codes: proto.ErrorDescriptors(), Unknown: "incoming unknown codes or changed code/category/message/retry/terminal combinations fail ErrorEnvelope.Validate; constructors map unknown local codes to internal.failure", Changes: "existing code/retry/execution meaning is stable; changed semantics require contract and protocol/feature gating; unknown codes are not treated as retryable"},
		Upgrade:         "N/N-1 refers to the explicit protocol/schema ranges, not an arbitrary release-version promise. Agent downgrade protection compares known clean build commit dates. Automatic upgrade/rollback combination certification remains Phase8.",
		BreakingChanges: []string{"CLI unknown/duplicate/conflicting flags and malformed numbers now fail", "exec, job wait and new job wall timeout: omitted/zero selects the shared bounded default; negative or unlimited values are rejected", "new job_start requires the negotiated job_resource_envelope feature; older peers must update before starting new jobs; read/ping and existing job observation/control retain their independently checked compatibility", "new job wall timeout omitted/zero is now bounded to one hour; existing supervisors are not retroactively changed"},
	}
}

func persistent(name string, version int, legacy string) Format {
	return Format{Name: name, Current: version, Minimum: version, Versioning: "exact schema", Legacy: legacy, Migration: "no downgrade; preserve durable state and use a compatible reader", Unknown: "unsupported schema rejected before registry publication"}
}

func fields(t reflect.Type, prefix string) []string {
	var out []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			continue
		}
		name = prefix + name
		out = append(out, name)
		ft := f.Type
		if ft.Kind() == reflect.Slice {
			ft = ft.Elem()
			name += "[]"
		}
		if ft.Kind() == reflect.Struct {
			out = append(out, fields(ft, name+".")...)
		}
	}
	sort.Strings(out)
	return out
}
