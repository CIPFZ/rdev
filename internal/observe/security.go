// Package observe defines the stable, low-cardinality observability seams used
// while rdev's full status/doctor subsystem is built out.
package observe

import (
	"crypto/sha256"
	"fmt"
	"sync"
)

const SchemaVersion = 3

type SecurityReason string

const (
	ReasonProjectUntrusted SecurityReason = "project_untrusted"
	ReasonConfigInvalid    SecurityReason = "config_invalid"
	ReasonConfigSymlink    SecurityReason = "config_symlink"
	ReasonDestination      SecurityReason = "destination_invalid"
	ReasonRemoteDir        SecurityReason = "remote_dir_invalid"
)

var securityReasons = [...]SecurityReason{
	ReasonProjectUntrusted,
	ReasonConfigInvalid,
	ReasonConfigSymlink,
	ReasonDestination,
	ReasonRemoteDir,
}

type SecretReason string

const (
	ReasonSecretReadFailed SecretReason = "read_failed"
	ReasonSecretTruncated  SecretReason = "truncated"
	ReasonSecretBinary     SecretReason = "binary"
	ReasonSecretEmpty      SecretReason = "empty"
	ReasonSecretTooShort   SecretReason = "too_short"
	ReasonSecretInvalid    SecretReason = "invalid"
)

var secretReasons = [...]SecretReason{
	ReasonSecretReadFailed,
	ReasonSecretTruncated,
	ReasonSecretBinary,
	ReasonSecretEmpty,
	ReasonSecretTooShort,
	ReasonSecretInvalid,
}

type ConnectionSecurityState string

const (
	SecurityCold         ConnectionSecurityState = "cold"
	SecurityInitializing ConnectionSecurityState = "initializing"
	SecurityReady        ConnectionSecurityState = "ready"
	SecurityFailed       ConnectionSecurityState = "failed"
)

var connectionSecurityStates = [...]ConnectionSecurityState{
	SecurityCold, SecurityInitializing, SecurityReady, SecurityFailed,
}

type Event struct {
	Name         string                  `json:"name"`
	Level        string                  `json:"level"`
	Reason       SecurityReason          `json:"reason,omitempty"`
	SecretReason SecretReason            `json:"secret_reason,omitempty"`
	State        ConnectionSecurityState `json:"state,omitempty"`
	TargetHash   string                  `json:"target_hash,omitempty"`
}

type Sink interface {
	Log(Event)
}

type Snapshot struct {
	SchemaVersion                 int               `json:"schema_version"`
	SecurityRejects               map[string]uint64 `json:"security_rejects"`
	SecretLoadFailures            map[string]uint64 `json:"secret_load_failures"`
	SecretRejections              map[string]uint64 `json:"secret_rejections"`
	ConnectionSecurityTransitions map[string]uint64 `json:"connection_security_transitions"`
	ProjectApprovals              uint64            `json:"project_approvals"`
	RedactionHits                 uint64            `json:"redaction_hits"`
	RequestEvents                 map[string]uint64 `json:"request_events"`
	ProtocolEvents                map[string]uint64 `json:"protocol_events"`
	ResourceEvents                map[string]uint64 `json:"resource_events"`
	DedupeEvents                  map[string]uint64 `json:"dedupe_events"`
	ConnectionEvents              map[string]uint64 `json:"connection_events"`
	ConnectionReasons             map[string]uint64 `json:"connection_reasons"`
}

// Registry accepts only enumerated reasons. Target identity is hashed for logs
// and never becomes a metric label, so arbitrary projects cannot grow series.
type Registry struct {
	mu                       sync.RWMutex
	rejects                  map[SecurityReason]uint64
	secretLoadFailures       map[SecretReason]uint64
	secretRejections         map[SecretReason]uint64
	connectionSecurityStates map[ConnectionSecurityState]uint64
	approvals                uint64
	redactionHits            uint64
	requestEvents            map[RequestEvent]uint64
	protocolEvents           map[ProtocolEvent]uint64
	resourceEvents           map[ResourceEvent]uint64
	dedupeEvents             map[DedupeEvent]uint64
	connectionEvents         map[ConnectionEvent]uint64
	connectionReasons        map[string]uint64
	sink                     Sink
}

func New(sink Sink) *Registry {
	return &Registry{
		rejects:                  make(map[SecurityReason]uint64),
		secretLoadFailures:       make(map[SecretReason]uint64),
		secretRejections:         make(map[SecretReason]uint64),
		connectionSecurityStates: make(map[ConnectionSecurityState]uint64),
		requestEvents:            make(map[RequestEvent]uint64),
		protocolEvents:           make(map[ProtocolEvent]uint64),
		resourceEvents:           make(map[ResourceEvent]uint64),
		dedupeEvents:             make(map[DedupeEvent]uint64),
		connectionEvents:         make(map[ConnectionEvent]uint64),
		connectionReasons:        make(map[string]uint64),
		sink:                     sink,
	}
}

func validReason(reason SecurityReason) bool {
	for _, candidate := range securityReasons {
		if reason == candidate {
			return true
		}
	}
	return false
}

func hashTarget(target string) string {
	sum := sha256.Sum256([]byte(target))
	return fmt.Sprintf("%x", sum[:8])
}

func validSecretReason(reason SecretReason) bool {
	for _, candidate := range secretReasons {
		if reason == candidate {
			return true
		}
	}
	return false
}

func validConnectionSecurityState(state ConnectionSecurityState) bool {
	for _, candidate := range connectionSecurityStates {
		if state == candidate {
			return true
		}
	}
	return false
}

func (r *Registry) Reject(reason SecurityReason, target string) {
	if r == nil || !validReason(reason) {
		return
	}
	r.mu.Lock()
	r.rejects[reason]++
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{
			Name: "security.config_rejected", Level: "warn", Reason: reason,
			TargetHash: hashTarget(target),
		})
	}
}

func (r *Registry) ProjectApproved(target string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.approvals++
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{
			Name: "security.project_approved", Level: "info",
			TargetHash: hashTarget(target),
		})
	}
}

func (r *Registry) SecretLoadFailed(reason SecretReason, target string) {
	if r == nil || !validSecretReason(reason) {
		return
	}
	r.mu.Lock()
	r.secretLoadFailures[reason]++
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{Name: "security.secret_load_failed", Level: "warn", SecretReason: reason, TargetHash: hashTarget(target)})
	}
}

func (r *Registry) SecretRejected(reason SecretReason, target string) {
	if r == nil || !validSecretReason(reason) {
		return
	}
	r.mu.Lock()
	r.secretRejections[reason]++
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{Name: "security.secret_rejected", Level: "warn", SecretReason: reason, TargetHash: hashTarget(target)})
	}
}

func (r *Registry) ConnectionSecurityStateChanged(state ConnectionSecurityState, target string) {
	if r == nil || !validConnectionSecurityState(state) {
		return
	}
	r.mu.Lock()
	r.connectionSecurityStates[state]++
	sink := r.sink
	r.mu.Unlock()
	if sink != nil {
		sink.Log(Event{Name: "connection.security_state_changed", Level: "info", State: state, TargetHash: hashTarget(target)})
	}
}

func (r *Registry) RedactionHit(count uint64) {
	if r == nil || count == 0 {
		return
	}
	r.mu.Lock()
	r.redactionHits += count
	r.mu.Unlock()
}

func (r *Registry) Snapshot() Snapshot {
	out := Snapshot{
		SchemaVersion:                 SchemaVersion,
		SecurityRejects:               make(map[string]uint64),
		SecretLoadFailures:            make(map[string]uint64),
		SecretRejections:              make(map[string]uint64),
		ConnectionSecurityTransitions: make(map[string]uint64),
		RequestEvents:                 make(map[string]uint64),
		ProtocolEvents:                make(map[string]uint64),
		ResourceEvents:                make(map[string]uint64),
		DedupeEvents:                  make(map[string]uint64),
		ConnectionEvents:              make(map[string]uint64),
		ConnectionReasons:             make(map[string]uint64),
	}
	if r == nil {
		return out
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, reason := range securityReasons {
		out.SecurityRejects[string(reason)] = r.rejects[reason]
	}
	for _, reason := range secretReasons {
		out.SecretLoadFailures[string(reason)] = r.secretLoadFailures[reason]
		out.SecretRejections[string(reason)] = r.secretRejections[reason]
	}
	for _, state := range connectionSecurityStates {
		out.ConnectionSecurityTransitions[string(state)] = r.connectionSecurityStates[state]
	}
	for _, event := range requestEvents {
		out.RequestEvents[string(event)] = r.requestEvents[event]
	}
	for _, event := range protocolEvents {
		out.ProtocolEvents[string(event)] = r.protocolEvents[event]
	}
	for _, event := range resourceEvents {
		out.ResourceEvents[string(event)] = r.resourceEvents[event]
	}
	for _, event := range dedupeEvents {
		out.DedupeEvents[string(event)] = r.dedupeEvents[event]
	}
	for _, event := range connectionEvents {
		out.ConnectionEvents[string(event)] = r.connectionEvents[event]
	}
	for _, reason := range []string{"canceled", "timeout", "auth", "network", "resource", "unknown", "LRU", "idle TTL", "drain complete", "health_failed", "explicit"} {
		out.ConnectionReasons[reason] = r.connectionReasons[reason]
	}
	out.ProjectApprovals = r.approvals
	out.RedactionHits = r.redactionHits
	return out
}
