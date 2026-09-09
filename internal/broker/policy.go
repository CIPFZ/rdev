package broker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Decision is a value captured before queuing. The digest names the complete
// policy snapshot that authorized this request, even if an administrator changes
// grants while the request waits for a worker.
type Decision struct {
	Allow      bool   `json:"allow"`
	Reason     string `json:"reason"`
	Digest     string `json:"digest"`
	Capability string `json:"capability,omitempty"`
	Host       string `json:"host,omitempty"`
}

type Policy struct {
	mu     sync.RWMutex
	grants map[string]map[string]bool
	path   string
	digest string
	failed error
	write  func(string, map[string]map[string]bool) error
}

func NewPolicy() *Policy {
	p := &Policy{grants: make(map[string]map[string]bool), write: savePolicy}
	p.digest = policyDigest(p.grants)
	return p
}

func policyDigest(grants map[string]map[string]bool) string {
	data, _ := json.Marshal(grants) // encoding/json sorts string map keys.
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (p *Policy) update(owner, operation string, grant bool) error {
	if owner == "" || len(owner) > 512 || operation == "" || len(operation) > 2048 {
		return fmt.Errorf("invalid policy grant")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed != nil {
		return p.failed
	}
	next := make(map[string]map[string]bool, len(p.grants))
	for key, operations := range p.grants {
		values := make(map[string]bool, len(operations))
		for operation, allow := range operations {
			values[operation] = allow
		}
		next[key] = values
	}
	if grant {
		if next[owner] == nil {
			next[owner] = make(map[string]bool)
		}
		next[owner][operation] = true
	} else {
		delete(next[owner], operation)
		if len(next[owner]) == 0 {
			delete(next, owner)
		}
	}
	if p.path != "" {
		if err := p.write(p.path, next); err != nil {
			var uncertain *policyCommitUncertain
			if errors.As(err, &uncertain) {
				p.failed = err
			}
			return err
		}
	}
	// A failed persistence operation never publishes a permission change.
	p.grants = next
	p.digest = policyDigest(next)
	return nil
}

// Grant authorizes an operation across all targets. Administrators should use
// GrantHost when only one host is intended. Existing files retain their explicit
// operation-wide grants; target scope is never inferred from a request hint.
func (p *Policy) Grant(owner, operation string) error  { return p.update(owner, operation, true) }
func (p *Policy) Revoke(owner, operation string) error { return p.update(owner, operation, false) }
func (p *Policy) Decide(owner, operation string) Decision {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.decision(p.grants[owner][operation], "", "")
}
func (p *Policy) decision(allow bool, capability, host string) Decision {
	reason := "denied by default"
	if allow {
		reason = "granted"
	}
	if p.failed != nil {
		allow, reason = false, "policy storage unavailable"
	}
	return Decision{Allow: allow, Reason: reason, Digest: p.digest, Capability: capability, Host: host}
}

func capabilityKey(capability, operation string) string {
	return "@cap:" + capability + "\x00" + operation
}
func (p *Policy) GrantCapability(owner, capability, operation string) error {
	if err := validateCapability(capability, operation); err != nil {
		return err
	}
	return p.Grant(owner, capabilityKey(capability, operation))
}
func validateCapability(capability, operation string) error {
	if capability == "" || operation == "" || len(capability) > 128 || len(operation) > 128 || strings.ContainsAny(capability+operation, "\x00\r\n") {
		return fmt.Errorf("invalid capability or operation")
	}
	return nil
}
func (p *Policy) DecideCapability(owner, capability, operation string) Decision {
	return p.Decide(owner, capabilityKey(capability, operation))
}

func hostGrantKey(host, capability, operation string) string {
	data, _ := json.Marshal([3]string{host, capability, operation})
	return "@host:" + string(data)
}
func (p *Policy) GrantHost(owner, host, capability, operation string) error {
	if err := validateHostGrant(host, capability, operation); err != nil {
		return err
	}
	return p.Grant(owner, hostGrantKey(host, capability, operation))
}
func (p *Policy) RevokeHost(owner, host, capability, operation string) error {
	if err := validateHostGrant(host, capability, operation); err != nil {
		return err
	}
	return p.Revoke(owner, hostGrantKey(host, capability, operation))
}
func validateHostGrant(host, capability, operation string) error {
	if host == "" || len(host) > 512 || strings.ContainsAny(host, "\x00\r\n") {
		return fmt.Errorf("invalid grant host")
	}
	return validateCapability(capability, operation)
}

// CapabilityForOperation is a server-owned classification. A caller cannot
// substitute an unrelated granted capability for the operation being executed.
func CapabilityForOperation(operation string) string {
	switch operation {
	case "mutation.status":
		return "mutation.read"
	case "job.events":
		return "job"
	case "exec":
		return "exec"
	case "read_file", "list":
		return "file.read"
	case "write_file":
		return "file.write"
	case "job_start", "job_list", "job_status", "job_logs", "job_stop", "job_wait", "job_rm":
		return "job"
	case "sync.push", "sync.pull", "sync.delete":
		return "sync"
	case "secret.set", "secret.delete", "secret.list", "secret.use", "secret.set_from_file":
		return "secret"
	case "fleet.plan", "fleet.execute", "fleet.approve":
		return "fleet"
	default:
		return operation
	}
}
func (p *Policy) DecideRequest(owner, operation, host string) Decision {
	capability := CapabilityForOperation(operation)
	p.mu.RLock()
	defer p.mu.RUnlock()
	grants := p.grants[owner]
	allow := grants[operation] || grants[capabilityKey(capability, operation)]
	if host != "" {
		allow = allow || grants[hostGrantKey(host, capability, operation)]
	}
	return p.decision(allow, capability, host)
}

type policyCommitUncertain struct{ cause error }

func (e *policyCommitUncertain) Error() string {
	return "policy commit durability uncertain; administrator recovery required"
}
func (e *policyCommitUncertain) Unwrap() error { return e.cause }

func savePolicy(path string, grants map[string]map[string]bool) error {
	data, err := json.Marshal(grants)
	if err != nil {
		return err
	}
	if len(data) > 4<<20 {
		return fmt.Errorf("policy exceeds size limit")
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".rdev-policy-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return &policyCommitUncertain{cause: err}
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return &policyCommitUncertain{cause: err}
	}
	return nil
}
func (p *Policy) Save(path string) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	// Never overwrite a possibly committed snapshot during shutdown.
	if p.failed != nil {
		return p.failed
	}
	return savePolicy(path, p.grants)
}

// parsePolicy rejects ambiguous duplicate keys as well as null, oversized,
// malformed and non-boolean grants. No partial map is published on error.
func parsePolicy(data []byte) (map[string]map[string]bool, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("policy requires an object")
	}
	grants := make(map[string]map[string]bool)
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return nil, err
		}
		owner, ok := token.(string)
		if !ok || owner == "" || len(owner) > 512 {
			return nil, fmt.Errorf("invalid policy owner")
		}
		if _, exists := grants[owner]; exists {
			return nil, fmt.Errorf("duplicate policy owner")
		}
		token, err = dec.Token()
		if err != nil || token != json.Delim('{') {
			return nil, fmt.Errorf("policy owner requires an object")
		}
		operations := make(map[string]bool)
		for dec.More() {
			token, err = dec.Token()
			if err != nil {
				return nil, err
			}
			operation, ok := token.(string)
			if !ok || operation == "" || len(operation) > 2048 {
				return nil, fmt.Errorf("invalid policy operation")
			}
			if _, exists := operations[operation]; exists {
				return nil, fmt.Errorf("duplicate policy operation")
			}
			token, err = dec.Token()
			if err != nil {
				return nil, err
			}
			allow, ok := token.(bool)
			if !ok {
				return nil, fmt.Errorf("policy grant requires a boolean")
			}
			operations[operation] = allow
		}
		token, err = dec.Token()
		if err != nil || token != json.Delim('}') || len(operations) == 0 {
			return nil, fmt.Errorf("invalid policy operations")
		}
		grants[owner] = operations
	}
	token, err = dec.Token()
	if err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("invalid policy object")
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("policy requires one object")
	}
	return grants, nil
}
func (p *Policy) Load(path string) error {
	data, err := ReadPrivateFile(path, 4<<20)
	if err != nil {
		return err
	}
	grants, err := parsePolicy(data)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.grants = grants
	p.digest = policyDigest(grants)
	p.failed = nil
	return nil
}
func (p *Policy) ConfigurePersistence(path string) error {
	// Configuration is startup-only, before callers can mutate grants.
	if err := p.Load(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := savePolicy(path, p.grants); err != nil {
		return err
	}
	p.path = path
	return nil
}

// DecideWireRequest captures execution and secret-use authorization under one
// policy lock. A concurrent revoke cannot splice together two policy snapshots.
func (p *Policy) DecideWireRequest(owner, operation, host string, useSecrets bool) Decision {
	return p.decideWireRequest(owner, operation, host, useSecrets, false)
}

func (p *Policy) decideWireRequest(owner, operation, host string, useSecrets, syncDelete bool) Decision {
	capability := CapabilityForOperation(operation)
	p.mu.RLock()
	defer p.mu.RUnlock()
	grants := p.grants[owner]
	allowed := func(op, cap string) bool {
		return grants[op] || grants[capabilityKey(cap, op)] || host != "" && grants[hostGrantKey(host, cap, op)]
	}
	return p.decision(allowed(operation, capability) && (!useSecrets || allowed("secret.use", "secret")) && (!syncDelete || allowed("sync.delete", "sync")) && (operation != "secret.set_from_file" || allowed("read_file", "file.read")), capability, host)
}
func (s *Service) DecideBrokerRequest(req Request) Decision {
	if err := req.Owner.Validate(); err != nil {
		return Decision{Reason: "invalid owner"}
	}
	if req.Operation == "support" {
		s.policy.mu.RLock()
		defer s.policy.mu.RUnlock()
		return s.policy.decision(true, "support", req.Host)
	}
	return s.policy.decideWireRequest(req.Owner.Key(), req.Operation, req.Host, wireUsesSecrets(req.Wire), isSyncOperation(req.Operation) && req.Sync != nil && req.Sync.Delete)
}
