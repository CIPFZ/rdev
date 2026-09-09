package broker

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/CIPFZ/rdev/internal/session"
)

const (
	FleetInventorySchemaVersion = 1
	FleetMaxHosts               = 1024
	FleetMaxAliases             = 16
	FleetMaxLabels              = 32
	fleetMaxRetiredIDs          = 65536
	FleetMaxInventoryBytes      = 8 << 20
	fleetMaxSelectorBytes       = 64 << 10
)

var (
	ErrFleetInventoryConflict = errors.New("fleet inventory revision conflict")
	ErrFleetTargetsNotFound   = errors.New("fleet targets unavailable")
	ErrFleetTargetChanged     = errors.New("fleet target identity changed")
)

// FleetHost is trusted metadata referring to the existing global host registry.
// TargetDigest binds connection and session configuration, without an alias or
// a process-local generation. It is never a second connection configuration.
type FleetHost struct {
	HostID       string            `json:"host_id"`
	Aliases      []string          `json:"aliases"`
	Labels       map[string]string `json:"labels"`
	TargetDigest string            `json:"target_digest"`
}

type FleetInventory struct {
	Schema     int         `json:"schema"`
	Revision   uint64      `json:"revision"`
	Records    []FleetHost `json:"records"`
	RetiredIDs []string    `json:"retired_ids"`
}

// FleetInventoryStore serializes whole-document compare-and-swap updates. The
// broker's existing exclusive state-root lock supplies process exclusion.
type FleetInventoryStore struct {
	mu       sync.RWMutex
	snapshot FleetInventory
	path     string
	failed   error
	write    func(string, FleetInventory) error
}

func newFleetInventoryStore() *FleetInventoryStore {
	return &FleetInventoryStore{snapshot: FleetInventory{Schema: FleetInventorySchemaVersion, Revision: 1, Records: []FleetHost{}, RetiredIDs: []string{}}, write: saveFleetInventory}
}

func cloneFleetHost(h FleetHost) FleetHost {
	h.Aliases = append([]string{}, h.Aliases...)
	labels := make(map[string]string, len(h.Labels))
	for k, v := range h.Labels {
		labels[k] = v
	}
	h.Labels = labels
	return h
}

func cloneFleetInventory(in FleetInventory) FleetInventory {
	out := in
	out.Records = make([]FleetHost, len(in.Records))
	for i, h := range in.Records {
		out.Records[i] = cloneFleetHost(h)
	}
	out.RetiredIDs = append([]string{}, in.RetiredIDs...)
	return out
}

// ConfigureFleetInventory loads the trusted state file, or durably creates an
// empty inventory. Importing global hosts is a separate administrator action.
// Existing stale bindings remain readable so plans can report identity drift.
func (s *Service) ConfigureFleetInventory(path string) error {
	if path == "" {
		return errors.New("fleet inventory persistence path required")
	}
	r := s.inventory
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.path != "" {
		return errors.New("fleet inventory already configured")
	}
	data, err := ReadPrivateFile(path, FleetMaxInventoryBytes)
	next := cloneFleetInventory(r.snapshot)
	if os.IsNotExist(err) {
		if err := r.write(path, next); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		next, err = parseFleetInventory(data)
		if err != nil {
			return err
		}
	}
	r.snapshot, r.path, r.failed = next, path, nil
	return nil
}

func (s *Service) FleetInventorySnapshot() FleetInventory {
	r := s.inventory
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneFleetInventory(r.snapshot)
}

func validFleetHex(v string, size int) bool {
	if len(v) != size {
		return false
	}
	for _, c := range v {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validFleetAlias(v string) bool {
	if len(v) == 0 || len(v) > 256 {
		return false
	}
	for _, c := range v {
		if c < 0x21 || c > 0x7e || strings.ContainsRune(",=&", c) {
			return false
		}
	}
	return true
}

// Label keys and values are case-sensitive ASCII tokens. Owner labels are
// descriptive only; every discovery and execution decision uses policy grants.
func validFleetLabel(v string, max int) bool {
	if len(v) == 0 || len(v) > max {
		return false
	}
	for i, c := range v {
		alphaNum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alphaNum && (i == 0 || !strings.ContainsRune("._-/", c)) {
			return false
		}
	}
	return true
}

func validateFleetInventory(in FleetInventory) error {
	if in.Schema != FleetInventorySchemaVersion {
		return errors.New("unsupported fleet inventory schema")
	}
	if in.Revision == 0 || in.Records == nil || in.RetiredIDs == nil || len(in.Records) > FleetMaxHosts || len(in.RetiredIDs) > fleetMaxRetiredIDs {
		return errors.New("invalid fleet inventory bounds")
	}
	ids, aliases, targets := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, id := range in.RetiredIDs {
		if !validFleetHex(id, 32) || ids[id] {
			return errors.New("invalid or duplicate retired host identity")
		}
		ids[id] = true
	}
	for _, h := range in.Records {
		if !validFleetHex(h.HostID, 32) || ids[h.HostID] || !validFleetHex(h.TargetDigest, 64) || targets[h.TargetDigest] {
			return errors.New("invalid or duplicate fleet host identity")
		}
		ids[h.HostID], targets[h.TargetDigest] = true, true
		if len(h.Aliases) == 0 || len(h.Aliases) > FleetMaxAliases || h.Labels == nil || len(h.Labels) > FleetMaxLabels {
			return errors.New("invalid fleet host metadata bounds")
		}
		for _, alias := range h.Aliases {
			if !validFleetAlias(alias) || aliases[alias] {
				return errors.New("invalid or duplicate fleet alias")
			}
			aliases[alias] = true
		}
		labelKeys := make(map[string]bool, len(h.Labels))
		for k, v := range h.Labels {
			folded := strings.ToLower(k)
			if !validFleetLabel(k, 63) || !validFleetLabel(v, 128) || labelKeys[folded] {
				return errors.New("invalid fleet label")
			}
			labelKeys[folded] = true
		}
	}
	return nil
}

func sortFleetInventory(in *FleetInventory) {
	for i := range in.Records {
		sort.Strings(in.Records[i].Aliases)
	}
	sort.Slice(in.Records, func(i, j int) bool { return in.Records[i].HostID < in.Records[j].HostID })
	sort.Strings(in.RetiredIDs)
}

func parseFleetInventory(data []byte) (FleetInventory, error) {
	var result FleetInventory
	if len(data) > FleetMaxInventoryBytes {
		return result, errors.New("fleet inventory size limit exceeded")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := rejectDuplicateJSON(dec); err != nil {
		return result, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return result, errors.New("fleet inventory requires one document")
	}
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		return result, err
	}
	if err := validateFleetInventory(result); err != nil {
		return result, err
	}
	sortFleetInventory(&result)
	return result, nil
}

func fleetSnapshotDigest(snapshot session.HostSnapshot) (string, error) {
	if snapshot.Scope != session.ScopeGlobal {
		return "", errors.New("fleet requires a trusted global host")
	}
	snapshot.Host.Name = ""
	snapshot.Generation = 0
	snapshot.Fingerprint, snapshot.ConnectionFingerprint = "", ""
	// Registry normalization handles destination/IPv6; normalize the equivalent
	// default state-directory spelling as well.
	if snapshot.Host.RemoteDir == "" || snapshot.Host.RemoteDir == "~/.cache/rdev" {
		snapshot.Host.RemoteDir = ".cache/rdev"
	} else {
		snapshot.Host.RemoteDir = strings.TrimPrefix(snapshot.Host.RemoteDir, "~/")
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Service) FleetTargetIdentity(alias string) (string, error) {
	snapshot, err := s.client.Hosts.Inspect(alias)
	if err != nil {
		return "", ErrFleetTargetChanged
	}
	return fleetSnapshotDigest(snapshot)
}

func randomFleetHostID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

// FleetInventoryUpdate replaces all metadata with a revision CAS. HostIDs for
// new records are broker-generated (empty in the submitted record); supplied
// IDs must still be active. Deleted IDs become permanent bounded tombstones.
// RPC callers must separately possess the inventory administrator capability.
func (s *Service) FleetInventoryUpdate(expectedRevision uint64, hosts []FleetHost) (FleetInventory, error) {
	r := s.inventory
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed != nil {
		return FleetInventory{}, r.failed
	}
	if expectedRevision != r.snapshot.Revision {
		return FleetInventory{}, ErrFleetInventoryConflict
	}
	if expectedRevision == math.MaxUint64 || len(hosts) > FleetMaxHosts {
		return FleetInventory{}, errors.New("fleet inventory limit exceeded")
	}
	next := FleetInventory{Schema: FleetInventorySchemaVersion, Revision: expectedRevision + 1, Records: make([]FleetHost, len(hosts)), RetiredIDs: append([]string{}, r.snapshot.RetiredIDs...)}
	previous, used := map[string]FleetHost{}, map[string]bool{}
	for _, h := range r.snapshot.Records {
		previous[h.HostID], used[h.HostID] = h, true
	}
	for _, id := range next.RetiredIDs {
		used[id] = true
	}
	retained := make(map[string]bool)
	for i, submitted := range hosts {
		h := cloneFleetHost(submitted)
		if len(h.Aliases) == 0 || len(h.Aliases) > FleetMaxAliases {
			return FleetInventory{}, errors.New("invalid fleet aliases")
		}
		var digest string
		for _, alias := range h.Aliases {
			if !validFleetAlias(alias) {
				return FleetInventory{}, errors.New("invalid fleet alias")
			}
			current, err := s.FleetTargetIdentity(alias)
			if err != nil || digest != "" && current != digest {
				return FleetInventory{}, ErrFleetTargetChanged
			}
			digest = current
		}
		if h.TargetDigest != "" && h.TargetDigest != digest {
			return FleetInventory{}, ErrFleetTargetChanged
		}
		h.TargetDigest = digest
		if h.HostID == "" {
			for {
				id, err := randomFleetHostID()
				if err != nil {
					return FleetInventory{}, err
				}
				if !used[id] {
					h.HostID, used[id] = id, true
					break
				}
			}
		} else {
			old, ok := previous[h.HostID]
			if !ok || old.TargetDigest != digest {
				return FleetInventory{}, ErrFleetTargetChanged
			}
			retained[h.HostID] = true
		}
		next.Records[i] = h
	}
	for id := range previous {
		if !retained[id] {
			next.RetiredIDs = append(next.RetiredIDs, id)
		}
	}
	if err := validateFleetInventory(next); err != nil {
		return FleetInventory{}, err
	}
	sortFleetInventory(&next)
	if r.path != "" {
		if err := r.write(r.path, next); err != nil {
			var uncertain *fleetInventoryCommitUncertain
			if errors.As(err, &uncertain) {
				r.failed = err
			}
			return FleetInventory{}, err
		}
	}
	r.snapshot = next
	return cloneFleetInventory(next), nil
}

// FleetInventoryImport explicitly synchronizes metadata to trusted global
// registry aliases. Unchanged connection/session identities retain HostIDs and
// labels; replaced targets get new IDs and removed identities are retired.
func (s *Service) FleetInventoryImport(expectedRevision uint64) (FleetInventory, error) {
	current := s.FleetInventorySnapshot()
	if current.Revision != expectedRevision {
		return FleetInventory{}, ErrFleetInventoryConflict
	}
	existing := make(map[string]FleetHost, len(current.Records))
	for _, h := range current.Records {
		existing[h.TargetDigest] = h
	}
	grouped := make(map[string]FleetHost)
	for _, alias := range s.client.Hosts.Names() {
		snapshot, err := s.client.Hosts.Inspect(alias)
		if err != nil {
			return FleetInventory{}, ErrFleetTargetChanged
		}
		if snapshot.Scope != session.ScopeGlobal {
			continue
		}
		digest, err := fleetSnapshotDigest(snapshot)
		if err != nil {
			return FleetInventory{}, err
		}
		h, ok := grouped[digest]
		if !ok {
			h = cloneFleetHost(existing[digest])
			h.Aliases, h.TargetDigest = []string{}, digest
		}
		h.Aliases = append(h.Aliases, alias)
		grouped[digest] = h
	}
	hosts := make([]FleetHost, 0, len(grouped))
	for _, h := range grouped {
		hosts = append(hosts, h)
	}
	return s.FleetInventoryUpdate(expectedRevision, hosts)
}

// FleetDispatchIdentity reads one registry snapshot for both canonical identity
// validation and the exact digest consumed by DispatchApproved. Independent
// reads would allow a concurrent alias replacement to choose the new target.
func (s *Service) FleetDispatchIdentity(host FleetHost, alias string) (string, error) {
	r := s.inventory
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.failed != nil {
		return "", r.failed
	}
	found := false
	for _, h := range r.snapshot.Records {
		if h.HostID != host.HostID || h.TargetDigest != host.TargetDigest {
			continue
		}
		for _, candidate := range h.Aliases {
			if candidate == alias {
				found = true
			}
		}
	}
	if !found {
		return "", ErrFleetTargetChanged
	}
	snapshot, err := s.client.Hosts.Inspect(alias)
	if err != nil {
		return "", ErrFleetTargetChanged
	}
	digest, err := fleetSnapshotDigest(snapshot)
	if err != nil || digest != host.TargetDigest {
		return "", ErrFleetTargetChanged
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Service) FleetTargetCurrent(host FleetHost, alias string) error {
	_, err := s.FleetDispatchIdentity(host, alias)
	return err
}

type fleetSelector struct {
	kind   string
	values []string
	labels map[string]string
}

func parseFleetSelector(selector string) (fleetSelector, error) {
	invalid := errors.New("invalid fleet selector; use all, id=ID[,ID], alias=NAME[,NAME], or label:KEY=VALUE[&KEY=VALUE]")
	if selector == "" || len(selector) > fleetMaxSelectorBytes || strings.TrimSpace(selector) != selector {
		return fleetSelector{}, invalid
	}
	if selector == "all" {
		return fleetSelector{kind: "all"}, nil
	}
	if strings.HasPrefix(selector, "label:") {
		labels := make(map[string]string)
		for _, term := range strings.Split(strings.TrimPrefix(selector, "label:"), "&") {
			k, v, ok := strings.Cut(term, "=")
			if !ok || !validFleetLabel(k, 63) || !validFleetLabel(v, 128) || labels[k] != "" || len(labels) >= FleetMaxLabels {
				return fleetSelector{}, invalid
			}
			labels[k] = v
		}
		return fleetSelector{kind: "label", labels: labels}, nil
	}
	kind, list, ok := strings.Cut(selector, "=")
	if !ok || kind != "id" && kind != "alias" {
		return fleetSelector{}, invalid
	}
	values := strings.Split(list, ",")
	if len(values) > FleetMaxHosts*FleetMaxAliases {
		return fleetSelector{}, invalid
	}
	for _, v := range values {
		if kind == "id" && !validFleetHex(v, 32) || kind == "alias" && !validFleetAlias(v) {
			return fleetSelector{}, invalid
		}
	}
	return fleetSelector{kind: kind, values: values}, nil
}

// ResolveFleetTargets returns a fixed, sorted, HostID-deduplicated value.
// Explicit missing and forbidden selections share one generic error; broad
// selection only sees hosts with Fleet and supported-operation grants.
func (s *Service) ResolveFleetTargets(owner Owner, selector string) ([]FleetHost, error) {
	parsed, err := parseFleetSelector(selector)
	if err != nil {
		return nil, err
	}
	if err := owner.Validate(); err != nil {
		return nil, ErrFleetTargetsNotFound
	}
	r := s.inventory
	r.mu.RLock()
	if r.failed != nil {
		r.mu.RUnlock()
		return nil, ErrFleetTargetsNotFound
	}
	snapshot := cloneFleetInventory(r.snapshot)
	r.mu.RUnlock()
	allowed := make(map[string]FleetHost)
	aliases := make(map[string]string)
	authorized := s.fleetHostAuthorizer(owner)
	for _, h := range snapshot.Records {
		if !authorized(h.HostID) {
			continue
		}
		allowed[h.HostID] = h
		for _, alias := range h.Aliases {
			aliases[alias] = h.HostID
		}
	}
	selected := make(map[string]FleetHost)
	if parsed.kind == "id" || parsed.kind == "alias" {
		for _, value := range parsed.values {
			id := value
			if parsed.kind == "alias" {
				id = aliases[value]
			}
			h, ok := allowed[id]
			if !ok {
				return nil, ErrFleetTargetsNotFound
			}
			selected[id] = h
		}
	} else {
		for id, h := range allowed {
			match := true
			for k, v := range parsed.labels {
				if h.Labels[k] != v {
					match = false
					break
				}
			}
			if match {
				selected[id] = h
			}
		}
	}
	if len(selected) == 0 {
		return nil, ErrFleetTargetsNotFound
	}
	out := make([]FleetHost, 0, len(selected))
	for _, h := range selected {
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].HostID < out[j].HostID })
	return out, nil
}

type fleetInventoryCommitUncertain struct{ cause error }

func (e *fleetInventoryCommitUncertain) Error() string {
	return "fleet inventory commit durability uncertain; administrator recovery required"
}
func (e *fleetInventoryCommitUncertain) Unwrap() error { return e.cause }

func saveFleetInventory(path string, snapshot FleetInventory) error {
	if err := validateFleetInventory(snapshot); err != nil {
		return err
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if len(data) > FleetMaxInventoryBytes {
		return errors.New("fleet inventory size limit exceeded")
	}
	if _, err := ReadPrivateFile(path, FleetMaxInventoryBytes); err != nil && !os.IsNotExist(err) {
		return err
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".rdev-fleet-inventory-*")
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
		return &fleetInventoryCommitUncertain{err}
	}
	err = d.Sync()
	closeErr := d.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return &fleetInventoryCommitUncertain{fmt.Errorf("inventory directory sync: %w", err)}
	}
	return nil
}
