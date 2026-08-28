// Package secrets keeps credential values out of tool results.
//
// Values registered here are replaced with <redacted:name> in every string that
// leaves the host, and can be referenced by name when building a remote
// environment so the caller never has to see the plaintext.
//
// This exists because in practice a token reaches a transcript by accident: a
// `cat` of a key file, an error message echoing a request, a config dump. Doing
// the substitution at the boundary means no individual tool has to remember.
package secrets

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// MinValueBytes is the minimum accepted secret size. Values shorter than this
// used to be injectable while Redact deliberately ignored them, which made a
// successful registration look protected when it was not.
const MinValueBytes = 6

// Scope is the authority boundary a secret belongs to. Host secrets use the
// registry scope (currently "global" or "project"). Output-only registrations
// are retained for compatibility with callers that do not select a host, but
// can never be resolved into a remote environment.
type Scope string

const ScopeOutput Scope = "output"

// HostIdentity binds a secret to one immutable alias generation. Fingerprint
// prevents accidental reuse across distinct destinations; Generation prevents
// reuse if an alias changes away and later returns to the same destination.
type HostIdentity struct {
	Alias       string `json:"alias"`
	Fingerprint string `json:"fingerprint"`
	Generation  uint64 `json:"generation"`
}

// Key is the complete lookup key. Injection always requires an exact
// scope+host-identity+name match; there is no fallback to an output-only or
// differently scoped value with the same name.
type Key struct {
	Scope Scope        `json:"scope"`
	Host  HostIdentity `json:"host"`
	Name  string       `json:"name"`
}

// Descriptor is the safe, value-free form returned by list operations.
type Descriptor = Key

// Source records how a value entered the Store. Declarative values refresh on
// secure reconnect; manual values retain explicit precedence.
type Source string

const (
	SourceManual      Source = "manual"
	SourceDeclarative Source = "declarative"
)

type entry struct {
	value  string
	source Source
}

func OutputKey(name string) Key { return Key{Scope: ScopeOutput, Name: name} }

func HostKey(scope Scope, host HostIdentity, name string) Key {
	return Key{Scope: scope, Host: host, Name: name}
}

// ValidateDeclarations checks the persisted/runtime {name:path} form before it
// can be published or saved into a host registry.
func ValidateDeclarations(declarations map[string]string) error {
	for name, path := range declarations {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(path) == "" {
			return errors.New("secret declaration requires a nonempty name and path")
		}
	}
	return nil
}

func (k Key) validate() error {
	if k.Name == "" {
		return errors.New("secret name required")
	}
	if k.Scope == "" {
		return errors.New("secret scope required")
	}
	if k.Scope == ScopeOutput {
		if k.Host != (HostIdentity{}) {
			return errors.New("output-only secret must not carry a host identity")
		}
		return nil
	}
	if k.Host.Alias == "" || k.Host.Fingerprint == "" || k.Host.Generation == 0 {
		return errors.New("host-scoped secret requires an immutable host identity")
	}
	return nil
}

// Store holds named secrets. Safe for concurrent use.
type Store struct {
	mu          sync.RWMutex
	values      map[Key]entry
	onRedaction func(uint64)
}

func New() *Store {
	return &Store{values: make(map[Key]entry)}
}

// Snapshot returns an immutable copy used to redact an in-flight operation after
// rotation, deletion, or host redefinition removes its old values. Callers also
// apply the live Store at the end so values registered during the operation are
// covered without globally serializing unrelated hosts.
func (s *Store) Snapshot() *Store {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := New()
	for key, value := range s.values {
		out.values[key] = value
	}
	out.onRedaction = s.onRedaction
	return out
}

// Set registers a secret under its complete security key.
func (s *Store) Set(key Key, value string) error {
	if err := key.validate(); err != nil {
		return err
	}
	if len(value) < MinValueBytes {
		return fmt.Errorf("secret value must be at least %d bytes", MinValueBytes)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = entry{value: value, source: SourceManual}
	return nil
}

// SetBatch validates every value before atomically publishing any of them.
// Connection initialization uses this so one failed declaration cannot leave a
// partially protected host in the store.
func (s *Store) SetBatch(values map[Key]string) error {
	return s.setBatch(values, SourceManual)
}

func (s *Store) SetDeclarativeBatch(values map[Key]string) error {
	return s.setBatch(values, SourceDeclarative)
}

func (s *Store) setBatch(values map[Key]string, source Source) error {
	for key, value := range values {
		if err := key.validate(); err != nil {
			return err
		}
		if len(value) < MinValueBytes {
			return fmt.Errorf("secret value must be at least %d bytes", MinValueBytes)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, value := range values {
		s.values[key] = entry{value: value, source: source}
	}
	return nil
}

// SetFromFile registers the trimmed contents of path under name.
//
// Trailing newlines are stripped because credential files usually end with one
// and sending it along breaks HTTP headers in confusing ways.
func (s *Store) SetFromFile(key Key, path string) error {
	expanded, err := expandHome(path)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(expanded)
	if err != nil {
		return fmt.Errorf("read secret file: %w", err)
	}
	v := strings.TrimSpace(string(b))
	if v == "" {
		return fmt.Errorf("secret file %s is empty", path)
	}
	return s.Set(key, v)
}

// Get returns a plaintext value for building a remote environment.
func (s *Store) Get(key Key) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.values[key]
	return e.value, ok
}

func (s *Store) SourceOf(key Key) (Source, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.values[key]
	return e.source, ok
}

// Names lists unique registered names for compatibility with older list
// consumers. Descriptors is the unambiguous API and should be preferred.
func (s *Store) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set := make(map[string]struct{}, len(s.values))
	for k := range s.values {
		set[k.Name] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func (s *Store) Descriptors() []Descriptor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Descriptor, 0, len(s.values))
	for key := range s.values {
		out = append(out, key)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scope != out[j].Scope {
			return out[i].Scope < out[j].Scope
		}
		if out[i].Host.Alias != out[j].Host.Alias {
			return out[i].Host.Alias < out[j].Host.Alias
		}
		if out[i].Host.Generation != out[j].Host.Generation {
			return out[i].Host.Generation < out[j].Host.Generation
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Delete removes a secret. Reports whether it existed.
func (s *Store) Delete(key Key) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.values[key]
	delete(s.values, key)
	return ok
}

// DeleteHost removes every generation of an alias. Host redefinition calls it
// after publication so old credentials cannot survive under a stale identity.
func (s *Store) DeleteHost(alias string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key := range s.values {
		if key.Scope != ScopeOutput && key.Host.Alias == alias {
			delete(s.values, key)
			removed++
		}
	}
	return removed
}

// DeleteStaleHost removes alias generations other than keep. A connection-only
// policy change (currently ForceAgentUpload) can invalidate transport state while
// preserving the exact credential identity and its scoped values.
func (s *Store) DeleteStaleHost(scope Scope, keep HostIdentity) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key := range s.values {
		if key.Scope == ScopeOutput || key.Host.Alias != keep.Alias {
			continue
		}
		if key.Scope != scope || key.Host != keep {
			delete(s.values, key)
			removed++
		}
	}
	return removed
}

// SetRedactionHook installs a low-cardinality observation callback. It receives
// only a hit count, never secret material or caller-controlled labels.
func (s *Store) SetRedactionHook(hook func(uint64)) {
	s.mu.Lock()
	s.onRedaction = hook
	s.mu.Unlock()
}

// Redact replaces every registered value in text with its placeholder.
//
// Longest values are substituted first: when one secret is a substring of
// another, replacing the shorter one first would leave a fragment of the longer
// one exposed.
//
// Matching tolerates whitespace inserted inside a value, so a credential that got
// line-wrapped by a config dump, a YAML folder, or `fold` is still caught. That is
// an accident of formatting rather than an attempt to hide the value, so it belongs
// on the defended side of the line. Only values of at least wrapTolerantMinLen are
// treated this way, since for a short value the scattered-character pattern could
// plausibly occur in unrelated output.
//
// Limitation worth knowing: matching is still whole-value. A command that emits
// only part of a secret, or transforms it first (`cut -c1-4`, a base64
// re-encoding, a hash, a case change), is not caught. This defends against the
// common accident -- a credential echoed, dumped, wrapped, or quoted back -- not
// against a caller deliberately reshaping the value before printing it.
func (s *Store) Redact(text string) string {
	if text == "" {
		return text
	}
	s.mu.RLock()
	pairs := make([][2]string, 0, len(s.values)*3)
	for key, entry := range s.values {
		val := entry.value
		placeholder := "<redacted:" + key.Name + ">"
		// Errors commonly quote remote stderr or malformed frames with %q.
		// Redaction happens later in client/MCP/CLI code, after that quoting has
		// converted quotes, slashes, and newlines. Match both the original and
		// escaped spellings so quoting cannot become a disclosure bypass.
		for _, form := range escapedForms(val) {
			pairs = append(pairs, [2]string{form, placeholder})
		}
	}
	hook := s.onRedaction
	s.mu.RUnlock()

	sort.Slice(pairs, func(i, j int) bool {
		if len(pairs[i][0]) != len(pairs[j][0]) {
			return len(pairs[i][0]) > len(pairs[j][0])
		}
		if pairs[i][0] != pairs[j][0] {
			return pairs[i][0] < pairs[j][0]
		}
		return pairs[i][1] < pairs[j][1]
	})
	// Whitespace-tolerant matching only matters for text that contains whitespace,
	// and Redact runs over every byte of every command's output. One check here
	// skips the scan for the common single-line case.
	hasSpace := strings.ContainsAny(text, " \t\n\r\v\f")
	original := text
	for _, p := range pairs {
		text = strings.ReplaceAll(text, p[0], p[1])
		if hasSpace && len(p[0]) >= wrapTolerantMinLen {
			text = replaceWrapped(text, p[0], p[1])
		}
	}
	if text != original && hook != nil {
		hook(1)
	}
	return text
}

func escapedForms(value string) []string {
	forms := map[string]struct{}{value: {}}
	addQuotedInterior := func(quoted string) string {
		if len(quoted) >= 2 {
			interior := quoted[1 : len(quoted)-1]
			forms[interior] = struct{}{}
			return interior
		}
		return quoted
	}
	goEscaped := addQuotedInterior(strconv.Quote(value))
	// A diagnostic can itself be quoted by an outer error. Two levels cover
	// that without attempting unbounded transformations.
	addQuotedInterior(strconv.Quote(goEscaped))
	if raw, err := json.Marshal(value); err == nil {
		addQuotedInterior(string(raw))
	}
	out := make([]string, 0, len(forms))
	for form := range forms {
		if form != "" {
			out = append(out, form)
		}
	}
	return out
}

// RedactValue recursively clones a structured value and redacts every string
// before serialization. Operating on raw strings is essential: JSON escaping
// changes quotes, backslashes, newlines and some Unicode code points, so scanning
// serialized bytes cannot reliably find the original secret.
func (s *Store) RedactValue(value any) any {
	if value == nil {
		return nil
	}
	return s.redactReflect(reflect.ValueOf(value)).Interface()
}

func (s *Store) redactReflect(v reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}
	switch v.Kind() {
	case reflect.String:
		out := reflect.New(v.Type()).Elem()
		out.SetString(s.Redact(v.String()))
		return out
	case reflect.Interface:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		inner := s.redactReflect(v.Elem())
		out := reflect.New(v.Type()).Elem()
		out.Set(inner)
		return out
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(s.redactReflect(v.Elem()))
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		// Preserve fields we cannot rewrite (for example time.Time's private
		// representation) and redact only settable fields. Starting from zero
		// would silently corrupt structured output even though no secret was
		// present in those private implementation details.
		out.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath == "" && out.Field(i).CanSet() {
				out.Field(i).Set(s.redactReflect(v.Field(i)))
			}
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(s.redactReflect(v.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(s.redactReflect(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			key := uniqueRedactedMapKey(out, s.redactReflect(iter.Key()))
			out.SetMapIndex(key, s.redactReflect(iter.Value()))
		}
		return out
	default:
		return v
	}
}

// uniqueRedactedMapKey preserves every entry when a secret-bearing key and an
// existing placeholder collapse to the same redacted spelling. String keys are
// the externally serialized case (session env and JSON objects); suffixes carry
// no caller data and therefore cannot disclose the original key.
func uniqueRedactedMapKey(out, key reflect.Value) reflect.Value {
	if !out.MapIndex(key).IsValid() {
		return key
	}
	makeKey := func(text string) (reflect.Value, bool) {
		switch key.Kind() {
		case reflect.String:
			candidate := reflect.New(key.Type()).Elem()
			candidate.SetString(text)
			return candidate, true
		case reflect.Interface:
			if !key.IsNil() && key.Elem().Kind() == reflect.String {
				candidate := reflect.New(key.Type()).Elem()
				str := reflect.New(key.Elem().Type()).Elem()
				str.SetString(text)
				candidate.Set(str)
				return candidate, true
			}
		}
		return reflect.Value{}, false
	}
	var base string
	if key.Kind() == reflect.String {
		base = key.String()
	} else if key.Kind() == reflect.Interface && !key.IsNil() && key.Elem().Kind() == reflect.String {
		base = key.Elem().String()
	} else {
		// Non-string comparable keys cannot normally change through redaction.
		// If a custom comparable composite does collide, confidentiality still
		// wins over retaining a secret-bearing key.
		return key
	}
	for suffix := 2; ; suffix++ {
		candidate, _ := makeKey(fmt.Sprintf("%s#%d", base, suffix))
		if !out.MapIndex(candidate).IsValid() {
			return candidate
		}
	}
}

// wrapTolerantMinLen is the length above which whitespace-tolerant matching is
// used. Long enough that a value's characters appearing in order, separated only
// by whitespace, is not a coincidence.
const wrapTolerantMinLen = 16

// replaceWrapped substitutes occurrences of value whose characters are separated
// by whitespace, e.g. a token split across two lines.
//
// The plain substitution has already run, so this only sees genuinely broken-up
// occurrences. It scans rather than using a regexp because Redact runs over every
// byte of command output, and building a pattern with one optional-whitespace group
// per character would cost far more than the first-byte check below.
func replaceWrapped(text, value, placeholder string) string {
	if len(value) == 0 {
		return text
	}
	var b strings.Builder
	i := 0
	for i < len(text) {
		// Cheap gate: nearly every position fails here without further work.
		if text[i] != value[0] {
			b.WriteByte(text[i])
			i++
			continue
		}
		end, ok := matchSkippingSpace(text, i, value)
		if !ok {
			b.WriteByte(text[i])
			i++
			continue
		}
		b.WriteString(placeholder)
		i = end
	}
	return b.String()
}

// matchSkippingSpace reports whether value occurs at text[start:], allowing
// whitespace between its characters, and returns the index just past the match.
//
// Whitespace is not allowed before the first or after the last character: that
// would let the match swallow surrounding formatting.
func matchSkippingSpace(text string, start int, value string) (int, bool) {
	i, v := start, 0
	for v < len(value) {
		if i >= len(text) {
			return 0, false
		}
		if text[i] == value[v] {
			i++
			v++
			continue
		}
		// A gap is only allowed between characters, never before the first.
		if v > 0 && isSpaceByte(text[i]) {
			i++
			continue
		}
		return 0, false
	}
	return i, true
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}

// ResolveEnv expands "secret:NAME" references into plaintext values.
//
// Callers pass {"GONGFENG_TOKEN": "secret:gongfeng"} and the real value is
// injected into the remote environment without ever appearing in a tool call or
// its result.
func (s *Store) ResolveEnv(scope Scope, host HostIdentity, env map[string]string) (map[string]string, error) {
	if len(env) == 0 {
		return env, nil
	}
	out := make(map[string]string, len(env))
	for k, v := range env {
		name, ok := strings.CutPrefix(v, "secret:")
		if !ok {
			out[k] = v
			continue
		}
		val, found := s.Get(HostKey(scope, host, name))
		if !found {
			return nil, fmt.Errorf("unknown secret %q referenced by env %s", name, k)
		}
		out[k] = val
	}
	return out, nil
}

func expandHome(p string) (string, error) {
	if p != "~" && !strings.HasPrefix(p, "~/") {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	if p == "~" {
		return home, nil
	}
	return home + p[1:], nil
}
