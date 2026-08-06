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
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

// minRedactLen guards against redacting trivially short values. A 3-character
// secret would match constantly and mangle unrelated output; such a value is
// not worth protecting anyway.
const minRedactLen = 6

// Store holds named secrets. Safe for concurrent use.
type Store struct {
	mu     sync.RWMutex
	values map[string]string // name -> plaintext
}

func New() *Store {
	return &Store{values: make(map[string]string)}
}

// Set registers a secret under name.
func (s *Store) Set(name, value string) error {
	if name == "" {
		return errors.New("secret name required")
	}
	if value == "" {
		return errors.New("secret value must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[name] = value
	return nil
}

// SetFromFile registers the trimmed contents of path under name.
//
// Trailing newlines are stripped because credential files usually end with one
// and sending it along breaks HTTP headers in confusing ways.
func (s *Store) SetFromFile(name, path string) error {
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
	return s.Set(name, v)
}

// Get returns a plaintext value for building a remote environment.
func (s *Store) Get(name string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.values[name]
	return v, ok
}

// Names lists registered secret names, sorted.
func (s *Store) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.values))
	for k := range s.values {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Delete removes a secret. Reports whether it existed.
func (s *Store) Delete(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.values[name]
	delete(s.values, name)
	return ok
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
	pairs := make([][2]string, 0, len(s.values))
	for name, val := range s.values {
		if len(val) < minRedactLen {
			continue
		}
		pairs = append(pairs, [2]string{val, "<redacted:" + name + ">"})
	}
	s.mu.RUnlock()

	sort.Slice(pairs, func(i, j int) bool { return len(pairs[i][0]) > len(pairs[j][0]) })
	// Whitespace-tolerant matching only matters for text that contains whitespace,
	// and Redact runs over every byte of every command's output. One check here
	// skips the scan for the common single-line case.
	hasSpace := strings.ContainsAny(text, " \t\n\r\v\f")
	for _, p := range pairs {
		text = strings.ReplaceAll(text, p[0], p[1])
		if hasSpace && len(p[0]) >= wrapTolerantMinLen {
			text = replaceWrapped(text, p[0], p[1])
		}
	}
	return text
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
func (s *Store) ResolveEnv(env map[string]string) (map[string]string, error) {
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
		val, found := s.Get(name)
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
