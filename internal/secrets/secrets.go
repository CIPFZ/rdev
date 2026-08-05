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
	for _, p := range pairs {
		text = strings.ReplaceAll(text, p[0], p[1])
	}
	return text
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
