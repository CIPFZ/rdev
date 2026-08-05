package secrets

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedactReplacesValue(t *testing.T) {
	s := New()
	if err := s.Set("tok", "82d9b49359b262b40bdbbfa844891b5e"); err != nil {
		t.Fatal(err)
	}

	got := s.Redact("Authorization: token 82d9b49359b262b40bdbbfa844891b5e done")
	want := "Authorization: token <redacted:tok> done"
	if got != want {
		t.Errorf("Redact() = %q, want %q", got, want)
	}
}

func TestRedactAllOccurrences(t *testing.T) {
	s := New()
	s.Set("k", "supersecret")
	got := s.Redact("supersecret and supersecret again")
	if strings.Contains(got, "supersecret") {
		t.Errorf("plaintext survived redaction: %q", got)
	}
	if n := strings.Count(got, "<redacted:k>"); n != 2 {
		t.Errorf("got %d placeholders, want 2: %q", n, got)
	}
}

// A secret that contains another must be redacted first, or the outer value
// leaks a fragment. Longest-first ordering is what prevents that.
func TestRedactLongestFirst(t *testing.T) {
	s := New()
	s.Set("short", "abc123")
	s.Set("long", "abc123456789")

	got := s.Redact("value=abc123456789")
	if got != "value=<redacted:long>" {
		t.Errorf("Redact() = %q, want %q", got, "value=<redacted:long>")
	}
}

// Very short values would match constantly and mangle unrelated output, so they
// are deliberately not redacted.
func TestRedactSkipsShortValues(t *testing.T) {
	s := New()
	s.Set("tiny", "ab")
	got := s.Redact("ab cd ab")
	if got != "ab cd ab" {
		t.Errorf("short secret should not be redacted, got %q", got)
	}
}

func TestSetRejectsEmpty(t *testing.T) {
	s := New()
	if err := s.Set("", "v"); err == nil {
		t.Error("expected error for empty name")
	}
	if err := s.Set("n", ""); err == nil {
		t.Error("expected error for empty value")
	}
}

func TestSetFromFileTrimsNewline(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key")
	// Credential files usually end with a newline; sending it along breaks
	// HTTP headers in ways that are hard to debug.
	if err := os.WriteFile(path, []byte("tokenvalue123\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := New()
	if err := s.SetFromFile("k", path); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get("k")
	if !ok {
		t.Fatal("secret not registered")
	}
	if got != "tokenvalue123" {
		t.Errorf("Get() = %q, want %q (newline should be trimmed)", got, "tokenvalue123")
	}
}

func TestSetFromFileRejectsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty")
	os.WriteFile(path, []byte("   \n"), 0o600)

	s := New()
	if err := s.SetFromFile("k", path); err == nil {
		t.Error("expected error for whitespace-only secret file")
	}
}

func TestResolveEnvExpandsReference(t *testing.T) {
	s := New()
	s.Set("gongfeng", "realtokenvalue")

	out, err := s.ResolveEnv(map[string]string{
		"GONGFENG_TOKEN": "secret:gongfeng",
		"PLAIN":          "literal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if out["GONGFENG_TOKEN"] != "realtokenvalue" {
		t.Errorf("secret reference not expanded: %q", out["GONGFENG_TOKEN"])
	}
	if out["PLAIN"] != "literal" {
		t.Errorf("plain value altered: %q", out["PLAIN"])
	}
}

func TestResolveEnvUnknownSecret(t *testing.T) {
	s := New()
	_, err := s.ResolveEnv(map[string]string{"V": "secret:nope"})
	if err == nil {
		t.Fatal("expected error for unknown secret")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error should name the missing secret, got %v", err)
	}
}

func TestDeleteAndNames(t *testing.T) {
	s := New()
	s.Set("b", "value-b")
	s.Set("a", "value-a")

	if got := s.Names(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Names() = %v, want sorted [a b]", got)
	}
	if !s.Delete("a") {
		t.Error("Delete() should report true for an existing secret")
	}
	if s.Delete("a") {
		t.Error("Delete() should report false for a missing secret")
	}
	if got := s.Names(); len(got) != 1 {
		t.Errorf("Names() = %v, want 1 entry after delete", got)
	}
}
