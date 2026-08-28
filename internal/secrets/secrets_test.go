package secrets

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testKey(name string) Key { return OutputKey(name) }

var testHost = HostIdentity{Alias: "dev", Fingerprint: "fingerprint", Generation: 1}

func TestRedactReplacesValue(t *testing.T) {
	s := New()
	if err := s.Set(testKey("tok"), "82d9b49359b262b40bdbbfa844891b5e"); err != nil {
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
	s.Set(testKey("k"), "supersecret")
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
	s.Set(testKey("short"), "abc123")
	s.Set(testKey("long"), "abc123456789")

	got := s.Redact("value=abc123456789")
	if got != "value=<redacted:long>" {
		t.Errorf("Redact() = %q, want %q", got, "value=<redacted:long>")
	}
}

// Short values must be rejected at registration so injection can never succeed
// without a matching redaction guarantee.
func TestSetRejectsShortValuesAndPreservesExisting(t *testing.T) {
	s := New()
	key := testKey("tiny")
	if err := s.Set(key, "abcdef"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(key, "ab"); err == nil {
		t.Fatal("short secret was accepted")
	}
	if got, _ := s.Get(key); got != "abcdef" {
		t.Errorf("rejected update changed Store to %q", got)
	}
}

func TestSetRejectsEmpty(t *testing.T) {
	s := New()
	if err := s.Set(testKey(""), "value1"); err == nil {
		t.Error("expected error for empty name")
	}
	if err := s.Set(testKey("n"), ""); err == nil {
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
	if err := s.SetFromFile(testKey("k"), path); err != nil {
		t.Fatal(err)
	}
	got, ok := s.Get(testKey("k"))
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
	if err := s.SetFromFile(testKey("k"), path); err == nil {
		t.Error("expected error for whitespace-only secret file")
	}
}

func TestResolveEnvExpandsReference(t *testing.T) {
	s := New()
	s.Set(HostKey("project", testHost, "gongfeng"), "realtokenvalue")

	out, err := s.ResolveEnv("project", testHost, map[string]string{
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
	_, err := s.ResolveEnv("project", testHost, map[string]string{"V": "secret:nope"})
	if err == nil {
		t.Fatal("expected error for unknown secret")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error should name the missing secret, got %v", err)
	}
}

func TestDeleteAndNames(t *testing.T) {
	s := New()
	s.Set(testKey("b"), "value-b")
	s.Set(testKey("a"), "value-a")

	if got := s.Names(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Names() = %v, want sorted [a b]", got)
	}
	if !s.Delete(testKey("a")) {
		t.Error("Delete() should report true for an existing secret")
	}
	if s.Delete(testKey("a")) {
		t.Error("Delete() should report false for a missing secret")
	}
	if got := s.Names(); len(got) != 1 {
		t.Errorf("Names() = %v, want 1 entry after delete", got)
	}
}

// Documents a known limitation: redaction matches whole values, so a caller who
// deliberately prints a fragment defeats it. This guards against the common
// accident (a credential echoed or dumped verbatim), not against transformation.
func TestRedactDoesNotCatchFragments(t *testing.T) {
	s := New()
	s.Set(testKey("tok"), "82d9b49359b262b40bdbbfa844891b5e")

	got := s.Redact("prefix=82d9")
	if got != "prefix=82d9" {
		t.Errorf("Redact() = %q; fragment matching is not implemented, so this should pass through", got)
	}
}

// A credential wrapped across lines by a config dump, a YAML folder, or `fold` is
// an accident of formatting, not an attempt to hide the value, so it belongs on the
// defended side of the line.
func TestRedactMasksWrappedValue(t *testing.T) {
	const tok = "ghp_A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8"
	s := New()
	s.Set(testKey("tok"), tok)

	cases := map[string]string{
		"wrapped at 20":  tok[:20] + "\n" + tok[20:],
		"wrapped twice":  tok[:10] + "\n" + tok[10:25] + "\n" + tok[25:],
		"space inserted": tok[:10] + " " + tok[10:],
		"tab inserted":   tok[:5] + "\t" + tok[5:],
		"crlf wrap":      tok[:15] + "\r\n" + tok[15:],
		"indented cont":  tok[:12] + "\n    " + tok[12:],
		"verbatim":       "Bearer " + tok,
	}
	for name, out := range cases {
		red := s.Redact(out)
		if strings.Contains(red, tok[:16]) {
			t.Errorf("%s: still leaks: %q", name, red)
		} else {
			t.Logf("%-16s -> %q", name, strings.ReplaceAll(red, "\n", "\\n"))
		}
	}
}

// Whitespace tolerance must not swallow unrelated text or over-match.
// Whitespace tolerance must not swallow unrelated text: scattered characters that
// merely start with the same byte are not a match.
func TestRedactWrapToleranceDoesNotOverMatch(t *testing.T) {
	s := New()
	s.Set(testKey("tok"), "abcdefghijklmnop") // 16 chars, at the tolerance threshold

	cases := []struct{ in, wantContains string }{
		// Unrelated text with the same first byte must survive.
		{"a b c d e f", "a b c d e f"},
		{"alpha beta gamma", "alpha beta gamma"},
		// A genuine wrap is masked but surrounding text is preserved.
		{"key=abcdefgh\nijklmnop; next=1", "key=<redacted:tok>; next=1"},
	}
	for _, c := range cases {
		got := s.Redact(c.in)
		if !strings.Contains(got, c.wantContains) {
			t.Errorf("Redact(%q) = %q, want it to contain %q", c.in, got, c.wantContains)
		}
	}
}

// Short values keep plain matching: the scattered pattern could occur naturally.
// Short values keep plain matching: for a handful of characters, the scattered
// pattern could plausibly occur in unrelated output.
func TestRedactShortValuesAreNotWrapTolerant(t *testing.T) {
	s := New()
	s.Set(testKey("short"), "abcdef") // 6 chars, below the threshold
	got := s.Redact("a b c d e f")
	if !strings.Contains(got, "a b c d e f") {
		t.Errorf("a short value should not match scattered characters: %q", got)
	}
}

func TestHostScopedSameNameNeverCrossResolves(t *testing.T) {
	s := New()
	a := HostIdentity{Alias: "a", Fingerprint: "fa", Generation: 1}
	b := HostIdentity{Alias: "b", Fingerprint: "fb", Generation: 1}
	if err := s.Set(HostKey("project", a, "tok"), "secret-a-value"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(HostKey("project", b, "tok"), "secret-b-value"); err != nil {
		t.Fatal(err)
	}
	gotA, err := s.ResolveEnv("project", a, map[string]string{"T": "secret:tok"})
	if err != nil || gotA["T"] != "secret-a-value" {
		t.Fatalf("host A resolution = %q, %v", gotA["T"], err)
	}
	gotB, err := s.ResolveEnv("project", b, map[string]string{"T": "secret:tok"})
	if err != nil || gotB["T"] != "secret-b-value" {
		t.Fatalf("host B resolution = %q, %v", gotB["T"], err)
	}
}

func TestOutputOnlySecretNeverFallsBackForInjection(t *testing.T) {
	s := New()
	if err := s.Set(OutputKey("tok"), "redaction-only-value"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveEnv("project", testHost, map[string]string{"T": "secret:tok"}); err == nil {
		t.Fatal("output-only value silently fell back during host injection")
	}
}

func TestRedactValueBeforeJSONSerializationHandlesEscapesAndUnicode(t *testing.T) {
	s := New()
	secret := "quo\"te\\line\n雪界"
	if err := s.Set(OutputKey("tok"), secret); err != nil {
		t.Fatal(err)
	}
	in := map[string]any{"nested": []any{map[string]string{"value": "prefix " + secret + " suffix"}}}
	out := s.RedactValue(in)
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "quo") || strings.Contains(string(raw), "雪界") {
		t.Fatalf("special-character secret survived structured redaction: %s", raw)
	}
	if !strings.Contains(string(raw), "redacted:tok") {
		t.Fatalf("placeholder missing: %s", raw)
	}
}

func TestRedactValuePreservesOpaqueStructState(t *testing.T) {
	s := New()
	if err := s.Set(OutputKey("tok"), "secret-value"); err != nil {
		t.Fatal(err)
	}
	wantTime := time.Date(2026, 8, 28, 12, 34, 56, 0, time.UTC)
	type result struct {
		When time.Time `json:"when"`
		Text string    `json:"text"`
	}
	got := s.RedactValue(result{When: wantTime, Text: "secret-value"}).(result)
	if !got.When.Equal(wantTime) {
		t.Fatalf("recursive redaction corrupted opaque struct state: got %s want %s", got.When, wantTime)
	}
	if got.Text != "<redacted:tok>" {
		t.Fatalf("exported string was not redacted: %q", got.Text)
	}
}

func TestRedactMatchesGoAndJSONEscapedErrorForms(t *testing.T) {
	s := New()
	secret := "quo\"te\\line\n雪界\u2028end"
	if err := s.Set(OutputKey("tok"), secret); err != nil {
		t.Fatal(err)
	}
	goQuoted := fmt.Sprintf("transport stderr=%q", secret)
	jsonQuoted, err := json.Marshal(map[string]string{"error": secret})
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{goQuoted, string(jsonQuoted), fmt.Sprintf("outer=%q", goQuoted)} {
		got := s.Redact(text)
		if strings.Contains(got, "quo") || strings.Contains(got, "雪界") || !strings.Contains(got, "<redacted:tok>") {
			t.Fatalf("escaped secret survived redaction: in=%q out=%q", text, got)
		}
	}
}

func TestRedactValuePreservesMapEntriesOnKeyCollision(t *testing.T) {
	s := New()
	if err := s.Set(OutputKey("tok"), "SECRETK"); err != nil {
		t.Fatal(err)
	}
	in := map[string]string{"SECRETK": "first", "<redacted:tok>": "second"}
	out := s.RedactValue(in).(map[string]string)
	if len(out) != 2 {
		t.Fatalf("redacted key collision dropped an entry: %+v", out)
	}
	values := map[string]bool{}
	for key, value := range out {
		if strings.Contains(key, "SECRETK") {
			t.Fatalf("secret-bearing map key survived: %q", key)
		}
		values[value] = true
	}
	if !values["first"] || !values["second"] {
		t.Fatalf("map values changed across collision: %+v", out)
	}
}
