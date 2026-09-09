package artifact

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A valid SSHSIG must not turn malformed inner metadata into an executable
// release decision. Use real Go executables and the isolated fixture signer.
func TestSignedMetadataRejectsMalformedCompatibilityAndBuild(t *testing.T) {
	dir, policy, manifest, key := signedFixture(t)
	if _, _, err := VerifyBundle(t.Context(), dir, policy, time.Now()); err != nil {
		t.Fatalf("valid baseline: %v", err)
	}
	original, err := ReadFile(dir, "manifest.json", MaxDocumentBytes)
	if err != nil {
		t.Fatal(err)
	}
	manifestJSON, _ := json.Marshal(manifest)
	policyPath := filepath.Join(dir, "runtime-policy.json")
	policyJSON, _ := json.Marshal(policy)
	if err := os.WriteFile(policyPath, policyJSON, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RDEV_RELEASE_POLICY", policyPath)
	agent, err := ReadFile(dir, "rdev-agent-linux-amd64", 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	changeCompatibility := func(name string, value json.RawMessage) func(map[string]json.RawMessage) {
		return func(local map[string]json.RawMessage) {
			var compatibility map[string]json.RawMessage
			if err := json.Unmarshal(local["compatibility"], &compatibility); err != nil {
				t.Fatal(err)
			}
			compatibility[name] = value
			local["compatibility"], _ = json.Marshal(compatibility)
		}
	}
	changeBuild := func(nullBuild bool) func(map[string]json.RawMessage) {
		return func(local map[string]json.RawMessage) {
			var artifacts []map[string]json.RawMessage
			if err := json.Unmarshal(local["artifacts"], &artifacts); err != nil {
				t.Fatal(err)
			}
			if nullBuild {
				artifacts[0]["build"] = json.RawMessage(`null`)
			} else {
				var build map[string]json.RawMessage
				if err := json.Unmarshal(artifacts[0]["build"], &build); err != nil {
					t.Fatal(err)
				}
				build["Deps"] = json.RawMessage(`[null]`)
				artifacts[0]["build"], _ = json.Marshal(build)
			}
			local["artifacts"], _ = json.Marshal(artifacts)
		}
	}
	cases := map[string]func(map[string]json.RawMessage){
		"future compatibility schema": changeCompatibility("schema_version", json.RawMessage(`999`)),
		"unknown compatibility field": changeCompatibility("future_security_field", json.RawMessage(`true`)),
		"null build":                  changeBuild(true),
		"null dependency":             changeBuild(false),
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			var local map[string]json.RawMessage
			if err := json.Unmarshal(original, &local); err != nil {
				t.Fatal(err)
			}
			change(local)
			raw, _ := json.Marshal(local)
			if err := os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			var changed Manifest
			if err := json.Unmarshal(manifestJSON, &changed); err != nil {
				t.Fatal(err)
			}
			changed.Metadata[0].Size = int64(len(raw))
			changed.Metadata[0].SHA256 = Hash(raw)
			signFixture(t, dir, key, changed)
			if _, _, err := VerifyBundle(t.Context(), dir, policy, time.Now()); err == nil {
				t.Fatal("signed malformed metadata accepted by bundle verifier")
			}
			if _, err := AuthorizeAgent(context.Background(), agent, "linux", "amd64", strings.Repeat("c", 64), false); err == nil {
				t.Fatal("signed malformed metadata accepted by runtime admission")
			}
		})
	}
}

func TestTrustDocumentsRequireExplicitSecurityFields(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	policy := Policy{
		SchemaVersion: 1, ValidUntil: now.Add(time.Hour), Channels: []string{"dev"},
		Roots: []Root{{ID: "isolated-test", PublicKey: "ssh-ed25519 test-fixture-key", Channels: []string{"dev"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), TestOnly: true}},
	}
	baseline, _ := json.Marshal(policy)
	var valid Policy
	if err := Decode(baseline, &valid); err != nil {
		t.Fatalf("valid policy shape: %v", err)
	}
	for _, name := range []string{"allow_unsigned_dev", "allow_test_roots", "roots", "root.revoked", "root.test_only", "root.not_before"} {
		t.Run(name, func(t *testing.T) {
			var raw map[string]json.RawMessage
			_ = json.Unmarshal(baseline, &raw)
			if field, rootField := strings.CutPrefix(name, "root."); rootField {
				var roots []map[string]json.RawMessage
				_ = json.Unmarshal(raw["roots"], &roots)
				delete(roots[0], field)
				raw["roots"], _ = json.Marshal(roots)
			} else {
				delete(raw, name)
			}
			b, _ := json.Marshal(raw)
			var parsed Policy
			if err := Decode(b, &parsed); err == nil {
				t.Fatal("missing required security field accepted")
			}
		})
	}
	var aliases map[string]json.RawMessage
	_ = json.Unmarshal(baseline, &aliases)
	aliases["ALLOW_UNSIGNED_DEV"] = json.RawMessage(`true`)
	ambiguous, _ := json.Marshal(aliases)
	var parsed Policy
	if err := Decode(ambiguous, &parsed); err == nil {
		t.Fatal("case alias accepted")
	}
	var source Source
	if err := Decode([]byte(`{"commit":"`+strings.Repeat("a", 40)+`","tree_sha256":"`+strings.Repeat("b", 64)+`"}`), &source); err == nil {
		t.Fatal("omitted source dirty flag accepted")
	}
}
