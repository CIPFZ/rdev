package release

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
)

func TestAuditDoesNotTrustJSONExitStatus(t *testing.T) {
	for _, data := range []string{
		"", `{}`, `{"config":{}}`,
		"{\"config\":{}}\n{\"finding\":{\"osv\":\"GO-test\",\"trace\":[{\"module\":\"stdlib\"}]}}",
		"{\"config\":{}}\n{\"progress\":{}}\ntruncated",
	} {
		if err := Audit(strings.NewReader(data)); err == nil {
			t.Fatalf("accepted incomplete or vulnerable audit: %s", data)
		}
	}
	if err := Audit(strings.NewReader("{\"config\":{}}\n{\"progress\":{\"message\":\"done\"}}")); err != nil {
		t.Fatal(err)
	}
}

func TestDependencyAuditRejectsUnsafeResolution(t *testing.T) {
	main := `{"Path":"github.com/CIPFZ/rdev","Main":true}` + "\n"
	for _, dependency := range []string{
		`{"Path":"dependency"}`,
		`{"Path":"dependency","Version":"v1.0.0","Replace":{"Path":"/local"}}`,
		`{"Path":"dependency","Version":"v1.0.0","Retracted":["security defect"]}`,
		`{"Path":"dependency","Version":"v1.0.0","Error":{"Err":"unavailable"}}`,
	} {
		if err := AuditModules(strings.NewReader(main + dependency)); err == nil {
			t.Fatal("accepted unsafe resolution", dependency)
		}
	}
	if err := AuditModules(strings.NewReader(main + `{"Path":"dependency","Version":"v1.0.0","Update":{"Version":"v1.1.0"}}`)); err != nil {
		t.Fatal(err)
	}
}

func TestSBOMIncludesEmbeddedAgentsAndActualPerBinaryDependencies(t *testing.T) {
	m := Manifest{Source: Source{Commit: "revision"}}
	for _, name := range append([]string{"rdev", "rdevd"}, AgentNames...) {
		b := &debug.BuildInfo{GoVersion: "go1.26.8"}
		if name == "rdev" {
			b.Deps = []*debug.Module{{Path: "only-in-cli", Version: "v1.0.0"}}
		}
		m.Artifacts = append(m.Artifacts, Artifact{Name: name, Build: b, SHA256: strings.Repeat("a", 64)})
	}
	b := sbom(m)
	edges := b["dependencies"].([]object)
	cli := edges[0]["dependsOn"].([]string)
	if len(cli) != 2+len(AgentNames) {
		t.Fatalf("missing CLI embedded or library edges: %v", cli)
	}
	for _, edge := range edges[1:] {
		for _, dep := range edge["dependsOn"].([]string) {
			if dep == "only-in-cli@v1.0.0" {
				t.Fatal("attributed unused CLI dependency to another binary")
			}
		}
	}
	before, _ := json.Marshal(b)
	m.Artifacts[2].SHA256 = strings.Repeat("b", 64)
	after, _ := json.Marshal(sbom(m))
	if bytes.Equal(before, after) {
		t.Fatal("SBOM did not bind artifact bytes")
	}
	p := provenance(m)
	if p["subject"].([]object)[2]["digest"].(object)["sha256"] != m.Artifacts[2].SHA256 {
		t.Fatal("provenance subject not bound to artifact")
	}
}

func TestVerifyRejectsMalformedManifestWithoutPanic(t *testing.T) {
	dir := t.TempDir()
	m := Manifest{SchemaVersion: 1, Artifacts: make([]Artifact, 6)}
	if err := WriteJSON(filepath.Join(dir, "manifest.json"), m); err != nil {
		t.Fatal(err)
	}
	if err := Verify(dir); err == nil {
		t.Fatal("accepted missing build metadata")
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(`{"schema_version":1} {}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Verify(dir); err == nil {
		t.Fatal("accepted multiple JSON documents")
	}
}

func TestProvenanceRoundTripAndTamperedSubject(t *testing.T) {
	m := Manifest{Source: Source{Commit: "revision", TreeSHA256: strings.Repeat("a", 64), Dirty: true}, Artifacts: []Artifact{{Name: "rdev", SHA256: strings.Repeat("b", 64)}}}
	expected := provenance(m)
	data, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	var decoded object
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !sameJSON(expected, decoded) {
		t.Fatal("valid provenance rejected after JSON round trip")
	}
	decoded["subject"].([]any)[0].(map[string]any)["digest"].(map[string]any)["sha256"] = strings.Repeat("c", 64)
	if sameJSON(expected, decoded) {
		t.Fatal("changed artifact digest accepted")
	}
}
