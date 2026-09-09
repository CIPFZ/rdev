// Package release builds and verifies local, unsigned release evidence.
package release

import (
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"

	artifactcontract "github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/compat"
)

type Source struct {
	Commit     string `json:"commit"`
	TreeSHA256 string `json:"tree_sha256"`
	Dirty      bool   `json:"dirty"`
}

type Artifact struct {
	Name   string           `json:"name"`
	SHA256 string           `json:"sha256"`
	Size   int64            `json:"size"`
	GOOS   string           `json:"goos"`
	GOARCH string           `json:"goarch"`
	Build  *debug.BuildInfo `json:"build"`
}

type Manifest struct {
	SchemaVersion int               `json:"schema_version"`
	Source        Source            `json:"source"`
	Artifacts     []Artifact        `json:"artifacts"`
	Compatibility compat.Contract   `json:"compatibility"`
	Evidence      map[string]string `json:"evidence_sha256"`
}

func git(args ...string) ([]byte, error) { return exec.Command("git", args...).Output() }

// Snapshot includes tracked and non-ignored untracked files, including file
// names and executable modes. Generated build outputs live under ignored bin/.
func Snapshot() (Source, error) {
	commit, err := git("rev-parse", "HEAD")
	if err != nil {
		return Source{}, err
	}
	status, err := git("status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return Source{}, err
	}
	names, err := git("ls-files", "--cached", "--others", "--exclude-standard", "-z")
	if err != nil {
		return Source{}, err
	}
	paths := strings.Split(strings.TrimSuffix(string(names), "\x00"), "\x00")
	sort.Strings(paths)
	h := sha256.New()
	for _, name := range paths {
		info, err := os.Lstat(name)
		if os.IsNotExist(err) {
			fmt.Fprintf(h, "%s\x00deleted\x00", name)
			continue
		}
		if err != nil {
			return Source{}, err
		}
		fmt.Fprintf(h, "%s\x00%d\x00", name, info.Mode())
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(name)
			if err != nil {
				return Source{}, err
			}
			fmt.Fprintf(h, "%s\x00", target)
		} else {
			data, err := os.ReadFile(name)
			if err != nil {
				return Source{}, err
			}
			sum := sha256.Sum256(data)
			h.Write(sum[:])
		}
	}
	return Source{Commit: strings.TrimSpace(string(commit)), Dirty: len(status) > 0, TreeSHA256: hex.EncodeToString(h.Sum(nil))}, nil
}

func Digest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Audit rejects every reported vulnerable module, including imported-but-not-
// called symbols. govulncheck -json itself can exit zero with findings.
func Audit(r io.Reader) error {
	d := json.NewDecoder(r)
	config := false
	count := 0
	for {
		var message map[string]json.RawMessage
		if err := d.Decode(&message); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		count++
		if _, ok := message["config"]; ok {
			config = true
		}
		if f, ok := message["finding"]; ok {
			return fmt.Errorf("vulnerability finding: %s", f)
		}
	}
	if !config || count < 2 {
		return errors.New("incomplete govulncheck report")
	}
	return nil
}

// AuditModules rejects dependency resolutions that cannot be reproduced from
// the public module checksums, and versions retracted by their publisher.
// Available updates alone do not justify a dependency upgrade.
func AuditModules(r io.Reader) error {
	d := json.NewDecoder(r)
	mainSeen := false
	count := 0
	for {
		var m struct {
			Path      string
			Version   string
			Main      bool
			Replace   json.RawMessage
			Retracted []string
			Error     json.RawMessage
		}
		if err := d.Decode(&m); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		count++
		if m.Path == "" {
			return errors.New("module audit contains an empty module")
		}
		if m.Main {
			if mainSeen {
				return errors.New("multiple main modules")
			}
			mainSeen = true
		} else if m.Version == "" {
			return fmt.Errorf("unpinned module: %s", m.Path)
		}
		if len(m.Replace) > 0 || len(m.Retracted) > 0 || len(m.Error) > 0 {
			return fmt.Errorf("replaced, retracted or unresolved dependency: %s", m.Path)
		}
	}
	if !mainSeen || count < 2 {
		return errors.New("incomplete module audit")
	}
	return nil
}

func WriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}

func ReadJSON(path string, v any) error {
	b, err := artifactcontract.ReadFile(filepath.Dir(path), filepath.Base(path), artifactcontract.MaxDocumentBytes)
	if err != nil {
		return err
	}
	return artifactcontract.DecodeEvidence(b, v)
}

func artifact(dir, name, goVersion string) (Artifact, error) {
	data, err := artifactcontract.ReadFile(dir, name, 512<<20)
	if err != nil {
		return Artifact{}, err
	}
	b, err := buildinfo.Read(bytes.NewReader(data))
	if err != nil {
		return Artifact{}, err
	}
	if b.GoVersion != goVersion {
		return Artifact{}, fmt.Errorf("%s toolchain %s, expected %s", name, b.GoVersion, goVersion)
	}
	a := Artifact{Name: name, Size: int64(len(data)), Build: b}
	for _, setting := range b.Settings {
		switch setting.Key {
		case "GOOS":
			a.GOOS = setting.Value
		case "GOARCH":
			a.GOARCH = setting.Value
		}
	}
	if strings.HasPrefix(name, "rdev-agent-") && name != "rdev-agent-"+a.GOOS+"-"+a.GOARCH {
		return Artifact{}, errors.New("artifact name/platform mismatch")
	}
	a.SHA256 = artifactcontract.Hash(data)
	return a, nil
}

var AgentNames = []string{"rdev-agent-linux-amd64", "rdev-agent-linux-arm64", "rdev-agent-darwin-amd64", "rdev-agent-darwin-arm64"}

func Generate(dir string, source Source, goVersion string) error {
	current, err := Snapshot()
	if err != nil {
		return err
	}
	if current != source {
		return errors.New("source changed during release gate; rebuild")
	}
	m := Manifest{SchemaVersion: 1, Source: source, Compatibility: compat.Current(), Evidence: make(map[string]string)}
	for _, name := range append([]string{"rdev", "rdevd"}, AgentNames...) {
		a, err := artifact(dir, name, goVersion)
		if err != nil {
			return err
		}
		m.Artifacts = append(m.Artifacts, a)
	}
	data, err := os.ReadFile(filepath.Join(dir, "rdev"))
	if err != nil {
		return err
	}
	version, err := artifactcontract.BinaryVersion(data)
	if err != nil {
		return err
	}
	m.Compatibility.Release = version

	// go:embed stores these assets uncompressed. Check their full bytes in the
	// produced CLI; a truncated human-readable hash is not sufficient evidence.
	cli, err := os.ReadFile(filepath.Join(dir, "rdev"))
	if err != nil {
		return err
	}
	for _, a := range m.Artifacts[2:] {
		data, err := os.ReadFile(filepath.Join(dir, a.Name))
		if err != nil {
			return err
		}
		if !bytes.Contains(cli, data) {
			return fmt.Errorf("embedded agent mismatch: %s", a.Name)
		}
	}
	for _, name := range auditNames() {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		err = Audit(f)
		f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		m.Evidence[name], err = Digest(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	for _, name := range []string{"modules.json", "module-verify.txt", "tools.txt", "source.json", "THIRD_PARTY_NOTICES.txt"} {
		m.Evidence[name], err = Digest(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	modules, err := os.Open(filepath.Join(dir, "modules.json"))
	if err != nil {
		return err
	}
	err = AuditModules(modules)
	modules.Close()
	if err != nil {
		return err
	}
	if err := WriteJSON(filepath.Join(dir, "manifest.json"), m); err != nil {
		return err
	}
	if err := WriteJSON(filepath.Join(dir, "sbom.cdx.json"), sbom(m)); err != nil {
		return err
	}
	if err := WriteJSON(filepath.Join(dir, "provenance.intoto.json"), provenance(m)); err != nil {
		return err
	}
	return Verify(dir)
}

func auditNames() []string {
	names := []string{"govulncheck-binary-rdev.json", "govulncheck-binary-rdevd.json"}
	for _, agent := range AgentNames {
		names = append(names, "govulncheck-binary-"+agent+".json", "govulncheck-source-"+strings.TrimPrefix(agent, "rdev-agent-")+".json")
	}
	return names
}

type object = map[string]any

func localEvidence(m Manifest) artifactcontract.LocalEvidence {
	b, _ := json.Marshal(m)
	var local artifactcontract.LocalEvidence
	_ = json.Unmarshal(b, &local)
	return local
}
func sbom(m Manifest) object       { return artifactcontract.SBOM(localEvidence(m)) }
func provenance(m Manifest) object { return artifactcontract.Provenance(localEvidence(m)) }

func Verify(dir string) error {
	raw, err := artifactcontract.ReadFile(dir, "manifest.json", artifactcontract.MaxDocumentBytes)
	if err != nil {
		return err
	}
	return verifyManifest(dir, raw)
}

// verifyExpected never selects a second, unsigned manifest for audit semantics.
func verifyExpected(dir string, expected artifactcontract.File) error {
	if expected.Name != "manifest.json" || expected.Size > artifactcontract.MaxDocumentBytes {
		return errors.New("invalid signed audit manifest")
	}
	raw, err := artifactcontract.ReadExpected(dir, expected)
	if err != nil {
		return err
	}
	return verifyManifest(dir, raw)
}
func verifyManifest(dir string, raw []byte) error {
	var m Manifest
	if err := artifactcontract.DecodeEvidence(raw, &m); err != nil {
		return err
	}
	if m.SchemaVersion != 1 || len(m.Artifacts) != 6 {
		return errors.New("invalid manifest")
	}
	contract, err := json.Marshal(m.Compatibility)
	if err != nil {
		return err
	}
	if err = artifactcontract.DecodeCompatibility(contract, m.Compatibility.Release); err != nil {
		return err
	}
	wanted := append([]string{"rdev", "rdevd"}, AgentNames...)
	binaries := make(map[string][]byte)
	for i, a := range m.Artifacts {
		if a.Name != wanted[i] || a.Build == nil {
			return errors.New("missing or unexpected artifact")
		}
		for _, dep := range a.Build.Deps {
			if dep == nil {
				return errors.New("null linked dependency")
			}
		}
		data, err := artifactcontract.ReadExpected(dir, artifactcontract.File{Name: a.Name, SHA256: a.SHA256, Size: a.Size})
		if err != nil {
			return err
		}
		if err = artifactcontract.CheckAgentBuild(data, artifactcontract.LocalArtifact{Build: a.Build}); err != nil {
			return err
		}
		if err = artifactcontract.CheckBinaryVersion(data, m.Compatibility.Release); err != nil {
			return err
		}
		settings := map[string]string{}
		for _, setting := range a.Build.Settings {
			if _, ok := settings[setting.Key]; ok {
				return errors.New("duplicate build setting")
			}
			settings[setting.Key] = setting.Value
		}
		if settings["GOOS"] != a.GOOS || settings["GOARCH"] != a.GOARCH || settings["vcs.revision"] != m.Source.Commit || settings["vcs.modified"] != fmt.Sprint(m.Source.Dirty) {
			return errors.New("build source/platform mismatch")
		}
		if i >= 2 && a.Name != "rdev-agent-"+a.GOOS+"-"+a.GOARCH {
			return errors.New("artifact platform mismatch")
		}
		binaries[a.Name] = data
	}
	for _, name := range AgentNames {
		if !bytes.Contains(binaries["rdev"], binaries[name]) {
			return errors.New("embedded agent bytes mismatch")
		}
	}
	expectedNames := append(auditNames(), "modules.json", "module-verify.txt", "tools.txt", "source.json", "THIRD_PARTY_NOTICES.txt")
	if len(m.Evidence) != len(expectedNames) {
		return errors.New("unexpected evidence set")
	}
	evidence := make(map[string][]byte)
	for _, name := range expectedNames {
		want := m.Evidence[name]
		if len(want) != 64 {
			return fmt.Errorf("missing evidence: %s", name)
		}
		data, err := artifactcontract.ReadFile(dir, name, artifactcontract.MaxDocumentBytes)
		if err != nil {
			return err
		}
		if artifactcontract.Hash(data) != want {
			return fmt.Errorf("evidence changed: %s", name)
		}
		evidence[name] = data
	}
	var source Source
	if err := artifactcontract.DecodeEvidence(evidence["source.json"], &source); err != nil {
		return err
	}
	if source != m.Source {
		return errors.New("source evidence mismatch")
	}
	for _, name := range auditNames() {
		if err := Audit(bytes.NewReader(evidence[name])); err != nil {
			return err
		}
	}
	if err := AuditModules(bytes.NewReader(evidence["modules.json"])); err != nil {
		return err
	}
	for name, expected := range map[string]object{"sbom.cdx.json": sbom(m), "provenance.intoto.json": provenance(m)} {
		var got object
		if err := ReadJSON(filepath.Join(dir, name), &got); err != nil {
			return err
		}
		if !sameJSON(expected, got) {
			return fmt.Errorf("metadata does not match artifacts: %s", name)
		}
	}
	return nil
}

func sameJSON(expected, actual any) bool {
	// Nested typed structs (Source) and decoded objects have different key
	// ordering. Compare canonical JSON objects, not struct field order.
	want, err := json.Marshal(expected)
	if err != nil {
		return false
	}
	var canonical any
	if json.Unmarshal(want, &canonical) != nil {
		return false
	}
	want, err = json.Marshal(canonical)
	if err != nil {
		return false
	}
	got, err := json.Marshal(actual)
	return err == nil && bytes.Equal(want, got)
}
