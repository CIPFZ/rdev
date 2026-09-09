package artifact

import (
	"bytes"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"regexp"
	"runtime/debug"
)

type LocalArtifact struct {
	Name   string           `json:"name"`
	SHA256 string           `json:"sha256"`
	Size   int64            `json:"size"`
	GOOS   string           `json:"goos"`
	GOARCH string           `json:"goarch"`
	Build  *debug.BuildInfo `json:"build"`
}
type LocalEvidence struct {
	SchemaVersion int               `json:"schema_version"`
	Source        Source            `json:"source"`
	Artifacts     []LocalArtifact   `json:"artifacts"`
	Compatibility json.RawMessage   `json:"compatibility"`
	Evidence      map[string]string `json:"evidence_sha256"`
}

// CheckBinding couples the signed identity to the inner audited build metadata.
// The same implementation is used before signing and on runtime admission.
func CheckBinding(dir string, m Manifest) (LocalEvidence, error) {
	var local LocalEvidence
	metadata := make(map[string][]byte)
	for _, f := range m.Metadata {
		if f.Size > MaxDocumentBytes {
			return local, errors.New("oversized release metadata")
		}
		b, err := ReadExpected(dir, f)
		if err != nil {
			return local, err
		}
		metadata[f.Name] = b
	}
	raw := metadata["manifest.json"]
	var err error
	if err = DecodeEvidence(raw, &local); err != nil {
		return local, err
	}
	if local.SchemaVersion != 1 || local.Source != m.Source || len(local.Artifacts) != len(m.Binaries) {
		return local, errors.New("release/source binding mismatch")
	}
	if err = DecodeCompatibility(local.Compatibility, m.Version); err != nil {
		return local, err
	}
	for i, a := range local.Artifacts {
		b := m.Binaries[i]
		if a.Name != b.Name || a.SHA256 != b.SHA256 || a.Size != b.Size || a.GOOS != b.GOOS || a.GOARCH != b.GOARCH || a.Build == nil {
			return local, errors.New("release/artifact binding mismatch")
		}
		for _, dep := range a.Build.Deps {
			if dep == nil {
				return local, errors.New("null linked dependency")
			}
		}
		values := map[string]string{}
		for _, s := range a.Build.Settings {
			if _, exists := values[s.Key]; exists {
				return local, errors.New("duplicate build setting")
			}
			values[s.Key] = s.Value
		}
		if values["GOOS"] != b.GOOS || values["GOARCH"] != b.GOARCH || values["vcs.revision"] != m.Source.Commit || values["vcs.modified"] != boolString(m.Source.Dirty) {
			return local, errors.New("release actual build/source/platform mismatch")
		}
	}
	for name, want := range map[string]map[string]any{"sbom.cdx.json": SBOM(local), "provenance.intoto.json": Provenance(local)} {
		b := metadata[name]
		var e error
		var got map[string]any
		if e = DecodeEvidence(b, &got); e != nil {
			return local, e
		}
		x, _ := json.Marshal(want)
		var norm map[string]any
		_ = json.Unmarshal(x, &norm)
		x, _ = json.Marshal(norm)
		y, _ := json.Marshal(got)
		if !bytes.Equal(x, y) {
			return local, errors.New("SBOM/provenance content binding mismatch")
		}
	}
	return local, nil
}
func boolString(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
func CheckAgentBuild(data []byte, a LocalArtifact) error {
	info, err := buildinfo.Read(bytes.NewReader(data))
	if err != nil {
		return errors.New("candidate is not a Go executable")
	}
	want, err := json.Marshal(a.Build)
	if err != nil {
		return err
	}
	got, err := json.Marshal(info)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, got) {
		return errors.New("candidate Go build information mismatch")
	}
	return nil
}

var versionMarker = regexp.MustCompile(`rdev-release-identity-v1\[([0-9]+\.[0-9]+\.[0-9]+(?:-(?:beta|dev)\.[0-9]+)?)\]`)

func BinaryVersion(data []byte) (string, error) {
	matches := versionMarker.FindAllSubmatch(data, -1)
	if len(matches) == 0 {
		return "", errors.New("binary has no release version stamp")
	}
	version := string(matches[0][1])
	for _, m := range matches {
		if string(m[1]) != version {
			return "", errors.New("binary contains conflicting release stamps")
		}
	}
	return version, nil
}
func CheckBinaryVersion(data []byte, version string) error {
	got, e := BinaryVersion(data)
	if e != nil {
		return e
	}
	if got != version {
		return errors.New("release version differs from actual binary stamp")
	}
	return nil
}
