package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

const SchemaVersion = 1
const SignatureNamespace = "rdev-release"
const ManifestName = "release.json"
const SignatureName = "release.json.sig"

var BinaryNames = []string{"rdev", "rdevd", "rdev-agent-linux-amd64", "rdev-agent-linux-arm64", "rdev-agent-darwin-amd64", "rdev-agent-darwin-arm64", "rdev-agent-windows-amd64"}
var MetadataNames = []string{"manifest.json", "sbom.cdx.json", "provenance.intoto.json", "THIRD_PARTY_NOTICES.txt"}
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var commitPattern = regexp.MustCompile(`^[a-f0-9]{40}$`)
var versionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-(beta|dev)\.(0|[1-9][0-9]*))?$`)
var identityPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._@+-]{0,127}$`)

type Source struct {
	Commit     string `json:"commit"`
	TreeSHA256 string `json:"tree_sha256"`
	Dirty      bool   `json:"dirty"`
}
type File struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}
type Binary struct {
	File
	GOOS   string `json:"goos"`
	GOARCH string `json:"goarch"`
}
type Manifest struct {
	SchemaVersion      int       `json:"schema_version"`
	Version            string    `json:"version"`
	Channel            string    `json:"channel"`
	Source             Source    `json:"source"`
	Signer             string    `json:"signer"`
	SignatureNamespace string    `json:"signature_namespace"`
	IssuedAt           time.Time `json:"issued_at"`
	ExpiresAt          time.Time `json:"expires_at"`
	Binaries           []Binary  `json:"binaries"`
	Metadata           []File    `json:"metadata"`
	// The complete protocol/features/state contract and actual build inputs are
	// in manifest.json, whose exact bytes are signed by the metadata digest.
}

func Hash(data []byte) string { s := sha256.Sum256(data); return hex.EncodeToString(s[:]) }
func validFile(f File) bool {
	return f.Name != "." && filepath.Base(f.Name) == f.Name && digestPattern.MatchString(f.SHA256) && f.Size > 0 && f.Size <= 512<<20
}
func (m Manifest) Validate(now time.Time) error {
	if m.SchemaVersion != SchemaVersion || !versionPattern.MatchString(m.Version) || !commitPattern.MatchString(m.Source.Commit) || !digestPattern.MatchString(m.Source.TreeSHA256) {
		return errors.New("invalid release identity or schema")
	}
	if m.Channel != "stable" && m.Channel != "beta" && m.Channel != "dev" {
		return errors.New("invalid release channel")
	}
	if _, err := CompareVersions(m.Version, m.Version); err != nil {
		return err
	}
	v := versionPattern.FindStringSubmatch(m.Version)
	suffix := v[5]
	if (m.Channel == "stable" && suffix != "") || (m.Channel != "stable" && suffix != m.Channel) {
		return errors.New("version/channel mismatch")
	}
	if m.Source.Dirty && m.Channel != "dev" {
		return errors.New("dirty build is restricted to dev")
	}
	if !identityPattern.MatchString(m.Signer) || m.SignatureNamespace != SignatureNamespace {
		return errors.New("invalid signer or signature purpose")
	}
	if m.IssuedAt.IsZero() || m.IssuedAt.After(now) || !m.ExpiresAt.After(now) || !m.ExpiresAt.After(m.IssuedAt) || m.ExpiresAt.Sub(m.IssuedAt) > 90*24*time.Hour {
		return errors.New("release expired, not yet valid, or validity exceeds 90 days")
	}
	if (len(m.Binaries) != 6 && len(m.Binaries) != len(BinaryNames)) || len(m.Metadata) != len(MetadataNames) {
		return errors.New("incomplete release")
	}
	for i, b := range m.Binaries {
		if b.Name != BinaryNames[i] || !validFile(b.File) {
			return errors.New("invalid or duplicate binary")
		}
		if (b.GOOS != "linux" && b.GOOS != "darwin" && !(i >= 2 && b.GOOS == "windows" && b.GOARCH == "amd64")) || (b.GOARCH != "amd64" && b.GOARCH != "arm64") {
			return errors.New("unsupported artifact platform")
		}
		if i >= 2 && b.Name != "rdev-agent-"+b.GOOS+"-"+b.GOARCH {
			return errors.New("binary/platform mismatch")
		}
	}
	if m.Binaries[0].GOOS != m.Binaries[1].GOOS || m.Binaries[0].GOARCH != m.Binaries[1].GOARCH {
		return errors.New("CLI/broker platform mismatch")
	}
	for i, f := range m.Metadata {
		if f.Name != MetadataNames[i] || !validFile(f) {
			return errors.New("invalid or duplicate metadata")
		}
	}
	return nil
}
func ReadFile(dir, name string, limit int64) ([]byte, error) {
	if filepath.Base(name) != name || name == "." {
		return nil, errors.New("invalid artifact path")
	}
	p := filepath.Join(dir, name)
	st, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Size() > limit {
		return nil, errors.New("artifact must be a bounded regular file")
	}
	f, err := openArtifact(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	actual, err := f.Stat()
	if err != nil || !os.SameFile(st, actual) {
		return nil, errors.New("artifact replaced while opening")
	}
	// Read at most the initial bound even if an untrusted writer grows the file.
	data, err := readBounded(f, limit)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// ReadExpected binds all semantic checks to the exact bytes whose digest was checked.
func ReadExpected(dir string, expected File) ([]byte, error) {
	if !validFile(expected) {
		return nil, errors.New("invalid file binding")
	}
	data, err := ReadFile(dir, expected.Name, expected.Size)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != expected.Size || Hash(data) != expected.SHA256 {
		return nil, fmt.Errorf("artifact digest/size mismatch: %s", expected.Name)
	}
	return data, nil
}
func CheckFile(dir string, expected File) error { _, err := ReadExpected(dir, expected); return err }
