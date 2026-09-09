package artifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/CIPFZ/rdev/internal/proto"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Root struct {
	ID        string    `json:"id"`
	PublicKey string    `json:"public_key"`
	Channels  []string  `json:"channels"`
	NotBefore time.Time `json:"not_before"`
	NotAfter  time.Time `json:"not_after"`
	Revoked   bool      `json:"revoked"`
	TestOnly  bool      `json:"test_only"`
}
type Restriction struct {
	Channels     []string `json:"channels"`
	PinVersion   string   `json:"pin_version,omitempty"`
	DenyUnsigned bool     `json:"deny_unsigned,omitempty"`
}
type Policy struct {
	SchemaVersion    int       `json:"schema_version"`
	ValidUntil       time.Time `json:"valid_until"`
	BundleDir        string    `json:"bundle_dir,omitempty"`
	Channels         []string  `json:"channels"`
	PinVersion       string    `json:"pin_version,omitempty"`
	AllowUnsignedDev bool      `json:"allow_unsigned_dev"`
	AllowTestRoots   bool      `json:"allow_test_roots"`
	Roots            []Root    `json:"roots"`
	// Keys are digests of normalized destination, port and namespace, never alias.
	Hosts           map[string]Restriction `json:"hosts,omitempty"`
	RollbackDigests map[string]string      `json:"rollback_digests,omitempty"`
}
type Decision struct {
	Version  string `json:"version"`
	Channel  string `json:"channel"`
	Digest   string `json:"digest"`
	Signer   string `json:"signer,omitempty"`
	TestRoot bool   `json:"test_root"`
	Unsigned bool   `json:"unsigned"`
	Rollback bool   `json:"rollback"`
}

func contains(items []string, s string) bool {
	for _, x := range items {
		if x == s {
			return true
		}
	}
	return false
}
func validChannels(ch []string) bool {
	if len(ch) == 0 || len(ch) > 3 {
		return false
	}
	seen := map[string]bool{}
	for _, c := range ch {
		if seen[c] || (c != "stable" && c != "beta" && c != "dev") {
			return false
		}
		seen[c] = true
	}
	return true
}
func (p Policy) Validate(now time.Time) error {
	if p.SchemaVersion != SchemaVersion || !p.ValidUntil.After(now) || !validChannels(p.Channels) || len(p.Roots) > 32 || len(p.Hosts) > 1024 || len(p.RollbackDigests) > 1024 {
		return errors.New("release policy is invalid or expired")
	}
	if p.PinVersion != "" && !versionPattern.MatchString(p.PinVersion) {
		return errors.New("invalid release pin")
	}
	if p.BundleDir != "" && !filepath.IsAbs(p.BundleDir) {
		return errors.New("release bundle path must be absolute")
	}
	if p.AllowUnsignedDev && !contains(p.Channels, "dev") {
		return errors.New("unsigned requires explicit dev channel")
	}
	seen := map[string]bool{}
	for _, r := range p.Roots {
		if !identityPattern.MatchString(r.ID) || seen[r.ID] || !validChannels(r.Channels) || r.NotBefore.IsZero() || !r.NotAfter.After(r.NotBefore) {
			return errors.New("invalid signing root")
		}
		seen[r.ID] = true
		fields := strings.Fields(r.PublicKey)
		if len(fields) != 2 || fields[0] != "ssh-ed25519" || strings.ContainsAny(r.PublicKey, "\r\n") {
			return errors.New("root must be one Ed25519 OpenSSH public key without options")
		}
	}
	for key, h := range p.Hosts {
		if !digestPattern.MatchString(key) || !validChannels(h.Channels) || (h.PinVersion != "" && !versionPattern.MatchString(h.PinVersion)) {
			return errors.New("invalid host release restriction")
		}
		for _, c := range h.Channels {
			if !contains(p.Channels, c) {
				return errors.New("host cannot expand release channels")
			}
		}
		if p.PinVersion != "" && h.PinVersion != "" && h.PinVersion != p.PinVersion {
			return errors.New("host cannot change global pin")
		}
	}
	for key, digest := range p.RollbackDigests {
		if !digestPattern.MatchString(key) || !digestPattern.MatchString(digest) {
			return errors.New("invalid exact rollback authorization")
		}
	}
	return nil
}
func readBounded(r io.Reader, n int64) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r, n+1))
	if e == nil && int64(len(b)) > n {
		e = errors.New("file exceeds bound")
	}
	return b, e
}
func LoadPolicy(path string, now time.Time) (Policy, error) {
	var p Policy
	if !filepath.IsAbs(path) {
		return p, errors.New("release policy path must be absolute")
	}
	if err := checkPrivatePath(path); err != nil {
		return p, err
	}
	data, err := readPolicyFile(path)
	if err != nil {
		return p, err
	}
	if err = Decode(data, &p); err != nil {
		return p, err
	}
	return p, p.Validate(now)
}
func DefaultPolicyPath() (string, error) {
	if p := os.Getenv("RDEV_RELEASE_POLICY"); p != "" {
		return p, nil
	}
	h, e := os.UserHomeDir()
	return filepath.Join(h, ".config", "rdev", "release-policy.json"), e
}

// VerifySignature uses OpenSSH's SSHSIG implementation. The signer key comes
// exclusively from the administrator policy, never from a bundle-supplied key.
func VerifySignature(ctx context.Context, dir string, p Policy, now time.Time) (Manifest, Decision, error) {
	var m Manifest
	var decision Decision
	if err := p.Validate(now); err != nil {
		return m, decision, err
	}
	raw, err := ReadFile(dir, ManifestName, MaxDocumentBytes)
	if err != nil {
		return m, decision, err
	}
	if err = Decode(raw, &m); err != nil {
		return m, decision, err
	}
	if err = m.Validate(now); err != nil {
		return m, decision, err
	}
	if !contains(p.Channels, m.Channel) || (p.PinVersion != "" && p.PinVersion != m.Version) {
		return m, decision, proto.NewError(proto.CodeReleaseChannel, "", proto.StateNotSent)
	}
	var root *Root
	for i := range p.Roots {
		if p.Roots[i].ID == m.Signer {
			root = &p.Roots[i]
			break
		}
	}
	if root == nil || root.Revoked || now.Before(root.NotBefore) || !now.Before(root.NotAfter) || !contains(root.Channels, m.Channel) || (root.TestOnly && !p.AllowTestRoots) {
		return m, decision, errors.New("release signer untrusted, revoked, expired or outside allowed purpose")
	}
	sig, err := ReadFile(dir, SignatureName, 64<<10)
	if err != nil {
		return m, decision, err
	}
	tmpRoot, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		return m, decision, err
	}
	tmp, err := os.MkdirTemp(tmpRoot, "rdev-signature-*")
	if err != nil {
		return m, decision, err
	}
	defer os.RemoveAll(tmp)
	allowed := m.Signer + " namespaces=\"" + SignatureNamespace + "\" " + root.PublicKey + "\n"
	if err = os.WriteFile(filepath.Join(tmp, "allowed_signers"), []byte(allowed), 0600); err != nil {
		return m, decision, err
	}
	if err = os.WriteFile(filepath.Join(tmp, "signature"), sig, 0600); err != nil {
		return m, decision, err
	}
	if err = checkPrivatePath(filepath.Join(tmp, "allowed_signers")); err != nil {
		return m, decision, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh-keygen", "-Y", "verify", "-f", filepath.Join(tmp, "allowed_signers"), "-I", m.Signer, "-n", SignatureNamespace, "-s", filepath.Join(tmp, "signature"))
	cmd.Stdin = bytes.NewReader(raw)
	if err = cmd.Run(); err != nil {
		return m, decision, errors.New("release SSHSIG verification failed")
	}
	return m, Decision{Version: m.Version, Channel: m.Channel, Signer: m.Signer, TestRoot: root.TestOnly}, nil
}
func VerifyBundle(ctx context.Context, dir string, p Policy, now time.Time) (Manifest, Decision, error) {
	m, d, e := VerifySignature(ctx, dir, p, now)
	if e != nil {
		return m, d, e
	}
	local, e := CheckBinding(dir, m)
	if e != nil {
		return m, d, e
	}
	for i, b := range m.Binaries {
		data, err := ReadExpected(dir, b.File)
		if err != nil {
			return m, d, err
		}
		if e = CheckBinaryVersion(data, m.Version); e != nil {
			return m, d, e
		}
		if e = CheckAgentBuild(data, local.Artifacts[i]); e != nil {
			return m, d, e
		}
	}
	return m, d, nil
}

// AuthorizeAgent is shared by the standalone CLI/MCP and rdevd. Frontend
// requests cannot supply this policy path or roots to the daemon.
func AuthorizeAgent(ctx context.Context, data []byte, goos, goarch, target string, force bool) (Decision, error) {
	var d Decision
	path, err := DefaultPolicyPath()
	if err != nil {
		return d, err
	}
	p, err := LoadPolicy(path, time.Now())
	if err != nil {
		return d, fmt.Errorf("%w: unsigned dev requires explicit trusted opt-in", proto.NewError(proto.CodeReleasePolicy, "", proto.StateNotSent))
	}
	h, scoped := p.Hosts[target]
	if len(p.Hosts) > 0 && !scoped {
		return d, proto.NewError(proto.CodeReleasePolicy, "", proto.StateNotSent)
	}
	if p.BundleDir == "" {
		if !p.AllowUnsignedDev || !contains(p.Channels, "dev") || p.PinVersion != "" || (scoped && (h.DenyUnsigned || !contains(h.Channels, "dev") || h.PinVersion != "")) {
			return d, proto.NewError(proto.CodeReleasePolicy, "", proto.StateNotSent)
		}
		version, err := BinaryVersion(data)
		if err != nil {
			return d, proto.NewError(proto.CodeUnsupportedFeature, "", proto.StateNotSent)
		}
		return Decision{Version: version, Channel: "dev", Digest: Hash(data), Unsigned: true}, nil
	}
	m, d, err := VerifySignature(ctx, p.BundleDir, p, time.Now())
	if err != nil {
		var envelope *proto.ErrorEnvelope
		if errors.As(err, &envelope) {
			return d, err
		}
		return d, proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent)
	}
	if scoped && (!contains(h.Channels, m.Channel) || (h.PinVersion != "" && h.PinVersion != m.Version)) {
		return d, proto.NewError(proto.CodeReleaseChannel, "", proto.StateNotSent)
	}

	local, err := CheckBinding(p.BundleDir, m)
	if err != nil {
		return d, proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent)
	}
	name := "rdev-agent-" + goos + "-" + goarch
	for i, b := range m.Binaries {
		if b.Name == name {
			if b.GOOS != goos || b.GOARCH != goarch || b.SHA256 != Hash(data) || b.Size != int64(len(data)) {
				return d, proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent)
			}
			if err = CheckBinaryVersion(data, m.Version); err != nil {
				return d, proto.NewError(proto.CodeReleaseVersion, "", proto.StateNotSent)
			}
			if err = CheckAgentBuild(data, local.Artifacts[i]); err != nil {
				return d, proto.NewError(proto.CodeReleaseUntrusted, "", proto.StateNotSent)
			}
			if force && p.RollbackDigests[target] != b.SHA256 {
				return d, proto.NewError(proto.CodeReleaseVersion, "", proto.StateNotSent)
			}
			d.Digest = b.SHA256
			d.Rollback = force
			return d, nil
		}
	}
	return d, proto.NewError(proto.CodeUnsupportedPlatform, "", proto.StateNotSent)
}
