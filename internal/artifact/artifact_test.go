package artifact

import (
	"context"
	"debug/buildinfo"
	"github.com/CIPFZ/rdev/internal/proto"
	"runtime"

	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func signedFixture(t *testing.T) (string, Policy, Manifest, string) {
	t.Helper()
	return signedFixturePlatforms(t, BinaryNames)
}

func signedFixturePlatforms(t *testing.T, binaryNames []string) (string, Policy, Manifest, string) {
	t.Helper()
	// Darwin's /tmp is a symlink; policy admission intentionally rejects it.
	tmpRoot, err := filepath.EvalSymlinks("/tmp")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TMPDIR", tmpRoot)
	if _, e := exec.LookPath("ssh-keygen"); e != nil {
		t.Fatal("OpenSSH ssh-keygen required for actual SSHSIG tests")
	}
	dir := t.TempDir()
	key := filepath.Join(dir, "test-signing-key")
	if b, e := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key).CombinedOutput(); e != nil {
		t.Fatalf("keygen: %v %s", e, b)
	}
	pub, e := os.ReadFile(key + ".pub")
	if e != nil {
		t.Fatal(e)
	}
	fields := strings.Fields(string(pub))
	now := time.Now().UTC().Truncate(time.Second)
	p := Policy{SchemaVersion: 1, ValidUntil: now.Add(24 * time.Hour), Channels: []string{"stable", "beta", "dev"}, AllowTestRoots: true, BundleDir: dir, Roots: []Root{{ID: "isolated-test", PublicKey: strings.Join(fields[:2], " "), Channels: []string{"stable", "beta", "dev"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(48 * time.Hour), TestOnly: true}}}
	m := Manifest{SchemaVersion: 1, Version: "1.2.3", Channel: "stable", Source: Source{Commit: strings.Repeat("a", 40), TreeSHA256: strings.Repeat("b", 64)}, Signer: "isolated-test", SignatureNamespace: SignatureNamespace, IssuedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
	repo := filepath.Join(dir, "source")
	if e = os.Mkdir(repo, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(repo, "go.mod"), []byte("module example.test/signingfixture\n\ngo 1.25.0\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(repo, "main.go"), []byte("package main\nimport \"fmt\"\nvar version=\"\"\nfunc main(){fmt.Println(version)}\n"), 0600); e != nil {
		t.Fatal(e)
	}
	for _, args := range [][]string{{"init", "-q"}, {"add", "go.mod", "main.go"}, {"-c", "user.name=isolated-test", "-c", "user.email=test@example.invalid", "commit", "-qm", "isolated signing fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		if b, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("fixture git %v %s", e, b)
		}
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = repo
	rev, e := cmd.Output()
	if e != nil {
		t.Fatal(e)
	}
	m.Source.Commit = strings.TrimSpace(string(rev))
	local := LocalEvidence{SchemaVersion: 1, Source: m.Source, Compatibility: fixtureCompatibility(), Evidence: map[string]string{}}
	for i, name := range binaryNames {
		osys, arch := "linux", "amd64"
		if i >= 2 {
			parts := strings.Split(name, "-")
			osys, arch = parts[2], parts[3]
		}
		cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-trimpath", "-ldflags=-X main.version=rdev-release-identity-v1[1.2.3]", "-o", filepath.Join(dir, name), ".")
		cmd.Dir = repo
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS="+osys, "GOARCH="+arch)
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("fixture build %v %s", e, out)
		}
		b, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil {
			t.Fatal(e)
		}
		info, e := buildinfo.ReadFile(filepath.Join(dir, name))
		if e != nil {
			t.Fatal(e)
		}
		m.Binaries = append(m.Binaries, Binary{File: File{Name: name, SHA256: Hash(b), Size: int64(len(b))}, GOOS: osys, GOARCH: arch})
		local.Artifacts = append(local.Artifacts, LocalArtifact{Name: name, SHA256: Hash(b), Size: int64(len(b)), GOOS: osys, GOARCH: arch, Build: info})
	}

	meta, _ := json.Marshal(local)
	if e = os.WriteFile(filepath.Join(dir, "manifest.json"), meta, 0600); e != nil {
		t.Fatal(e)
	}
	for _, name := range MetadataNames {
		var b []byte
		switch name {
		case "manifest.json":
			b = meta
		case "sbom.cdx.json":
			b, _ = json.Marshal(SBOM(local))
		case "provenance.intoto.json":
			b, _ = json.Marshal(Provenance(local))
		default:
			b = []byte("isolated test fixture notices")
		}
		if e = os.WriteFile(filepath.Join(dir, name), b, 0600); e != nil {
			t.Fatal(e)
		}
		m.Metadata = append(m.Metadata, File{Name: name, Size: int64(len(b)), SHA256: Hash(b)})
	}

	signFixture(t, dir, key, m)
	return dir, p, m, key
}
func signFixture(t *testing.T, dir, key string, m Manifest) {
	t.Helper()
	b, e := json.Marshal(m)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, ManifestName), b, 0600); e != nil {
		t.Fatal(e)
	}
	_ = os.Remove(filepath.Join(dir, SignatureName))
	if b, e := exec.Command("ssh-keygen", "-Y", "sign", "-f", key, "-n", SignatureNamespace, filepath.Join(dir, ManifestName)).CombinedOutput(); e != nil {
		t.Fatalf("sign: %v %s", e, b)
	}
}
func clonePolicy(p Policy) Policy {
	b, _ := json.Marshal(p)
	var q Policy
	_ = json.Unmarshal(b, &q)
	return q
}

func TestRealSSHSIGBundleAndTamper(t *testing.T) {
	dir, p, _, _ := signedFixture(t)
	if _, d, e := VerifyBundle(context.Background(), dir, p, time.Now()); e != nil || !d.TestRoot || d.Unsigned {
		t.Fatalf("signed test-root bundle: %+v %v", d, e)
	}
	for _, name := range append(append(append([]string{}, BinaryNames...), MetadataNames...), ManifestName, SignatureName) {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			b, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			defer os.WriteFile(path, b, 0600)
			bad := append([]byte{}, b...)
			bad[len(bad)/2] ^= 1
			if e = os.WriteFile(path, bad, 0600); e != nil {
				t.Fatal(e)
			}
			if _, _, e = VerifyBundle(context.Background(), dir, p, time.Now()); e == nil {
				t.Fatal("tampered bytes accepted")
			}
		})
	}
}

func TestHistoricalSixBinarySignedBundle(t *testing.T) {
	// Phase 8 releases contain four agents. Adding Windows to current bundles
	// must not invent a Windows SBOM edge or invalidate their signed metadata.
	dir, policy, manifest, _ := signedFixturePlatforms(t, BinaryNames[:6])
	verified, decision, err := VerifyBundle(context.Background(), dir, policy, time.Now())
	if err != nil || decision.Unsigned || !decision.TestRoot || len(verified.Binaries) != len(manifest.Binaries) {
		t.Fatalf("historical signed bundle: %+v %v", decision, err)
	}
}
func TestTrustedPolicyCannotBeReplacedBySignerClaim(t *testing.T) {
	dir, p, _, _ := signedFixture(t)
	cases := map[string]func(*Policy){
		"no roots":     func(p *Policy) { p.Roots = nil },
		"wrong signer": func(p *Policy) { p.Roots[0].ID = "other" },
		"wrong key": func(p *Policy) {
			p.Roots[0].PublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGdnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dnZ2dn"
		},
		"revoked":          func(p *Policy) { p.Roots[0].Revoked = true },
		"root expired":     func(p *Policy) { p.Roots[0].NotAfter = time.Now().Add(-time.Minute) },
		"root not valid":   func(p *Policy) { p.Roots[0].NotBefore = time.Now().Add(time.Minute) },
		"root channel":     func(p *Policy) { p.Roots[0].Channels = []string{"dev"} },
		"channel denied":   func(p *Policy) { p.Channels = []string{"dev"} },
		"version pin":      func(p *Policy) { p.PinVersion = "1.2.4" },
		"test root denied": func(p *Policy) { p.AllowTestRoots = false },
		"policy expired":   func(p *Policy) { p.ValidUntil = time.Now().Add(-time.Minute) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			q := clonePolicy(p)
			change(&q)
			if _, _, e := VerifyBundle(context.Background(), dir, q, time.Now()); e == nil {
				t.Fatal("untrusted release accepted")
			}
		})
	}
}
func TestSignedMalformedReleaseStillRejected(t *testing.T) {
	dir, p, m, key := signedFixture(t)
	cases := map[string]func(*Manifest){
		"future":                func(m *Manifest) { m.SchemaVersion = 2 },
		"wrong platform":        func(m *Manifest) { m.Binaries[2].GOOS = "darwin" },
		"wrong channel":         func(m *Manifest) { m.Channel = "beta" },
		"wrong version":         func(m *Manifest) { m.Version = "latest" },
		"dirty stable":          func(m *Manifest) { m.Source.Dirty = true },
		"duplicate artifact":    func(m *Manifest) { m.Binaries[1] = m.Binaries[0] },
		"traversal":             func(m *Manifest) { m.Metadata[0].Name = "../manifest.json" },
		"expired":               func(m *Manifest) { m.ExpiresAt = time.Now().Add(-time.Second) },
		"future issued":         func(m *Manifest) { m.IssuedAt = time.Now().Add(time.Hour) },
		"purpose":               func(m *Manifest) { m.SignatureNamespace = "file" },
		"metadata substitution": func(m *Manifest) { m.Metadata[0].SHA256 = strings.Repeat("c", 64) },
	}
	original, _ := json.Marshal(m)
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			var q Manifest
			_ = json.Unmarshal(original, &q)
			change(&q)
			signFixture(t, dir, key, q)
			if _, _, e := VerifyBundle(context.Background(), dir, p, time.Now()); e == nil {
				t.Fatal("signed invalid identity accepted")
			}
		})
	}
}
func TestStrictManifestAndSignatureMetadataJSON(t *testing.T) {
	for _, s := range []string{`null`, `{"schema_version":1,"schema_version":1}`, `{"x": {"a":1,"\u0061":2}}`, `{"schema_version":1,"unknown":1}`, `{} {}`, `{"roots":null}`, `{"schema_version":1,"SCHEMA_VERSION":2}`, strings.Repeat("[", 66) + "1" + strings.Repeat("]", 66)} {
		var m Manifest
		if Decode([]byte(s), &m) == nil {
			t.Fatalf("ambiguous input accepted: %s", s)
		}
	}
}
func TestRuntimeAgentPolicyAndHostNarrowing(t *testing.T) {
	dir, p, m, _ := signedFixture(t)
	path := filepath.Join(dir, "policy.json")
	t.Setenv("RDEV_RELEASE_POLICY", path)
	save := func() {
		b, _ := json.Marshal(p)
		if e := os.WriteFile(path, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	save()
	data, _ := os.ReadFile(filepath.Join(dir, m.Binaries[2].Name))
	target := strings.Repeat("c", 64)
	if _, e := AuthorizeAgent(context.Background(), data, "linux", "amd64", target, false); e != nil {
		t.Fatal(e)
	}
	if _, e := AuthorizeAgent(context.Background(), data, "linux", "arm64", target, false); e == nil {
		t.Fatal("wrong platform passed")
	}
	if _, e := AuthorizeAgent(context.Background(), data, "linux", "amd64", target, true); e == nil {
		t.Fatal("force bypass")
	}
	p.Hosts = map[string]Restriction{target: {Channels: []string{"dev"}}}
	save()
	if _, e := AuthorizeAgent(context.Background(), data, "linux", "amd64", target, false); e == nil {
		t.Fatal("host narrowing bypass")
	}
	p.BundleDir = ""
	p.Hosts = nil
	p.Roots = []Root{}
	p.AllowUnsignedDev = false
	save()
	if _, e := AuthorizeAgent(context.Background(), data, "linux", "amd64", target, false); e == nil {
		t.Fatal("unsigned default accepted")
	}
	p.AllowUnsignedDev = true
	save()
	if d, e := AuthorizeAgent(context.Background(), data, "linux", "amd64", target, false); e != nil || !d.Unsigned {
		t.Fatalf("explicit dev: %+v %v", d, e)
	}
	p.BundleDir = dir
	save()
	_ = os.Remove(filepath.Join(dir, SignatureName))
	if _, e := AuthorizeAgent(context.Background(), data, "linux", "amd64", target, false); e == nil {
		t.Fatal("missing signature fell back to unsigned")
	}
}
func FuzzManifest(f *testing.F) {
	f.Add([]byte(`{"schema_version":1}`))
	f.Add([]byte(`{"schema_version":1,"schema_version":2}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		var m Manifest
		if Decode(b, &m) == nil {
			_ = m.Validate(time.Unix(1800000000, 0))
		}
	})
}
func FuzzSignaturePolicy(f *testing.F) {
	f.Add([]byte(`{"schema_version":1,"roots":[]}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		var p Policy
		if Decode(b, &p) == nil {
			_ = p.Validate(time.Unix(1800000000, 0))
		}
	})
}

func fixtureCompatibility() json.RawMessage {
	c := Compatibility{SchemaVersion: 1, Release: "1.2.3", Protocols: []CompatibilityProtocol{}, Formats: []CompatibilityFormat{{Name: "agent_state", Current: 1}}, Errors: CompatibilityErrors{Version: proto.ErrorContractVersion, Codes: proto.ErrorDescriptors()}, BreakingChanges: []string{}, Timeouts: map[string]int{"exec_default": 60, "job_wait_default": 300, "new_job_wall_default": 3600, "hard_maximum": proto.MaxTimeoutSeconds}}
	for _, name := range []string{"client_agent", "client_broker", "broker_agent"} {
		c.Protocols = append(c.Protocols, CompatibilityProtocol{Name: name, Range: proto.ProtocolRange{Min: 1, Max: 1}})
	}
	b, _ := json.Marshal(c)
	return b
}
