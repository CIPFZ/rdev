package release

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	artifactcontract "github.com/CIPFZ/rdev/internal/artifact"
)

// PrepareSigned freezes the existing audited artifacts and metadata into the
// final SSHSIG payload. No artifact embeds this outer manifest or its signature.
func PrepareSigned(dir, version, channel, signer string, issued, expires time.Time) error {
	if err := Verify(dir); err != nil {
		return err
	}
	var local Manifest
	if err := ReadJSON(filepath.Join(dir, "manifest.json"), &local); err != nil {
		return err
	}
	m := artifactcontract.Manifest{SchemaVersion: artifactcontract.SchemaVersion, Version: version, Channel: channel, Source: artifactcontract.Source{Commit: local.Source.Commit, TreeSHA256: local.Source.TreeSHA256, Dirty: local.Source.Dirty}, Signer: signer, SignatureNamespace: artifactcontract.SignatureNamespace, IssuedAt: issued, ExpiresAt: expires}
	for _, a := range local.Artifacts {
		data, err := artifactcontract.ReadExpected(dir, artifactcontract.File{Name: a.Name, Size: a.Size, SHA256: a.SHA256})
		if err != nil {
			return err
		}
		if err = artifactcontract.CheckBinaryVersion(data, version); err != nil {
			return fmt.Errorf("%s: %w", a.Name, err)
		}
		m.Binaries = append(m.Binaries, artifactcontract.Binary{File: artifactcontract.File{Name: a.Name, SHA256: a.SHA256, Size: a.Size}, GOOS: a.GOOS, GOARCH: a.GOARCH})
	}
	for _, name := range artifactcontract.MetadataNames {
		data, err := artifactcontract.ReadFile(dir, name, artifactcontract.MaxDocumentBytes)
		if err != nil {
			return err
		}
		m.Metadata = append(m.Metadata, artifactcontract.File{Name: name, Size: int64(len(data)), SHA256: artifactcontract.Hash(data)})
	}
	if err := m.Validate(time.Now()); err != nil {
		return err
	}
	if _, err := artifactcontract.CheckBinding(dir, m); err != nil {
		return err
	}
	if err := verifyExpected(dir, m.Metadata[0]); err != nil {
		return err
	}
	// Refuse overwriting an already prepared/signed identity; create a fresh
	// output directory to issue a different candidate.
	path := filepath.Join(dir, artifactcontract.ManifestName)
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.New("release identity already exists")
	}
	return WriteJSON(path, m)
}

func Sign(ctx context.Context, dir, key string) error {
	raw, err := artifactcontract.ReadFile(dir, artifactcontract.ManifestName, artifactcontract.MaxDocumentBytes)
	if err != nil {
		return err
	}
	var m artifactcontract.Manifest
	if err = artifactcontract.Decode(raw, &m); err != nil {
		return err
	}
	if err = m.Validate(time.Now()); err != nil {
		return err
	}
	local, err := artifactcontract.CheckBinding(dir, m)
	if err != nil {
		return err
	}
	if err = verifyExpected(dir, m.Metadata[0]); err != nil {
		return err
	}
	for i, b := range m.Binaries {
		data, e := artifactcontract.ReadExpected(dir, b.File)
		if e != nil {
			return e
		}
		if e = artifactcontract.CheckBinaryVersion(data, m.Version); e != nil {
			return e
		}
		if err = artifactcontract.CheckAgentBuild(data, local.Artifacts[i]); err != nil {
			return err
		}
	}
	for _, f := range m.Metadata {
		if err = artifactcontract.CheckFile(dir, f); err != nil {
			return err
		}
	}
	if _, err = os.Lstat(filepath.Join(dir, artifactcontract.SignatureName)); !errors.Is(err, os.ErrNotExist) {
		return errors.New("signature already exists")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	// ssh-keygen emits a detached SSHSIG; the key may also identify an SSH agent
	// backed signer. Signing is separate from tests and never publishes anything.
	tmp, err := os.MkdirTemp("/tmp", "rdev-release-sign-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	payload := filepath.Join(tmp, "release.json")
	if err = os.WriteFile(payload, raw, 0600); err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "ssh-keygen", "-Y", "sign", "-f", key, "-n", artifactcontract.SignatureNamespace, payload)
	if err = cmd.Run(); err != nil {
		return errors.New("OpenSSH release signing failed")
	}
	current, err := artifactcontract.ReadFile(dir, artifactcontract.ManifestName, artifactcontract.MaxDocumentBytes)
	if err != nil || !bytes.Equal(current, raw) {
		return errors.New("release manifest changed during signing")
	}
	sig, err := os.ReadFile(payload + ".sig")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, artifactcontract.SignatureName), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(sig)
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	return err
}

func VerifySigned(ctx context.Context, dir, policyPath string) error {
	p, err := artifactcontract.LoadPolicy(policyPath, time.Now())
	if err != nil {
		return err
	}
	m, _, err := artifactcontract.VerifyBundle(ctx, dir, p, time.Now())
	if err != nil {
		return err
	}
	return verifyExpected(dir, m.Metadata[0])
}
