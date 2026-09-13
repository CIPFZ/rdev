// Package keys owns rdev-generated SSH identities. It never discovers or
// adopts keys from the user's existing SSH configuration.
package keys

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Identity struct {
	DeviceID string
	HostID   string
	Private  string
	Public   string
	Metadata string
}

func DefaultRoot() (string, error) {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		base = filepath.Join(h, ".config")
	}
	if !filepath.IsAbs(base) {
		return "", errors.New("key config root must be absolute")
	}
	return filepath.Join(base, "rdev", "keys"), nil
}

func (i Identity) Validate() error {
	priv, err := os.ReadFile(i.Private)
	if err != nil {
		return fmt.Errorf("read private key: %w", err)
	}
	pub, err := os.ReadFile(i.Public)
	if err != nil {
		return fmt.Errorf("read public key: %w", err)
	}
	ps, err := os.Stat(i.Private)
	if err != nil || ps.Mode().Perm() != 0600 {
		return errors.New("private key is not mode 0600")
	}
	if len(priv) != ed25519.PrivateKeySize || len(pub) != ed25519.PublicKeySize {
		return errors.New("key files have invalid length")
	}
	if !ed25519.PublicKey(priv[32:]).Equal(ed25519.PublicKey(pub)) {
		return errors.New("private and public keys do not match")
	}
	meta, err := os.ReadFile(i.Metadata)
	if err != nil {
		return fmt.Errorf("read key metadata: %w", err)
	}
	var m struct {
		DeviceID     string `json:"device_id"`
		HostID       string `json:"host_id"`
		PublicSHA256 string `json:"public_sha256"`
	}
	if json.Unmarshal(meta, &m) != nil || m.DeviceID != i.DeviceID || m.HostID != i.HostID {
		return errors.New("key metadata does not match identity")
	}
	sum := sha256.Sum256(pub)
	if m.PublicSHA256 != hex.EncodeToString(sum[:]) {
		return errors.New("key fingerprint does not match metadata")
	}
	return nil
}

func HostID(user, address string, port int, namespace string) string {
	h := sha256.New()
	fmt.Fprintf(h, "user=%s\x00address=%s\x00port=%d\x00namespace=%s", user, address, port, namespace)
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func Open(root, user, address string, port int, namespace string) (Identity, error) {
	if root == "" || filepath.IsAbs(root) == false {
		return Identity{}, errors.New("key root must be absolute")
	}
	devicePath := filepath.Join(root, "device-id")
	if err := os.MkdirAll(root, 0700); err != nil {
		return Identity{}, err
	}
	st, err := os.Lstat(root)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return Identity{}, errors.New("key root must be a real directory")
	}
	if err := os.Chmod(root, 0700); err != nil {
		return Identity{}, err
	}
	device, err := readOrCreateID(devicePath)
	if err != nil {
		return Identity{}, err
	}
	host := HostID(user, address, port, namespace)
	dir := filepath.Join(root, "hosts", host)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return Identity{}, err
	}
	st, err = os.Lstat(dir)
	if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return Identity{}, errors.New("host key directory must be a real directory")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return Identity{}, err
	}
	return Identity{DeviceID: device, HostID: host, Private: filepath.Join(dir, "id_ed25519"), Public: filepath.Join(dir, "id_ed25519.pub"), Metadata: filepath.Join(dir, "metadata.json")}, nil
}

func (i Identity) Generate() error {
	if i.Private == "" || i.Public == "" {
		return errors.New("identity paths required")
	}
	if _, err := os.Stat(i.Private); err == nil {
		return errors.New("private key already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := atomicCreate(i.Private, priv); err != nil {
		return err
	}
	if err := atomicCreate(i.Public, pub); err != nil {
		_ = os.Remove(i.Private)
		return err
	}
	sum := sha256.Sum256(pub)
	meta, err := json.Marshal(struct {
		DeviceID     string `json:"device_id"`
		HostID       string `json:"host_id"`
		PublicSHA256 string `json:"public_sha256"`
	}{i.DeviceID, i.HostID, hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	if err := atomicCreate(i.Metadata, meta); err != nil {
		_ = os.Remove(i.Private)
		_ = os.Remove(i.Public)
		return err
	}
	return nil
}

func atomicCreate(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".rdev-key-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	// Link publishes the fully written inode without replacing an existing key.
	// This gives concurrent creators one winner and leaves no partial destination.
	return os.Link(tmp, path)
}

func readOrCreateID(path string) (string, error) {
	return readOrCreateIDAttempt(path, 0)
}

func readOrCreateIDAttempt(path string, attempt int) (string, error) {
	if b, err := os.ReadFile(path); err == nil {
		if attempt > 4 {
			return "", errors.New("device id changed concurrently")
		}
		st, statErr := os.Lstat(path)
		if statErr != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
			return "", errors.New("device id file is not private")
		}
		id := strings.TrimSpace(string(b))
		if len(id) >= 16 && len(id) <= 128 {
			return id, nil
		}
		return "", errors.New("invalid device id")
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return readOrCreateIDAttempt(path, attempt+1)
		}
		return "", err
	}
	if _, err = f.WriteString(id + "\n"); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return id, nil
}

func atomicPrivate(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".rdev-key-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
