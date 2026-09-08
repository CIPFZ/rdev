package broker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const MaxPrincipalTokenTTL = 24 * time.Hour

// Principal tokens authorize a protocol caller to declare one owner. The signing
// key belongs only to the daemon and provisioning administrator; clients receive
// the resulting bearer token. This is not an OS sandbox: processes with access
// to the daemon's signing key can mint credentials for any owner.
type principalClaims struct {
	Version int   `json:"v"`
	Owner   Owner `json:"owner"`
	Expires int64 `json:"exp"`
}

func ValidatePrincipalSecret(secret string) error {
	if len(secret) < 32 {
		return errors.New("principal signing secret must contain at least 32 bytes")
	}
	return nil
}

// MintPrincipalToken provisions an owner-scoped, expiring bearer credential.
// Rotating the daemon signing key and restarting revokes all issued credentials.
func MintPrincipalToken(secret string, owner Owner, expires time.Time) (string, error) {
	if err := ValidatePrincipalSecret(secret); err != nil {
		return "", err
	}
	if err := owner.Validate(); err != nil {
		return "", err
	}
	now := time.Now()
	if !expires.After(now) || expires.After(now.Add(MaxPrincipalTokenTTL)) || expires.Unix() <= now.Unix() {
		return "", errors.New("principal token expiry must be in the next 24 hours")
	}
	payload, err := json.Marshal(principalClaims{Version: 1, Owner: owner, Expires: expires.Unix()})
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(encoded))
	return encoded + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// PrincipalToken is a compatibility helper for tests and in-process callers.
// Production clients must obtain credentials from `rdevd principal-token`.
func PrincipalToken(secret string, owner Owner) string {
	token, _ := MintPrincipalToken(secret, owner, time.Now().Add(time.Hour))
	return token
}

// PrincipalTokenExpiry validates the signature, exact owner and expiration. The
// caller must also end established sessions at the returned deadline.
func PrincipalTokenExpiry(secret string, owner Owner, token string) (time.Time, error) {
	invalid := errors.New("invalid or expired principal credential")
	if ValidatePrincipalSecret(secret) != nil || owner.Validate() != nil || len(token) > 4096 {
		return time.Time{}, invalid
	}
	encoded, signature, ok := strings.Cut(token, ".")
	if !ok {
		return time.Time{}, invalid
	}
	provided, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		return time.Time{}, invalid
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(encoded))
	if !hmac.Equal(mac.Sum(nil), provided) {
		return time.Time{}, invalid
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return time.Time{}, invalid
	}
	var claims principalClaims
	if json.Unmarshal(payload, &claims) != nil || claims.Version != 1 || claims.Owner != owner {
		return time.Time{}, invalid
	}
	expires := time.Unix(claims.Expires, 0)
	if !expires.After(time.Now()) {
		return time.Time{}, invalid
	}
	return expires, nil
}

func ValidatePrincipalToken(secret string, owner Owner, token string) bool {
	_, err := PrincipalTokenExpiry(secret, owner, token)
	return err == nil
}
