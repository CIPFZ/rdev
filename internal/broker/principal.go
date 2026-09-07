package broker

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

// PrincipalToken binds a declared owner to a daemon-local secret. It is an
// optional second factor for same-UID clients; peer credentials still gate
// socket access, while this prevents arbitrary owner spoofing.
func PrincipalToken(secret string, owner Owner) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(owner.Key()))
	return hex.EncodeToString(mac.Sum(nil))
}

func ValidatePrincipalToken(secret string, owner Owner, token string) bool {
	if secret == "" || token == "" {
		return false
	}
	want := PrincipalToken(secret, owner)
	return hmac.Equal([]byte(want), []byte(token))
}
