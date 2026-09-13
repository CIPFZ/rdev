package keys

import (
	"errors"
	"strings"
)

// AddAuthorizedKey appends key exactly once. Matching compares the key body,
// never the optional comment, so a forged comment cannot claim ownership.
func AddAuthorizedKey(existing, key, comment string) (updated string, changed bool, err error) {
	key = strings.TrimSpace(key)
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return existing, false, errors.New("authorized key is empty or multiline")
	}
	for _, line := range strings.Split(existing, "\n") {
		if authorizedKeyBody(line) == authorizedKeyBody(key) {
			return existing, false, nil
		}
	}
	line := key
	if comment != "" {
		line += " " + strings.TrimSpace(comment)
	}
	if existing != "" && !strings.HasSuffix(existing, "\n") {
		existing += "\n"
	}
	return existing + line + "\n", true, nil
}

// RemoveAuthorizedKey removes only lines whose key body equals key and keeps
// comments, unrelated keys and blank lines byte-for-byte.
func RemoveAuthorizedKey(existing, key string) (updated string, removed bool, err error) {
	key = authorizedKeyBody(key)
	if key == "" {
		return existing, false, errors.New("authorized key is empty")
	}
	var out []string
	for _, line := range strings.SplitAfter(existing, "\n") {
		if authorizedKeyBody(line) == key {
			removed = true
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, ""), removed, nil
}

func authorizedKeyBody(line string) string {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return ""
	}
	for i, f := range fields {
		if strings.HasPrefix(f, "ssh-") || strings.HasPrefix(f, "ecdsa-") || strings.HasPrefix(f, "sk-") {
			if i+1 < len(fields) {
				return f + " " + fields[i+1]
			}
			return ""
		}
	}
	return ""
}
