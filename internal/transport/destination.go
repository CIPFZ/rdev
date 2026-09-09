package transport

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

// ParseDestination accepts SSH aliases, DNS names, IPv4 and IPv6, optionally
// prefixed with user@. A bare IPv6 literal is always an address; an IPv6 port
// requires brackets. A separately specified nonzero port cannot be combined
// with an embedded port, even if their values happen to match.
func ParseDestination(destination string, port int) (string, int, error) {
	if destination == "" || port < 0 || port > 65535 {
		return "", 0, fmt.Errorf("invalid SSH destination or port (expected 0/default or 1..65535)")
	}
	for _, r := range destination {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return "", 0, fmt.Errorf("SSH destination contains whitespace or control characters")
		}
	}
	host, user := destination, ""
	if strings.Contains(host, "@") {
		var ok bool
		user, host, ok = strings.Cut(host, "@")
		if !ok || !validDestinationWord(user) || strings.Contains(host, "@") {
			return "", 0, fmt.Errorf("invalid SSH user")
		}
		user += "@"
	}
	embeddedPort := ""
	hasPort := false
	if strings.HasPrefix(host, "[") {
		end := strings.IndexByte(host, ']')
		if end < 0 {
			return "", 0, fmt.Errorf("unterminated IPv6 address")
		}
		literal, suffix := host[1:end], host[end+1:]
		ip, err := netip.ParseAddr(literal)
		if err != nil || !ip.Is6() || ip.Zone() != "" && !validDestinationWord(ip.Zone()) {
			return "", 0, fmt.Errorf("brackets require an IPv6 literal")
		}
		host = ip.String()
		if suffix != "" {
			if !strings.HasPrefix(suffix, ":") {
				return "", 0, fmt.Errorf("invalid text after IPv6 address")
			}
			embeddedPort, hasPort = suffix[1:], true
		}
	} else if strings.Count(host, ":") > 1 {
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is6() || ip.Zone() != "" && !validDestinationWord(ip.Zone()) {
			return "", 0, fmt.Errorf("invalid bare IPv6 address; use [IPv6]:port for a port")
		}
		host = ip.String()
	} else {
		host, embeddedPort, hasPort = strings.Cut(host, ":")
		if !validDestinationWord(host) {
			return "", 0, fmt.Errorf("invalid SSH hostname or alias")
		}
	}
	if hasPort {
		if port != 0 {
			return "", 0, fmt.Errorf("SSH port specified both in destination and separately")
		}
		for _, r := range embeddedPort {
			if r < '0' || r > '9' {
				return "", 0, fmt.Errorf("SSH port must be a decimal number within 1..65535")
			}
		}
		parsed, err := strconv.ParseUint(embeddedPort, 10, 16)
		if err != nil || parsed == 0 {
			return "", 0, fmt.Errorf("SSH port must be within 1..65535")
		}
		port = int(parsed)
	}
	return user + host, port, nil
}

func validDestinationWord(s string) bool {
	if s == "" || strings.HasPrefix(s, "-") {
		return false
	}
	for _, r := range s {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && !strings.ContainsRune("._-", r) {
			return false
		}
	}
	return true
}

// NormalizeHost is used at registry publication and immediately before SSH.
func NormalizeHost(h Host) (Host, error) {
	addr, port, err := ParseDestination(h.Addr, h.Port)
	if err != nil {
		return Host{}, err
	}
	if _, err := ValidateRemoteDir(h.RemoteDir); err != nil {
		return Host{}, err
	}
	h.Addr, h.Port = addr, port
	return h, nil
}

// RsyncDestination brackets IPv6 in rsync's host:path grammar. SSH itself uses
// the normalized unbracketed destination; the two argv grammars differ.
func RsyncDestination(addr string) string {
	user, host := "", addr
	if before, after, ok := strings.Cut(addr, "@"); ok {
		user, host = before+"@", after
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return user + host
}
