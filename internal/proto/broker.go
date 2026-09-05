package proto

import "fmt"

// BrokerProtocolVersion is the local client/broker protocol version. It is
// deliberately separate from the remote agent wire version.
const BrokerProtocolVersion = 1

// BrokerMinVersion is the oldest broker protocol this build can speak.
const BrokerMinVersion = 1

// BrokerHello is exchanged before any broker request is accepted.
type BrokerHello struct {
	Version    int      `json:"version"`
	MinVersion int      `json:"min_version"`
	ClientID   string   `json:"client_id,omitempty"`
	Features   []string `json:"features,omitempty"`
}

// BrokerHelloResponse reports the negotiated protocol range.
type BrokerHelloResponse struct {
	OK         bool     `json:"ok"`
	Version    int      `json:"version,omitempty"`
	MinVersion int      `json:"min_version,omitempty"`
	Features   []string `json:"features,omitempty"`
	Error      string   `json:"error,omitempty"`
}

// NegotiateBrokerVersion returns the highest mutually supported version.
// A zero result means the ranges do not overlap.
func NegotiateBrokerVersion(local, remote BrokerHello) int {
	if local.Version < local.MinVersion || remote.Version < remote.MinVersion {
		return 0
	}
	hi := local.Version
	if remote.Version < hi { hi = remote.Version }
	lo := local.MinVersion
	if remote.MinVersion > lo { lo = remote.MinVersion }
	if hi < lo { return 0 }
	return hi
}

// ValidateBrokerHello rejects malformed or incompatible handshakes with a
// stable error suitable for presenting to a client.
func ValidateBrokerHello(local, remote BrokerHello) error {
	if remote.Version < remote.MinVersion || remote.MinVersion < 1 {
		return fmt.Errorf("invalid broker protocol range %d..%d", remote.MinVersion, remote.Version)
	}
	if v := NegotiateBrokerVersion(local, remote); v == 0 {
		return fmt.Errorf("incompatible broker protocol: local %d..%d, peer %d..%d", local.MinVersion, local.Version, remote.MinVersion, remote.Version)
	}
	return nil
}
