//go:build darwin || linux

package broker

import "net"

// PeerIsCurrentUser enforces the OS-level identity boundary for broker clients.
func PeerIsCurrentUser(conn *net.UnixConn) (bool, error) { return peerIsCurrentUser(conn) }
