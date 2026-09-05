//go:build darwin

package broker

import (
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func peerIsCurrentUser(conn *net.UnixConn) (bool, error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false, err
	}
	var uid uint32
	var inner error
	err = raw.Control(func(fd uintptr) {
		c, e := unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		if e != nil {
			inner = e
			return
		}
		uid = c.Uid
	})
	if err != nil {
		return false, err
	}
	if inner != nil {
		return false, inner
	}
	return uid == uint32(os.Getuid()), nil
}
