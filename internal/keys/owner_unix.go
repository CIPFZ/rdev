//go:build !windows

package keys

import (
	"fmt"
	"os"
	"syscall"
)

func validateOwner(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	raw, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot inspect owner of %s", path)
	}
	if int64(raw.Uid) != int64(os.Geteuid()) {
		return fmt.Errorf("key file %s is not owned by effective uid", path)
	}
	return nil
}
