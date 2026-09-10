//go:build !windows

package synctree

import (
	"errors"
	"os"
)

func PrivateDirectory(path string) error {
	if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := nativeLstatPath(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || !privateInfo(info, 0700) {
		return errors.New("sync state requires a private owned directory")
	}
	return nil
}
