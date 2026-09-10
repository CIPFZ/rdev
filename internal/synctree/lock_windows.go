package synctree

import (
	"context"
	"errors"
	"github.com/CIPFZ/rdev/internal/winutil"
	"os"
)

func lockStore(ctx context.Context, path string) (func(), error) {
	f, err := winutil.OpenLock(path, os.O_RDWR|os.O_CREATE)
	if err != nil {
		return nil, err
	}
	if err = winutil.LockContext(ctx, f, true); err != nil {
		f.Close()
		return nil, err
	}
	return func() { _ = winutil.Unlock(f); _ = f.Close() }, nil
}
func setLinkTime(*os.Root, string, int64) error {
	return errors.New("Windows sync symlinks are unsupported")
}
