package synctree

import (
	"context"
	"os"
)

func lockStore(ctx context.Context, path string) (func(), error) { return nil, ErrStage }
func setLinkTime(root *os.Root, name string, ns int64) error     { return ErrStage }
