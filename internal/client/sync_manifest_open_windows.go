//go:build windows

package client

import "os"

// Windows is not a supported local runtime. Keep build compatibility; callers
// still perform Lstat and post-transfer verification before trusting a digest.
func openManifestFile(path string) (*os.File, error) { return os.Open(path) }
