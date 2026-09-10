package artifact

import (
	"errors"
	"os"

	"github.com/CIPFZ/rdev/internal/winutil"
)

func openArtifact(path string) (*os.File, error) {
	f, err := winutil.Open(path, os.O_RDONLY, false)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("artifact is not a regular file")
	}
	return f, nil
}
