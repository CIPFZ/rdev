//go:build !linux && !darwin

package artifact

import "errors"

func checkPrivatePath(string) error {
	return errors.New("release policy ownership validation unsupported on this platform")
}

func readPolicyFile(string) ([]byte, error) {
	return nil, errors.New("release policy unsupported on this platform")
}
