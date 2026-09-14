package artifact

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// PolicyDiagnosis is a read-only explanation of the policy selected for this
// process. It deliberately contains no administrator key or private path beyond
// the path the caller explicitly selected.
type PolicyDiagnosis struct {
	Path    string `json:"path"`
	Source  string `json:"source"`
	Exists  bool   `json:"exists"`
	Valid   bool   `json:"valid"`
	Expired bool   `json:"expired"`
	Error   string `json:"error,omitempty"`
	Action  string `json:"action"`
}

func DiagnosePolicy(now time.Time) PolicyDiagnosis {
	path := os.Getenv("RDEV_RELEASE_POLICY")
	source := "default"
	if path != "" {
		source = "RDEV_RELEASE_POLICY"
	} else {
		path, _ = DefaultPolicyPath()
	}
	d := PolicyDiagnosis{Path: path, Source: source, Action: "policy is ready for release admission"}
	st, err := os.Lstat(filepath.Clean(path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			d.Error = "policy is missing"
			d.Action = "run make dev-policy or create an administrator policy"
			return d
		}
		d.Error = fmt.Sprintf("cannot inspect policy: %v", err)
		d.Action = "fix policy path access and retry"
		return d
	}
	d.Exists = true
	if st.Mode()&os.ModeSymlink != 0 {
		d.Error = "policy path is a symlink"
		d.Action = "replace it with a private regular file"
		return d
	}
	p, err := LoadPolicy(path, now)
	if err != nil {
		d.Error = err.Error()
		if !p.ValidUntil.IsZero() && !p.ValidUntil.After(now) {
			d.Expired = true
			d.Action = "renew the administrator policy before retrying"
		} else {
			d.Action = "repair the policy JSON, ownership, permissions or schema"
		}
		return d
	}
	d.Valid = true
	return d
}
