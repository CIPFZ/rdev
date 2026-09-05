package broker

import (
	"errors"
	"strings"
)

// Owner identifies the local principal and project that own broker state.
type Owner struct {
	ClientID  string `json:"client_id"`
	ProjectID string `json:"project_id"`
}

func (o Owner) Validate() error {
	if strings.TrimSpace(o.ClientID) == "" {
		return errors.New("client_id required")
	}
	if strings.TrimSpace(o.ProjectID) == "" {
		return errors.New("project_id required")
	}
	if len(o.ClientID) > 128 || len(o.ProjectID) > 256 {
		return errors.New("owner identity too long")
	}
	return nil
}

func (o Owner) Key() string { return o.ClientID + "\x00" + o.ProjectID }
