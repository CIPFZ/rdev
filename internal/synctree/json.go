package synctree

import "encoding/json"

// JSON carries paths as base64 byte strings, preserving Unix names which are
// not valid UTF-8. Digests and execution must refer to the same byte-exact path.
type entryJSON struct {
	Path       []byte `json:"path"`
	Kind       string `json:"kind"`
	Size       int64  `json:"size"`
	Mode       uint32 `json:"mode"`
	ModifiedNS int64  `json:"modified_ns"`
	Digest     string `json:"digest,omitempty"`
	Link       []byte `json:"link,omitempty"`
}

func (e Entry) MarshalJSON() ([]byte, error) {
	return json.Marshal(entryJSON{[]byte(e.Path), e.Kind, e.Size, e.Mode, e.ModifiedNS, e.Digest, []byte(e.Link)})
}
func (e *Entry) UnmarshalJSON(data []byte) error {
	var wire entryJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*e = Entry{Path: string(wire.Path), Kind: wire.Kind, Size: wire.Size, Mode: wire.Mode, ModifiedNS: wire.ModifiedNS, Digest: wire.Digest, Link: string(wire.Link)}
	return nil
}
func (c Change) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Path   []byte `json:"path"`
		Before *Entry `json:"before,omitempty"`
		After  *Entry `json:"after,omitempty"`
	}{[]byte(c.Path), c.Before, c.After})
}
func (c *Change) UnmarshalJSON(data []byte) error {
	var wire struct {
		Path   []byte `json:"path"`
		Before *Entry `json:"before,omitempty"`
		After  *Entry `json:"after,omitempty"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*c = Change{string(wire.Path), wire.Before, wire.After}
	return nil
}

// The source basename follows the same byte-preserving contract as entry paths.
func (s Stage) MarshalJSON() ([]byte, error) {
	type alias Stage
	return json.Marshal(struct {
		alias
		SourceName []byte `json:"source_name"`
	}{alias(s), []byte(s.SourceName)})
}
func (s *Stage) UnmarshalJSON(data []byte) error {
	type alias Stage
	var wire struct {
		alias
		SourceName []byte `json:"source_name"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*s = Stage(wire.alias)
	s.SourceName = string(wire.SourceName)
	return nil
}
