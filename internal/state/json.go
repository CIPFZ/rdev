package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const MaxMetadataBytes = 1 << 20

// ValidateRecordSchema preserves the historical missing-version format while
// refusing explicit zero, future versions and ambiguous duplicate JSON keys.
func ValidateRecordSchema(data []byte) (int, error) {
	var fields map[string]json.RawMessage
	if err := decodeMetadata(data, &fields, false); err != nil {
		return 0, err
	}
	if fields == nil {
		return 0, errors.New("state record must be an object")
	}
	for key := range fields {
		if key != "schema_version" && strings.EqualFold(key, "schema_version") {
			return 0, errors.New("invalid state schema field spelling")
		}
	}
	raw, exists := fields["schema_version"]
	if !exists {
		return 0, nil
	}
	var version int
	if bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &version) != nil || version <= 0 {
		return 0, errors.New("invalid state record schema")
	}
	if version > CurrentSchemaVersion {
		return version, ErrFutureSchema
	}
	return version, nil
}

func decodeMetadata(data []byte, out any, strict bool) error {
	if len(data) > MaxMetadataBytes {
		return errors.New("state metadata exceeds budget")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := checkJSONValue(d, 0); err != nil {
		return err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing state metadata")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	if strict {
		d.DisallowUnknownFields()
	}
	return d.Decode(out)
}

func checkJSONValue(d *json.Decoder, depth int) error {
	if depth > 64 {
		return errors.New("state metadata nesting exceeds budget")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return errors.New("duplicate state metadata field")
			}
			seen[name] = true
			if err := checkJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := checkJSONValue(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid state metadata delimiter")
	}
	_, err = d.Token()
	return err
}
