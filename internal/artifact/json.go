// Package artifact verifies release identity independently of transport and broker policy.
package artifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
)

const MaxDocumentBytes = 4 << 20

// Decode rejects duplicate keys (including escaped spellings), unknown struct
// fields, null, excessive nesting and trailing documents before interpretation.
func Decode(data []byte, out any) error { return decode(data, out, false) }

// DecodeEvidence retains legitimate null fields in Go buildinfo and SBOM.
func DecodeEvidence(data []byte, out any) error { return decode(data, out, true) }

func decode(data []byte, out any, allowNull bool) error {
	if len(data) == 0 || len(data) > MaxDocumentBytes {
		return errors.New("invalid document size")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := unique(d, 0, allowNull); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("expected one JSON document")
	}
	if err := exactFields(data, reflect.TypeOf(out), !allowNull); err != nil {
		return err
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	return d.Decode(out)
}
func unique(d *json.Decoder, depth int, allowNull bool) error {
	if depth > 64 {
		return errors.New("JSON nesting limit")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if t == nil && !allowNull {
		return errors.New("null is not a release value")
	}
	switch t {
	case json.Delim('{'):
		seen := map[string]bool{}
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return err
			}
			s, ok := k.(string)
			if !ok || seen[s] {
				return errors.New("duplicate JSON field")
			}
			seen[s] = true
			if err := unique(d, depth+1, allowNull); err != nil {
				return err
			}
		}
		t, err = d.Token()
		if err != nil || t != json.Delim('}') {
			return errors.New("invalid object")
		}
	case json.Delim('['):
		for d.More() {
			if err := unique(d, depth+1, allowNull); err != nil {
				return err
			}
		}
		t, err = d.Token()
		if err != nil || t != json.Delim(']') {
			return errors.New("invalid array")
		}
	}
	return nil
}

// encoding/json accepts case-insensitive struct field aliases. Trust documents
// deliberately do not: the signed spelling must be the schema spelling.
func exactFields(raw json.RawMessage, typ reflect.Type, required bool) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == reflect.TypeFor[json.RawMessage]() || reflect.PointerTo(typ).Implements(reflect.TypeFor[json.Unmarshaler]()) {
		return nil
	}
	switch typ.Kind() {
	case reflect.Struct:
		var values map[string]json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		fields := map[string]reflect.Type{}
		requiredFields := map[string]bool{}
		var collect func(reflect.Type)
		collect = func(t reflect.Type) {
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if f.PkgPath != "" {
					continue
				}
				tag := strings.Split(f.Tag.Get("json"), ",")[0]
				if tag == "-" {
					continue
				}
				ft := f.Type
				for ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if f.Anonymous && tag == "" && ft.Kind() == reflect.Struct {
					collect(ft)
					continue
				}
				if tag == "" {
					tag = f.Name
				}
				fields[tag] = f.Type
				requiredFields[tag] = f.Tag.Get("json") != "" && !strings.Contains(f.Tag.Get("json"), ",omitempty")
			}
		}
		collect(typ)
		if required {
			for name, needed := range requiredFields {
				if _, ok := values[name]; needed && !ok {
					return errors.New("missing required JSON field")
				}
			}
		}
		for key, value := range values {
			field, ok := fields[key]
			if !ok {
				return errors.New("unknown or noncanonical JSON field")
			}
			if err := exactFields(value, field, required); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 {
			return nil
		}
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		for _, v := range values {
			if err := exactFields(v, typ.Elem(), required); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return err
		}
		for _, v := range values {
			if err := exactFields(v, typ.Elem(), required); err != nil {
				return err
			}
		}
	}
	return nil
}
