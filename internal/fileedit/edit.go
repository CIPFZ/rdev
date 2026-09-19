// Package fileedit implements bounded, deterministic edits of one text snapshot.
// It does not access the filesystem, execute a shell, or perform fuzzy matching.
package fileedit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"unicode/utf8"

	"github.com/CIPFZ/rdev/internal/proto"
)

const MaxBytes = 4 << 20
const MaxEdits = 128

func Digest(b []byte) string             { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func failure(code proto.ErrorCode) error { return proto.NewError(code, "", proto.StateFailed) }
func text(b []byte) bool                 { return utf8.Valid(b) && !bytes.ContainsRune(b, 0) }

func Validate(p *proto.EditParams) error {
	if p == nil || p.Path == "" || len(p.BaseDigest) != 64 {
		return failure(proto.CodeInvalidRequest)
	}
	d, err := hex.DecodeString(p.BaseDigest)
	if err != nil || hex.EncodeToString(d) != p.BaseDigest {
		return failure(proto.CodeInvalidRequest)
	}
	switch p.Kind {
	case "replace":
		if p.Content == nil || p.Patch != "" || len(p.Lines) != 0 || p.Search != "" || p.Replacement != "" || p.ReplaceAll {
			return failure(proto.CodeInvalidRequest)
		}
		if len(*p.Content) > MaxBytes {
			return failure(proto.CodeLimitExceeded)
		}
	case "patch":
		if p.Content != nil || p.Patch == "" || len(p.Lines) != 0 || p.Search != "" || p.Replacement != "" || p.ReplaceAll {
			return failure(proto.CodeInvalidRequest)
		}
		if len(p.Patch) > MaxBytes {
			return failure(proto.CodeLimitExceeded)
		}
	case "lines":
		if p.Content != nil || p.Patch != "" || len(p.Lines) == 0 || p.Search != "" || p.Replacement != "" || p.ReplaceAll {
			return failure(proto.CodeInvalidRequest)
		}
		if len(p.Lines) > MaxEdits {
			return failure(proto.CodeLimitExceeded)
		}
		total := 0
		for _, e := range p.Lines {
			if e.StartLine < 1 || e.EndLine < 0 || e.EndLine != 0 && e.EndLine < e.StartLine {
				return failure(proto.CodeInvalidRequest)
			}
			total += len(e.Replacement)
			if e.Expected != nil {
				total += len(*e.Expected)
			}
			if total > MaxBytes {
				return failure(proto.CodeLimitExceeded)
			}
		}
	case "search":
		if p.Content != nil || p.Patch != "" || len(p.Lines) != 0 || p.Search == "" || !text([]byte(p.Search)) || !text([]byte(p.Replacement)) {
			return failure(proto.CodeInvalidRequest)
		}
		if len(p.Search)+len(p.Replacement) > MaxBytes {
			return failure(proto.CodeLimitExceeded)
		}
	default:
		return failure(proto.CodeInvalidRequest)
	}
	return nil
}

// Line boundaries include each line terminator. A trailing newline does not
// create an extra empty line; an empty file has zero lines and one EOF boundary.
func boundaries(b []byte) []int {
	offsets := []int{0}
	for i, v := range b {
		if v == '\n' {
			offsets = append(offsets, i+1)
		}
	}
	if offsets[len(offsets)-1] != len(b) {
		offsets = append(offsets, len(b))
	}
	return offsets
}

type splice struct {
	start, end  int
	replacement string
}

func Apply(base []byte, p *proto.EditParams) ([]byte, error) {
	if err := Validate(p); err != nil {
		return nil, err
	}
	if len(base) > MaxBytes {
		return nil, failure(proto.CodeLimitExceeded)
	}
	if Digest(base) != p.BaseDigest {
		return nil, failure(proto.CodeEditConflict)
	}
	if !text(base) {
		return nil, failure(proto.CodeEditText)
	}
	if p.Kind == "replace" {
		if !text([]byte(*p.Content)) {
			return nil, failure(proto.CodeEditText)
		}
		return []byte(*p.Content), nil
	}
	if p.Kind == "search" {
		count := bytes.Count(base, []byte(p.Search))
		if count == 0 {
			return nil, failure(proto.CodeEditMismatch)
		}
		n := 1
		if p.ReplaceAll {
			n = count
		}
		out := bytes.Replace(base, []byte(p.Search), []byte(p.Replacement), n)
		if len(out) > MaxBytes {
			return nil, failure(proto.CodeLimitExceeded)
		}
		return out, nil
	}
	offsets := boundaries(base)
	var edits []splice
	if p.Kind == "patch" {
		var err error
		edits, err = parsePatch(base, offsets, p.Patch)
		if err != nil {
			return nil, err
		}
	} else {
		for _, e := range p.Lines {
			if e.StartLine > len(offsets) || e.EndLine >= len(offsets) {
				return nil, failure(proto.CodeEditMismatch)
			}
			start := offsets[e.StartLine-1]
			end := start
			if e.EndLine != 0 {
				end = offsets[e.EndLine]
			}
			if e.Expected != nil && *e.Expected != string(base[start:end]) {
				return nil, failure(proto.CodeEditMismatch)
			}
			edits = append(edits, splice{start, end, e.Replacement})
		}
	}
	sort.Slice(edits, func(i, j int) bool {
		if edits[i].start == edits[j].start {
			return edits[i].end < edits[j].end
		}
		return edits[i].start < edits[j].start
	})
	size := len(base)
	for i, e := range edits {
		if !text([]byte(e.replacement)) {
			return nil, failure(proto.CodeEditText)
		}
		if i > 0 && (e.start < edits[i-1].end || e.start == edits[i-1].start) {
			return nil, failure(proto.CodeEditOverlap)
		}
		size += len(e.replacement) - (e.end - e.start)
		if size > MaxBytes {
			return nil, failure(proto.CodeLimitExceeded)
		}
	}
	out := make([]byte, 0, size)
	pos := 0
	for _, e := range edits {
		out = append(out, base[pos:e.start]...)
		out = append(out, e.replacement...)
		pos = e.end
	}
	return append(out, base[pos:]...), nil
}
