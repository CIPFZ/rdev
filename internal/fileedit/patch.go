package fileedit

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/CIPFZ/rdev/internal/proto"
)

var header = regexp.MustCompile(`^@@ -([0-9]+)(?:,([0-9]+))? \+([0-9]+)(?:,([0-9]+))? @@(?: .*)?$`)

// Accept a single-file unified diff or bare @@ hunks. File header names are
// descriptive only: the request's path is the sole destination. Renames,
// binary patches, extra files and malformed counts are rejected.
func parsePatch(base []byte, offsets []int, patch string) ([]splice, error) {
	if !text([]byte(patch)) {
		return nil, failure(proto.CodeEditText)
	}
	if !strings.HasSuffix(patch, "\n") {
		return nil, failure(proto.CodeInvalidRequest)
	}
	lines := strings.SplitAfter(patch, "\n")
	lines = lines[:len(lines)-1]
	i := 0
	if strings.HasPrefix(lines[0], "--- ") {
		if len(lines) < 3 || !strings.HasPrefix(lines[1], "+++ ") {
			return nil, failure(proto.CodeInvalidRequest)
		}
		i = 2
	}
	var edits []splice
	delta := 0
	lastEnd := 0
	for i < len(lines) {
		m := header.FindStringSubmatch(strings.TrimSuffix(lines[i], "\n"))
		if m == nil {
			return nil, failure(proto.CodeInvalidRequest)
		}
		nums := [4]int{}
		for j := range nums {
			v := m[j+1]
			if v == "" {
				nums[j] = 1
				continue
			}
			n, e := strconv.Atoi(v)
			if e != nil || n > MaxBytes {
				return nil, failure(proto.CodeInvalidRequest)
			}
			nums[j] = n
		}
		oldStart, oldCount, newStart, newCount := nums[0], nums[1], nums[2], nums[3]
		oldIndex, newIndex := oldStart-1, newStart-1
		if oldCount == 0 {
			oldIndex = oldStart
		}
		if newCount == 0 {
			newIndex = newStart
		}
		if oldIndex < 0 || newIndex < 0 || newIndex != oldIndex+delta || oldIndex < lastEnd || oldIndex >= len(offsets) || oldCount > len(offsets)-1-oldIndex {
			return nil, failure(proto.CodeEditMismatch)
		}
		i++
		var oldText, newText strings.Builder
		oldN, newN := 0, 0
		for i < len(lines) && !strings.HasPrefix(lines[i], "@@ ") {
			line := lines[i]
			i++
			if len(line) < 2 {
				return nil, failure(proto.CodeInvalidRequest)
			}
			prefix := line[0]
			if prefix != ' ' && prefix != '-' && prefix != '+' {
				return nil, failure(proto.CodeInvalidRequest)
			}
			body := line[1:]
			if i < len(lines) && lines[i] == "\\ No newline at end of file\n" {
				body = strings.TrimSuffix(body, "\n")
				i++
			}
			if prefix != '+' {
				oldText.WriteString(body)
				oldN++
			}
			if prefix != '-' {
				newText.WriteString(body)
				newN++
			}
		}
		if oldN != oldCount || newN != newCount || oldN+newN == 0 {
			return nil, failure(proto.CodeInvalidRequest)
		}
		start, end := offsets[oldIndex], offsets[oldIndex+oldCount]
		if oldText.String() != string(base[start:end]) {
			return nil, failure(proto.CodeEditMismatch)
		}
		edits = append(edits, splice{start, end, newText.String()})
		if len(edits) > MaxEdits {
			return nil, failure(proto.CodeLimitExceeded)
		}
		delta += newCount - oldCount
		lastEnd = oldIndex + oldCount
	}
	if len(edits) == 0 {
		return nil, failure(proto.CodeInvalidRequest)
	}
	return edits, nil
}
