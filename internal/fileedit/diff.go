package fileedit

import (
	"fmt"
	"strings"
)

const MaxDiffBytes = 128 << 10

// UnifiedDiff returns a bounded, single-file diff suitable for an agent preview.
// It deliberately uses the same literal line boundaries as Apply; no newline
// is added or normalized while rendering the preview.
func UnifiedDiff(before, after []byte) string {
	oldLines, newLines := diffLines(string(before)), diffLines(string(after))
	prefix := 0
	for prefix < len(oldLines) && prefix < len(newLines) && oldLines[prefix] == newLines[prefix] {
		prefix++
	}
	suffix := 0
	for suffix < len(oldLines)-prefix && suffix < len(newLines)-prefix && oldLines[len(oldLines)-1-suffix] == newLines[len(newLines)-1-suffix] {
		suffix++
	}
	oldStart := max(0, prefix-3)
	newStart := max(0, prefix-3)
	oldEnd := min(len(oldLines), len(oldLines)-suffix+3)
	newEnd := min(len(newLines), len(newLines)-suffix+3)
	var b strings.Builder
	fmt.Fprintf(&b, "@@ -%d,%d +%d,%d @@\n", oldStart+1, oldEnd-oldStart, newStart+1, newEnd-newStart)
	for i, j := oldStart, newStart; i < oldEnd || j < newEnd; {
		if i < prefix && j < prefix {
			writeDiffLine(&b, ' ', oldLines[i])
			i++
			j++
			continue
		}
		oldChangedEnd := len(oldLines) - suffix
		newChangedEnd := len(newLines) - suffix
		if i < oldChangedEnd {
			writeDiffLine(&b, '-', oldLines[i])
			i++
		}
		if j < newChangedEnd {
			writeDiffLine(&b, '+', newLines[j])
			j++
		}
		if i >= oldChangedEnd && j >= newChangedEnd {
			for i < oldEnd && j < newEnd {
				writeDiffLine(&b, ' ', oldLines[i])
				i++
				j++
			}
			for i < oldEnd {
				writeDiffLine(&b, ' ', oldLines[i])
				i++
			}
			for j < newEnd {
				writeDiffLine(&b, ' ', newLines[j])
				j++
			}
		}
		if b.Len() > MaxDiffBytes {
			return b.String()[:MaxDiffBytes] + "\n... diff truncated ...\n"
		}
	}
	return b.String()
}

func diffLines(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.SplitAfter(s, "\n")
	if parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func writeDiffLine(b *strings.Builder, prefix byte, line string) {
	b.WriteByte(prefix)
	b.WriteString(line)
	if !strings.HasSuffix(line, "\n") {
		b.WriteByte('\n')
	}
}
