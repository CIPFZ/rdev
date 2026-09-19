package fileedit

import (
	"github.com/CIPFZ/rdev/internal/proto"
	"testing"
)

func ptr(s string) *string { return &s }
func TestLinesReplaceInsertDelete(t *testing.T) {
	base := []byte("one\ntwo\nthree\n")
	p := &proto.EditParams{Path: "x", Kind: "lines", BaseDigest: Digest(base), Lines: []proto.LineEdit{{StartLine: 2, EndLine: 2, Expected: ptr("two\n"), Replacement: "TWO\n"}, {StartLine: 4, Replacement: "four\n"}}}
	got, e := Apply(base, p)
	if e != nil || string(got) != "one\nTWO\nthree\nfour\n" {
		t.Fatalf("got %q err %v", got, e)
	}
}
func TestLineRangesUseOriginalSnapshot(t *testing.T) {
	base := []byte("a\nb\nc\n")
	p := &proto.EditParams{Path: "x", Kind: "lines", BaseDigest: Digest(base), Lines: []proto.LineEdit{{StartLine: 1, EndLine: 1, Replacement: "A\n"}, {StartLine: 3, EndLine: 3, Replacement: "C\n"}}}
	got, e := Apply(base, p)
	if e != nil || string(got) != "A\nb\nC\n" {
		t.Fatalf("%q %v", got, e)
	}
}
func TestDigestAndMismatch(t *testing.T) {
	base := []byte("a\n")
	p := &proto.EditParams{Path: "x", Kind: "lines", BaseDigest: Digest([]byte("old\n")), Lines: []proto.LineEdit{{StartLine: 1, EndLine: 1, Replacement: "x\n"}}}
	if _, e := Apply(base, p); e == nil || e.(*proto.ErrorEnvelope).Code != proto.CodeEditConflict {
		t.Fatalf("%v", e)
	}
}
func TestPatchStrictAndReplace(t *testing.T) {
	base := []byte("a\nb\n")
	p := &proto.EditParams{Path: "x", Kind: "patch", BaseDigest: Digest(base), Patch: "@@ -1,2 +1,2 @@\n-a\n+b\n b\n"}
	got, e := Apply(base, p)
	if e != nil || string(got) != "b\nb\n" {
		t.Fatalf("%q %v", got, e)
	}
	c := ""
	p = &proto.EditParams{Path: "x", Kind: "replace", BaseDigest: Digest(got), Content: &c}
	got, e = Apply(got, p)
	if e != nil || len(got) != 0 {
		t.Fatalf("%q %v", got, e)
	}
}

func TestUnifiedDiffIsBoundedAndLiteral(t *testing.T) {
	diff := UnifiedDiff([]byte("a\nb\nc\n"), []byte("a\nB\nc\n"))
	want := "@@ -1,3 +1,3 @@\n a\n-b\n+B\n c\n"
	if diff != want {
		t.Fatalf("diff = %q, want %q", diff, want)
	}
}
func TestOverlapRejected(t *testing.T) {
	base := []byte("a\nb\n")
	p := &proto.EditParams{Path: "x", Kind: "lines", BaseDigest: Digest(base), Lines: []proto.LineEdit{{StartLine: 1, EndLine: 2, Replacement: "x"}, {StartLine: 2, EndLine: 2, Replacement: "y"}}}
	if _, e := Apply(base, p); e == nil || e.(*proto.ErrorEnvelope).Code != proto.CodeEditOverlap {
		t.Fatalf("%v", e)
	}
}

func TestSearchEditIsExactAndDigestBound(t *testing.T) {
	base := []byte("one two two\n")
	p := &proto.EditParams{Path: "x", Kind: "search", BaseDigest: Digest(base), Search: "two", Replacement: "TWO"}
	got, err := Apply(base, p)
	if err != nil || string(got) != "one TWO two\n" {
		t.Fatalf("search result=%q err=%v", got, err)
	}
	p.ReplaceAll = true
	got, err = Apply(base, p)
	if err != nil || string(got) != "one TWO TWO\n" {
		t.Fatalf("replace-all result=%q err=%v", got, err)
	}
}
