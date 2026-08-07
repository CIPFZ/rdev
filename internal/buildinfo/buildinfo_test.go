package buildinfo

import (
	"testing"
	"time"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v.UTC()
}

// A stamp has to survive the round trip through ssh output, since that is the only
// way a host learns what the installed agent is.
func TestStampRoundTrips(t *testing.T) {
	got := ParseVersionOutput("rdev-agent proto=1-2 linux/amd64\nrdev-build 0.1.0 60503d1 2026-08-07T10:22:31Z\n")
	if !got.Known() {
		t.Fatal("parsed stamp should be usable")
	}
	if got.Version != "0.1.0" || got.Commit != "60503d1" {
		t.Errorf("got %+v, want version 0.1.0 commit 60503d1", got)
	}
	if want := mustTime(t, "2026-08-07T10:22:31Z"); !got.Time.Equal(want) {
		t.Errorf("Time = %v, want %v", got.Time, want)
	}
	if got.Dirty {
		t.Error("a clean commit should not parse as dirty")
	}
}

// The host parses output from a binary that may be older than this code, or may not
// be an rdev agent at all. Nothing here may be reported as comparable.
func TestParseStampRejectsUnusableInput(t *testing.T) {
	for _, in := range []string{
		"",
		"   ",
		"rdev-build 0.1.0",                    // marker but no commit
		"rdev-agent proto=1-2 linux/amd64",    // the other -version line
		"bash: rdev-agent: command not found", // a shell error, not a stamp
		"0.1.0 60503d1 2026-08-07T10:22:31Z",  // stamp fields with no marker
		"rdev-buildX 0.1.0 60503d1",           // prefix must be a whole field
		"Welcome to prod! Last login: Tue",    // a chatty login profile
	} {
		if got := ParseVersionOutput(in); got.Known() {
			t.Errorf("ParseStamp(%q) = %+v, want an unusable build", in, got)
		}
	}
}

// A stamp from a build that predates CommitTime still identifies itself, it just
// cannot be ordered.
func TestParseStampWithoutTimeIsKnownButUnorderable(t *testing.T) {
	got := ParseVersionOutput("rdev-build 0.1.0 abc1234")
	if !got.Known() {
		t.Fatal("a stamp with a commit is still identifiable")
	}
	if !got.Time.IsZero() {
		t.Errorf("Time = %v, want zero", got.Time)
	}
	newer := Build{Commit: "def5678", Time: mustTime(t, "2030-01-01T00:00:00Z")}
	if newer.NewerThan(got) {
		t.Error("nothing is newer than a build with no timestamp: the answer is unknown")
	}
	if got.NewerThan(newer) {
		t.Error("a build with no timestamp is not newer than anything")
	}
}

func TestNewerThan(t *testing.T) {
	older := Build{Commit: "aaa", Time: mustTime(t, "2026-08-01T00:00:00Z")}
	newer := Build{Commit: "bbb", Time: mustTime(t, "2026-08-07T00:00:00Z")}

	if !newer.NewerThan(older) {
		t.Error("later commit date should be newer")
	}
	if older.NewerThan(newer) {
		t.Error("earlier commit date should not be newer")
	}
	// Equal is not newer: an equal-time comparison must not authorize an action
	// that only a strictly newer build should get.
	same := Build{Commit: "ccc", Time: older.Time}
	if same.NewerThan(older) || older.NewerThan(same) {
		t.Error("equal times are not strictly newer in either direction")
	}
}

// A dirty build carries its parent commit's timestamp, so that timestamp says
// nothing about its contents and must never win a comparison.
func TestDirtyBuildsAreNeverOrdered(t *testing.T) {
	clean := Build{Commit: "aaa", Time: mustTime(t, "2026-08-01T00:00:00Z")}
	dirty := Build{Commit: "bbb-dirty", Dirty: true, Time: mustTime(t, "2026-08-07T00:00:00Z")}

	if dirty.NewerThan(clean) {
		t.Error("a dirty build must not be treated as newer, however late its parent commit")
	}
	if clean.NewerThan(dirty) {
		t.Error("a dirty build must not be treated as older either: it is unorderable")
	}
}

func TestParseStampDetectsDirty(t *testing.T) {
	got := ParseVersionOutput("rdev-build 0.1.0 60503d1-dirty 2026-08-07T10:22:31Z")
	if !got.Dirty {
		t.Error("a -dirty commit should parse as dirty")
	}
}

// StampLine and ParseVersionOutput are two halves of one wire contract: the agent prints, the
// host parses. Changing the field order in one without the other would leave a host
// silently unable to compare builds, so the pairing is asserted rather than assumed.
func TestStampIsParseableByItsOwnParser(t *testing.T) {
	origCommit, origTime := Commit, CommitTime
	t.Cleanup(func() { Commit, CommitTime = origCommit, origTime })

	Commit, CommitTime = "60503d1", "2026-08-07T10:22:31Z"
	got := ParseVersionOutput(StampLine())

	if !got.Known() {
		t.Fatalf("StampLine() = %q, which its own parser cannot read", StampLine())
	}
	want := Current()
	if got.Commit != want.Commit || !got.Time.Equal(want.Time) || got.Version != want.Version {
		t.Errorf("round trip lost information: parsed %+v, want %+v", got, want)
	}
}

// An unstamped build -- `go build` with no -ldflags -- must still run and must
// report itself as not comparable rather than pretending to a version.
func TestUnstampedBuildIsNotKnown(t *testing.T) {
	b := Build{Version: "0.1.0", Commit: "unknown"}
	if b.Known() {
		t.Error("an unstamped build must not claim a comparable identity")
	}
	if b.String() != "unknown build" {
		t.Errorf("String() = %q, want it to say the build is unknown", b.String())
	}
}
