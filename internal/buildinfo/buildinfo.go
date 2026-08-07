// Package buildinfo carries the identity of a build so a running binary can say
// what it is.
//
// This exists because "rdev 0.1.0" was not enough to answer the question that
// actually comes up: which agent is inside this rdev, and is the one on the remote
// host older or newer than it? A hash answers "same or different" but not "which
// way", and a version constant bumped by hand answers neither.
//
// Values are injected at link time with -ldflags -X (see the Makefile). A build
// without them still works and reports "unknown", so `go build ./cmd/rdev` is not
// broken -- just less informative.
package buildinfo

import (
	"fmt"
	"strings"
	"time"
)

// Version is the release version, kept in sync with what MCP clients are told.
var Version = "0.1.0"

// Commit is the git revision, from `git describe --always --dirty`.
var Commit = "unknown"

// CommitTime is the committer date of Commit, as RFC3339 in UTC.
//
// Deliberately the commit's time, not the build's. A build timestamp would change
// the binary's bytes on every rebuild, which would defeat rdev's content-hash
// check for whether the remote agent needs replacing and turn every reconnect
// into a 9 MB upload. The commit date orders two builds just as well for the
// purpose it serves here, and keeps builds reproducible.
//
// A dirty tree is the case this cannot order: uncommitted changes carry their
// parent commit's date. Hence the -dirty marker in Commit, which callers comparing
// builds must treat as "unorderable" rather than "equal".
var CommitTime = ""

// StampPrefix marks the line of `rdev-agent -version` output carrying the stamp.
//
// Part of the wire contract, not a display detail: an rdev host runs an installed
// agent's -version over ssh and reads this line to decide whether overwriting it
// would be a downgrade. Prefixed rather than positional because a login profile
// that prints to stdout would otherwise shift the fields -- the same reason the
// connect probe prefixes every value it reports.
const StampPrefix = "rdev-build"

// Stamp is a one-line build identity, e.g.
//
//	0.1.0 60503d1 2026-08-07T10:22:31Z
//
// Read back by ParseVersionOutput on the other side of an ssh connection, so the
// field order is part of the contract between a host and an installed agent.
func Stamp() string {
	parts := []string{Version, Commit}
	if CommitTime != "" {
		parts = append(parts, CommitTime)
	}
	return strings.Join(parts, " ")
}

// StampLine is the full line an agent prints, prefix included.
func StampLine() string { return StampPrefix + " " + Stamp() }

// Dirty reports whether the build came from a modified working tree, and so
// cannot be meaningfully ordered against another build.
func Dirty() bool { return strings.HasSuffix(Commit, "-dirty") }

// Build is a parsed build identity.
type Build struct {
	Version string
	Commit  string
	// Time is the commit date, zero when the build carried none.
	Time time.Time
	// Dirty marks a build from a modified tree. Its Time is inherited from the
	// parent commit and therefore says nothing about the actual contents.
	Dirty bool
}

// Known reports whether this identity carries enough to be compared.
func (b Build) Known() bool { return b.Commit != "" && b.Commit != "unknown" }

// NewerThan reports whether b is strictly newer than other.
//
// False whenever the answer is not knowable: either side missing a timestamp,
// either side dirty, or equal times. A caller uses this to refuse an action, so an
// unknown ordering must not read as "yes".
//
// Note that this is a comparison of commit dates, not ancestry. Two people on
// different branches of a shared machine are not older and newer than each other
// but divergent, and a date comparison will still pick a winner. Callers should
// phrase what they say to the user accordingly.
func (b Build) NewerThan(other Build) bool {
	if b.Time.IsZero() || other.Time.IsZero() {
		return false
	}
	if b.Dirty || other.Dirty {
		return false
	}
	return b.Time.After(other.Time)
}

// Current returns this binary's own identity.
func Current() Build {
	b := Build{Version: Version, Commit: Commit, Dirty: Dirty()}
	if CommitTime != "" {
		if t, err := time.Parse(time.RFC3339, CommitTime); err == nil {
			b.Time = t.UTC()
		}
	}
	return b
}

// ParseVersionOutput extracts a build identity from `rdev-agent -version` output.
//
// Deliberately strict about the prefix, and tolerant about everything else. The
// input is whatever a remote binary wrote to stdout -- it may be an agent too old
// to print a stamp, a shell error like "rdev-agent: command not found", or a chatty
// ~/.bashrc's greeting. Requiring the marker line is what separates a real stamp
// from text that merely happens to have two words in it; without that check,
// "bash: rdev-agent: not found" parses as version "bash:" commit "rdev-agent:".
//
// Anything unrecognized yields a Build that is not Known, which callers must treat
// as "cannot compare" rather than as an error or as a match.
func ParseVersionOutput(out string) Build {
	for _, line := range strings.Split(out, "\n") {
		rest, ok := cutPrefix(strings.TrimSpace(line), StampPrefix)
		if !ok {
			continue
		}
		return parseStampFields(rest)
	}
	return Build{}
}

// cutPrefix reports the remainder after prefix and a following space.
func cutPrefix(line, prefix string) (string, bool) {
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}
	rest := strings.TrimPrefix(line, prefix)
	if rest == "" || !strings.HasPrefix(rest, " ") {
		return "", false
	}
	return strings.TrimSpace(rest), true
}

// parseStampFields reads the fields Stamp writes.
func parseStampFields(s string) Build {
	fields := strings.Fields(s)
	if len(fields) < 2 {
		return Build{}
	}
	b := Build{Version: fields[0], Commit: fields[1]}
	b.Dirty = strings.HasSuffix(b.Commit, "-dirty")
	if len(fields) >= 3 {
		if t, err := time.Parse(time.RFC3339, fields[2]); err == nil {
			b.Time = t.UTC()
		}
	}
	return b
}

// String renders a build for a human, including whether it is comparable.
func (b Build) String() string {
	if !b.Known() {
		return "unknown build"
	}
	s := fmt.Sprintf("%s %s", b.Version, b.Commit)
	if !b.Time.IsZero() {
		s += " " + b.Time.Format(time.RFC3339)
	}
	return s
}
