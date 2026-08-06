// Reading job output.
//
// Everything here is written to be independent of log size. A batch run can emit
// hundreds of megabytes, and these calls are used to peek at it repeatedly, so
// reading a whole file to return twenty lines would be both slow and a way to
// exhaust memory on a shared machine.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/tonynyyan/rdev/internal/proto"
)

const defaultLogTail = 200

// maxLogLineLen bounds a single log line. A process emitting one enormous line
// (a minified bundle, a base64 blob) should not be able to make the agent
// allocate without limit.
const maxLogLineLen = 1 << 20

func jobLogs(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.ID == "" {
		return nil, errors.New("job id required")
	}
	stream := p.Stream
	if stream == "" {
		stream = "stdout"
	}
	if stream != "stdout" && stream != "stderr" {
		return nil, fmt.Errorf("stream must be stdout or stderr, got %q", stream)
	}

	path := filepath.Join(jobDir(state, p.ID), stream)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// Clamp the offset to the file: a caller polling incrementally can hold a
	// next_offset from before the log was rotated or truncated, and the
	// resulting negative length would panic in make. Treat a stale offset as
	// "nothing new to read" rather than an error, since the caller's next poll
	// with the returned offset then recovers on its own.
	since := p.SinceOffset
	if since < 0 {
		since = 0
	}
	if since > info.Size() {
		since = info.Size()
	}
	if since > 0 {
		if _, err := f.Seek(since, 0); err != nil {
			return nil, err
		}
	}

	res := &proto.JobResult{LogSize: info.Size()}

	tail := p.TailLines
	if tail <= 0 {
		tail = defaultLogTail
	}

	// Fast path: plain "tail the last N lines". Seek backward from the end instead
	// of walking the file, so cost depends on the output size rather than the log
	// size. This is the common shape -- checking on a running batch -- and on a
	// 50 MB log it is ~40x faster than scanning.
	if p.Grep == "" && since == 0 {
		logs, err := readTail(path, tail)
		if err != nil {
			return nil, err
		}
		res.Logs = logs
		res.NextOffset = info.Size()
		return res, nil
	}

	// Otherwise stream the region, keeping only the lines that will be returned.
	//
	// Reading it whole would allocate the entire span: measured at 412 MB to
	// return 1900 bytes from a 190 MB log, which is enough to OOM a shared dev box
	// during a long batch. Grep and tail both reduce, so neither needs the full
	// text in memory at once.
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64<<10), maxLogLineLen)

	ring := newLineRing(tail)
	grep := []byte(p.Grep)
	var consumed int64
	matched := 0
	for scanner.Scan() {
		// Bytes() reuses the scanner's buffer, so a filtered-out line costs no
		// allocation at all. Converting every line to a string instead cost ~200 MB
		// of garbage on a 190 MB log.
		line := scanner.Bytes()
		// +1 for the newline the scanner stripped. The last line may not have one,
		// so the offset is clamped to the file size below.
		consumed += int64(len(line)) + 1
		if len(grep) > 0 {
			if !bytes.Contains(line, grep) {
				continue
			}
			matched++
		}
		ring.add(line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s log: %w", stream, err)
	}

	// A line longer than the scanner buffer, or a final line without a newline,
	// can leave the count slightly over; never report an offset past the end.
	next := since + consumed
	if next > info.Size() {
		next = info.Size()
	}
	res.NextOffset = next
	if p.Grep != "" {
		res.Matched = matched
	}
	res.Logs = strings.Join(ring.lines(), "\n")
	return res, nil
}

// readTail returns the last n lines of a file.
//
// Reads backward in chunks from the end rather than loading the whole file:
// tail_on_exit is commonly used on batch logs that can reach hundreds of
// megabytes, and os.ReadFile on one of those would allocate the entire thing to
// return a handful of lines.
func readTail(path string, n int) (string, error) {
	if n < 1 {
		n = 1
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}

	size := info.Size()
	// Cap the scan: a file whose last n lines are enormous should still not pull
	// an unbounded amount into memory.
	const chunk = 64 << 10
	maxScan := int64(chunk) * 16

	var tail []byte
	var pos = size
	for pos > 0 && int64(len(tail)) < maxScan {
		step := int64(chunk)
		if pos < step {
			step = pos
		}
		pos -= step

		buf := make([]byte, step)
		if _, err := f.ReadAt(buf, pos); err != nil && err != io.EOF {
			return "", err
		}
		tail = append(buf, tail...)

		// Stop once the window holds enough newlines for n lines. One extra
		// accounts for a partial line at the front of the window.
		if bytes.Count(tail, []byte("\n")) > n {
			break
		}
	}

	lines := strings.Split(strings.TrimRight(string(tail), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n"), nil
}

// lineRing keeps the last n lines seen, discarding earlier ones.
//
// This is what makes tailing independent of log size: memory is bounded by the
// number of lines actually returned rather than by the file.
type lineRing struct {
	buf   []string
	next  int
	full  bool
	limit int
}

func newLineRing(limit int) *lineRing {
	if limit < 1 {
		limit = 1
	}
	return &lineRing{buf: make([]string, 0, limit), limit: limit}
}

// add copies line into the ring. The copy is required: callers pass the
// scanner's reusable buffer, which the next Scan overwrites.
func (r *lineRing) add(line []byte) {
	s := string(line)
	if len(r.buf) < r.limit {
		r.buf = append(r.buf, s)
		return
	}
	r.buf[r.next] = s
	r.next = (r.next + 1) % r.limit
	r.full = true
}

// lines returns the retained lines in arrival order.
func (r *lineRing) lines() []string {
	if !r.full {
		return r.buf
	}
	out := make([]string, 0, len(r.buf))
	out = append(out, r.buf[r.next:]...)
	return append(out, r.buf[:r.next]...)
}
