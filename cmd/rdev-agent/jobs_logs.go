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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/CIPFZ/rdev/internal/proto"
)

const defaultLogTail = 200
const hardLogTailLines = 1000

// maxLogLineLen bounds a single log line. A process emitting one enormous line
// (a minified bundle, a base64 blob) should not be able to make the agent
// allocate without limit.
const maxLogLineLen = 1 << 20

func jobLogs(p *proto.JobParams, state string) (*proto.JobResult, error) {
	dir, err := validatedJobDir(state, p.ID)
	if err != nil {
		return nil, err
	}
	stream := p.Stream
	if stream == "" {
		stream = "stdout"
	}
	if stream != "stdout" && stream != "stderr" {
		return nil, invalidRequestError("stream must be stdout or stderr")
	}

	path := filepath.Join(dir, stream)
	if err := secureRecordFile(path); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// Clamp a positive stale offset to the file after rejecting a negative wire
	// value. A rotated/truncated log is a legitimate race; a negative offset is
	// an invalid request and must not silently change meaning.
	since := p.SinceOffset
	if since < 0 {
		return nil, invalidRequestError("since_offset must not be negative")
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
	var st struct {
		ExitCode     *int            `json:"exit_code"`
		StdoutLedger proto.LogLedger `json:"stdout_ledger"`
		StderrLedger proto.LogLedger `json:"stderr_ledger"`
	}
	_ = readJSON(filepath.Join(dir, "status.json"), &st)
	if st.StdoutLedger.LimitBytes == 0 && st.StderrLedger.LimitBytes == 0 {
		_ = readJSON(filepath.Join(dir, "ledger.json"), &st)
	}
	if stream == "stdout" {
		res.LogLedger = st.StdoutLedger
	} else {
		res.LogLedger = st.StderrLedger
	}
	// The supervisor flushes the bounded sink and then publishes status.json
	// while holding the job lock. If this call observed a terminal status while
	// the finalization write was still in flight, the descriptor opened above may
	// refer to the pre-flush inode. Reacquire the same lock and reopen the stream
	// so exited jobs never report an empty/stale log merely due to an atomic
	// rename race. Running jobs keep the non-blocking tail behavior.
	if st.ExitCode != nil {
		if err := withJobLock(dir, func() error {
			if err := f.Close(); err != nil {
				return err
			}
			f, err = os.Open(path)
			if err != nil {
				return err
			}
			info, err = f.Stat()
			return err
		}); err != nil {
			return nil, err
		}
		res.LogSize = info.Size()
	}

	tail := p.TailLines
	if tail < 0 || tail > hardLogTailLines {
		return nil, limitExceededError("tail_lines is outside the hard limit")
	}
	if tail == 0 {
		tail = defaultLogTail
	}
	if int64(len(p.Grep)) > proto.AbsoluteLineBytes {
		return nil, limitExceededError("grep is outside the hard line limit")
	}

	// Fast path: plain "tail the last N lines". Seek backward from the end instead
	// of walking the file, so cost depends on the output size rather than the log
	// size. This is the common shape -- checking on a running batch -- and on a
	// 50 MB log it is ~40x faster than scanning.
	if p.Grep == "" && since == 0 {
		logs, tailTruncated, tailScanBytes, err := readTailStatus(path, tail)
		if err != nil {
			return nil, err
		}
		res.Logs = logs
		res.TailTruncated = tailTruncated
		res.TailScanBytes = tailScanBytes
		res.LogsTruncation, _ = proto.NewTruncation(info.Size(), int64(len(logs)))
		if res.LogLedger.LimitBytes != 0 {
			res.LogsTruncation, _ = proto.NewTruncation(res.LogLedger.OriginalBytes, res.LogLedger.RetainedBytes)
		}
		res.NextOffset = info.Size()
		return res, nil
	}

	// Otherwise stream a fixed snapshot of the region, keeping only the lines
	// that will be returned. Limiting to the size observed above makes the byte
	// accounting exact even if the running job appends while this call scans.
	//
	// Reading it whole would allocate the entire span: measured at 412 MB to
	// return 1900 bytes from a 190 MB log, which is enough to OOM a shared dev box
	// during a long batch. Grep and tail both reduce, so neither needs the full
	// text in memory at once.
	regionBytes := info.Size() - since
	scanner := bufio.NewScanner(io.LimitReader(f, regionBytes))
	scanner.Buffer(make([]byte, 0, 64<<10), maxLogLineLen)

	ring := newLineRing(tail)
	grep := []byte(p.Grep)
	matched := 0
	for scanner.Scan() {
		// Bytes() reuses the scanner's buffer, so a filtered-out line costs no
		// allocation at all. Converting every line to a string instead cost ~200 MB
		// of garbage on a 190 MB log.
		line := scanner.Bytes()
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

	res.NextOffset = info.Size()
	if p.Grep != "" {
		res.Matched = matched
	}
	res.Logs = strings.Join(ring.lines(), "\n")
	res.LogsTruncation, _ = proto.NewTruncation(regionBytes, int64(len(res.Logs)))
	if res.LogLedger.LimitBytes != 0 {
		res.LogsTruncation, _ = proto.NewTruncation(res.LogLedger.OriginalBytes, res.LogLedger.RetainedBytes)
	}
	return res, nil
}

// readTail returns the last n lines of a file.
//
// Reads backward in chunks from the end rather than loading the whole file:
// tail_on_exit is commonly used on batch logs that can reach hundreds of
// megabytes, and os.ReadFile on one of those would allocate the entire thing to
// return a handful of lines.
func readTail(path string, n int) (string, error) {
	logs, _, _, err := readTailStatus(path, n)
	return logs, err
}

// readTailStatus reports when the bounded backward scan could not reach the
// requested window (for example, a single gigantic line). This is observable
// so callers can choose a larger strategy instead of assuming completeness.
func readTailStatus(path string, n int) (string, bool, int64, error) {
	if n < 1 {
		n = 1
	}
	f, err := os.Open(path)
	if err != nil {
		return "", false, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", false, 0, err
	}

	size := info.Size()
	// Cap the scan: a file whose last n lines are enormous should still not pull
	// an unbounded amount into memory.
	const chunk = 64 << 10
	maxScan := proto.AbsoluteOutputBytes

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
			return "", false, 0, err
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
	return strings.Join(lines, "\n"), pos > 0 && bytes.Count(tail, []byte("\n")) <= n, size - pos, nil
}

// lineRing keeps the last n lines seen, discarding earlier ones.
//
// This is what makes tailing independent of log size: memory is bounded by the
// number of lines actually returned rather than by the file.
type lineRing struct {
	buf      []string
	head     int
	count    int
	bytes    int
	limit    int
	maxBytes int
}

func newLineRing(limit int) *lineRing {
	if limit < 1 {
		limit = 1
	}
	if limit > hardLogTailLines {
		limit = hardLogTailLines
	}
	return &lineRing{buf: make([]string, limit), limit: limit, maxBytes: int(proto.AbsoluteOutputBytes)}
}

// add copies line into the ring. The copy is required: callers pass the
// scanner's reusable buffer, which the next Scan overwrites.
func (r *lineRing) add(line []byte) {
	if len(line) > r.maxBytes {
		line = line[len(line)-r.maxBytes:]
	}
	s := string(line)
	for r.count > 0 && (r.count == r.limit || r.bytes+len(s) > r.maxBytes) {
		r.bytes -= len(r.buf[r.head])
		r.buf[r.head] = ""
		r.head = (r.head + 1) % r.limit
		r.count--
	}
	index := (r.head + r.count) % r.limit
	r.buf[index] = s
	r.count++
	r.bytes += len(s)
}

// lines returns the retained lines in arrival order.
func (r *lineRing) lines() []string {
	out := make([]string, 0, r.count)
	for i := 0; i < r.count; i++ {
		out = append(out, r.buf[(r.head+i)%r.limit])
	}
	return out
}
