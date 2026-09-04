package main

// Incremental, owner-safe reclamation for managed detached-job records.
//
// This is intentionally a package-local primitive.  P4-09C will expose the
// status/gc/doctor operations; keeping the scanner here first lets automatic
// callers and those future APIs share exactly the same candidate ordering and
// safety checks.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/CIPFZ/rdev/internal/storage"
)

const (
	defaultGCMaxScanJobs    = 1000
	defaultGCMaxDeleteJobs  = 100
	defaultGCMaxDeleteBytes = 1 << 30
)

// GCOptions bounds one pass. Zero values use the policy/default budget. Now is
// a test seam and is otherwise set to the current UTC time.
type GCOptions struct {
	DryRun         bool
	MaxScanJobs    int
	MaxDeleteJobs  int
	MaxDeleteBytes int64
	Now            time.Time
}

// GCCandidate is an auditable deletion decision. A dry-run returns the same
// records and reasons that a subsequent run would attempt, subject to races
// with jobs finishing or being removed by another process.
type GCCandidate struct {
	ID        string `json:"id"`
	Bytes     int64  `json:"bytes"`
	Reason    string `json:"reason"`
	StartedAt string `json:"started_at,omitempty"`
	EndedAt   string `json:"ended_at,omitempty"`
}

// GCReport contains both the pressure snapshot and bounded work accounting.
type GCReport struct {
	Root          string        `json:"root"`
	UsedBytes     int64         `json:"used_bytes"`
	FreeBytes     int64         `json:"free_bytes,omitempty"`
	TargetBytes   int64         `json:"target_bytes,omitempty"`
	Scanned       int           `json:"scanned"`
	ScanTruncated bool          `json:"scan_truncated,omitempty"`
	Pressure      bool          `json:"pressure"`
	DryRun        bool          `json:"dry_run"`
	Candidates    []GCCandidate `json:"candidates,omitempty"`
	Removed       []GCCandidate `json:"removed,omitempty"`
	Skipped       []string      `json:"skipped,omitempty"`
	FreedBytes    int64         `json:"freed_bytes"`
	Errors        []string      `json:"errors,omitempty"`
}

type gcJob struct {
	id      string
	dir     string
	meta    *jobMeta
	info    *protoJobInfo
	size    int64
	endedAt time.Time
}

// protoJobInfo is the small part of JobInfo used here. Keeping a private view
// avoids coupling the scanner's candidate logic to response-only fields.
type protoJobInfo struct {
	state, startedAt, endedAt string
}

// planStorageGC scans only state/jobs and computes an incremental plan. It
// never follows a symlink, accepts only current-user-owned records, and treats
// unknown files as non-reclaimable. A caller can use report.Candidates for a
// dry-run without mutating the filesystem.
func planStorageGC(state string, scope storage.ScopePolicy, options GCOptions) (*GCReport, error) {
	root, err := secureJobRoot(state)
	if err != nil {
		return nil, err
	}
	options = normalizeGCOptions(options)
	now := options.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	report := &GCReport{Root: root, DryRun: options.DryRun}
	report.UsedBytes, err = safeTreeSize(root)
	if err != nil {
		return nil, err
	}
	report.FreeBytes = filesystemFreeBytes(root)
	report.Pressure, report.TargetBytes = gcPressure(scope, report.UsedBytes, report.FreeBytes)

	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	jobs := make([]gcJob, 0, len(entries))
	for _, entry := range entries {
		if report.Scanned >= options.MaxScanJobs {
			report.ScanTruncated = true
			break
		}
		// DirEntry.IsDir deliberately returns false for symlinks, but Lstat is
		// repeated below to close the check against replacement races.
		if !entry.IsDir() || validateJobID(entry.Name()) != nil {
			continue
		}
		report.Scanned++
		dir := filepath.Join(root, entry.Name())
		st, statErr := os.Lstat(dir)
		if statErr != nil || st.Mode()&os.ModeSymlink != 0 || !st.IsDir() || !pathOwnedByCurrentUser(st) {
			report.Skipped = append(report.Skipped, entry.Name())
			continue
		}
		meta, metaErr := readMeta(dir)
		if metaErr != nil {
			report.Skipped = append(report.Skipped, entry.Name())
			continue
		}
		info := metaToInfo(meta, dir)
		if info.State == "running" {
			report.Skipped = append(report.Skipped, entry.Name())
			continue
		}
		// Unknown or unsafe contents are never removed as part of a whole-job
		// deletion. This is what prevents a user file copied into a job record
		// from becoming collateral damage.
		size, sizeErr := managedJobSize(dir)
		if sizeErr != nil {
			report.Skipped = append(report.Skipped, entry.Name())
			continue
		}
		// A dry-run must have the same lock admission semantics as execution;
		// otherwise it would promise deletion of a job that is currently being
		// started, stopped, or finalized.
		lockFn := tryJobLock
		if options.DryRun {
			lockFn = tryExistingJobLock
		}
		locked, lockErr := lockFn(dir, func() error {
			fresh, freshErr := readMeta(dir)
			if freshErr != nil {
				return errGCSkip
			}
			if fresh.ID != meta.ID || fresh.StartedAt != meta.StartedAt {
				return errGCSkip
			}
			if jobAlive(fresh, dir) {
				return errGCSkip
			}
			freshSize, sizeErr := managedJobSize(dir)
			if sizeErr != nil {
				return errGCSkip
			}
			size = freshSize
			return nil
		})
		if lockErr != nil || !locked {
			report.Skipped = append(report.Skipped, entry.Name())
			continue
		}
		if parseJobTime(info.StartedAt).IsZero() {
			// A timestamp is part of the retention proof. Never infer age from
			// directory mtime or an invalid value: doing so could reclaim a
			// record whose provenance cannot be established.
			report.Skipped = append(report.Skipped, entry.Name())
			continue
		}
		ended := parseJobTime(info.EndedAt)
		if ended.IsZero() {
			ended = parseJobTime(info.StartedAt)
		}
		jobs = append(jobs, gcJob{id: entry.Name(), dir: dir, meta: meta, info: &protoJobInfo{state: info.State, startedAt: info.StartedAt, endedAt: info.EndedAt}, size: size, endedAt: ended})
	}

	// Oldest first is deterministic. Keep-last is calculated from the exact
	// reverse order, so malformed/foreign records cannot displace a valid job.
	sort.SliceStable(jobs, func(i, j int) bool {
		return jobRecencyNewer(jobs[j].meta.StartedAt, jobs[j].id, jobs[i].meta.StartedAt, jobs[i].id)
	})
	protected := make(map[string]bool)
	if scope.KeepLastJobs > 0 {
		newest := append([]gcJob(nil), jobs...)
		sort.SliceStable(newest, func(i, j int) bool {
			return jobRecencyNewer(newest[i].meta.StartedAt, newest[i].id, newest[j].meta.StartedAt, newest[j].id)
		})
		if scope.KeepLastJobs > len(newest) {
			scope.KeepLastJobs = len(newest)
		}
		for _, candidate := range newest[:scope.KeepLastJobs] {
			protected[candidate.id] = true
		}
	}

	need := report.UsedBytes - report.TargetBytes
	if need < 0 {
		need = 0
	}
	if report.FreeBytes >= 0 && scope.MinFreeBytes > 0 && scope.MinFreeBytes-report.FreeBytes > need {
		need = scope.MinFreeBytes - report.FreeBytes
	}
	for _, candidate := range jobs {
		if protected[candidate.id] {
			continue
		}
		aged := scope.RetentionSec > 0 && !candidate.endedAt.IsZero() && now.Sub(candidate.endedAt) >= time.Duration(scope.RetentionSec)*time.Second
		if !aged && !report.Pressure {
			continue
		}
		reason := "quota"
		if aged {
			reason = "retention"
		}
		item := GCCandidate{ID: candidate.id, Bytes: candidate.size, Reason: reason, StartedAt: candidate.info.startedAt, EndedAt: candidate.info.endedAt}
		if len(report.Candidates) >= options.MaxDeleteJobs || (options.MaxDeleteBytes > 0 && gcCandidateBytes(report.Candidates)+item.Bytes > options.MaxDeleteBytes) {
			break
		}
		report.Candidates = append(report.Candidates, item)
		// Under pressure stop once low-watermark/min-free target is met. For
		// retention-only runs, all aged jobs are candidates.
		if report.Pressure && need > 0 {
			need -= candidate.size
			if need <= 0 {
				break
			}
		}
		if len(report.Candidates) >= options.MaxDeleteJobs || (options.MaxDeleteBytes > 0 && gcCandidateBytes(report.Candidates) >= options.MaxDeleteBytes) {
			break
		}
	}
	return report, nil
}

// runStorageGC executes a plan with non-blocking per-job locks. A failed
// rename/remove leaves the original record intact whenever possible; the
// rename-to-tombstone step makes a crash between those operations recoverable
// without exposing a half-deleted live job directory.
func runStorageGC(state string, scope storage.ScopePolicy, options GCOptions) (*GCReport, error) {
	report, err := planStorageGC(state, scope, options)
	if err != nil || options.DryRun {
		return report, err
	}
	for _, item := range report.Candidates {
		if len(report.Removed) >= normalizeGCOptions(options).MaxDeleteJobs {
			break
		}
		if normalizeGCOptions(options).MaxDeleteBytes > 0 && report.FreedBytes+item.Bytes > normalizeGCOptions(options).MaxDeleteBytes {
			break
		}
		dir := filepath.Join(report.Root, item.ID)
		locked, lockErr := tryJobLock(dir, func() error {
			st, statErr := os.Lstat(dir)
			if statErr != nil || st.Mode()&os.ModeSymlink != 0 || !st.IsDir() || !pathOwnedByCurrentUser(st) {
				return errGCSkip
			}
			meta, metaErr := readMeta(dir)
			if metaErr != nil || jobAlive(meta, dir) {
				return errGCSkip
			}
			// A directory can disappear and be recreated with the same ID
			// between planning and execution (notably with a deterministic test
			// generator, and in practice after a failed/manual restore). The
			// immutable start timestamp binds this deletion to the record that
			// was actually planned; without it GC could remove a newly-started
			// job while its PID is still in a short pre-publication window.
			if meta.ID != item.ID || meta.StartedAt != item.StartedAt {
				return errGCSkip
			}
			size, sizeErr := managedJobSize(dir)
			if sizeErr != nil {
				return errGCSkip
			}
			maxDeleteBytes := normalizeGCOptions(options).MaxDeleteBytes
			if maxDeleteBytes > 0 && report.FreedBytes+size > maxDeleteBytes {
				return errGCBudget
			}
			tombstone := filepath.Join(report.Root, fmt.Sprintf(".rdev-gc-%s-%d", item.ID, time.Now().UnixNano()))
			rel, relErr := filepath.Rel(report.Root, tombstone)
			if relErr != nil || strings.Contains(rel, string(filepath.Separator)) || !strings.HasPrefix(rel, ".rdev-gc-") {
				return errGCSkip
			}
			if renameErr := os.Rename(dir, tombstone); renameErr != nil {
				return renameErr
			}
			if removeErr := os.RemoveAll(tombstone); removeErr != nil {
				// Best effort rollback preserves the record after transient I/O
				// failures; a process crash leaves only our private tombstone.
				_ = os.Rename(tombstone, dir)
				return removeErr
			}
			removed := item
			// Account the bytes observed while holding the lock, rather than the
			// potentially stale plan estimate. This keeps the delete-byte budget
			// and the report honest if a log changed after planning.
			removed.Bytes = size
			report.Removed = append(report.Removed, removed)
			report.FreedBytes += size
			removeJobLock(dir)
			return nil
		})
		if errors.Is(lockErr, errGCBudget) {
			break
		}
		if lockErr != nil && !errors.Is(lockErr, errGCSkip) {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %v", item.ID, lockErr))
		} else if errors.Is(lockErr, errGCSkip) {
			report.Skipped = append(report.Skipped, item.ID)
		} else if !locked {
			report.Skipped = append(report.Skipped, item.ID)
		}
	}
	return report, nil
}

var errGCSkip = errors.New("gc candidate changed or is unsafe")
var errGCBudget = errors.New("gc delete byte budget exhausted")

func normalizeGCOptions(options GCOptions) GCOptions {
	if options.MaxScanJobs <= 0 {
		options.MaxScanJobs = defaultGCMaxScanJobs
	}
	if options.MaxDeleteJobs <= 0 {
		options.MaxDeleteJobs = defaultGCMaxDeleteJobs
	}
	if options.MaxDeleteBytes <= 0 {
		options.MaxDeleteBytes = defaultGCMaxDeleteBytes
	}
	return options
}

func gcCandidateBytes(items []GCCandidate) int64 {
	var n int64
	for _, item := range items {
		n += item.Bytes
	}
	return n
}

func gcPressure(scope storage.ScopePolicy, used, free int64) (bool, int64) {
	high := int64(float64(scope.MaxBytes) * scope.HighWatermark)
	low := int64(float64(scope.MaxBytes) * scope.LowWatermark)
	if high <= 0 {
		high = scope.MaxBytes
	}
	if low < 0 || low > high {
		low = high
	}
	pressure := used >= high
	if scope.MinFreeBytes > 0 && free >= 0 && free < scope.MinFreeBytes {
		pressure = true
	}
	return pressure, low
}

func parseJobTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// managedJobSize rejects symlinks, foreign ownership and unknown names before
// any whole-directory removal. Directories nested below a job are unknown by
// design: logs and metadata are all flat managed records.
func managedJobSize(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		st, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if st.Mode()&os.ModeSymlink != 0 || !pathOwnedByCurrentUser(st) {
			return fmt.Errorf("unsafe job entry %s", path)
		}
		if entry.IsDir() {
			if path != dir {
				return fmt.Errorf("nested job directory %s", path)
			}
			return nil
		}
		if !st.Mode().IsRegular() || !managedJobName(filepath.Base(path)) {
			return fmt.Errorf("unknown job entry %s", path)
		}
		total += st.Size()
		return nil
	})
	return total, err
}

func managedJobName(name string) bool {
	switch name {
	case "meta.json", "status.json", "child.json", "ledger.json", "stdout", "stderr", "storage-policy.json":
		return true
	default:
		return false
	}
}

// safeTreeSize counts logical bytes without following symlink directories.
// Symlinks are counted as their link payload, which ensures an unsafe entry
// still contributes to pressure while never becoming a deletion target.
func safeTreeSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		st, err := os.Lstat(path)
		if err != nil {
			return err
		}
		total += st.Size()
		return nil
	})
	return total, err
}

func filesystemFreeBytes(path string) int64 {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return -1
	}
	return int64(st.Bavail) * int64(st.Bsize)
}
