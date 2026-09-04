package main

// Storage inspection and bounded reclamation operations. These handlers never
// accept a filesystem path from the caller: the agent's state directory is the
// sole authority for scope and all mutations reuse the owner/lock-safe GC.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/storage"
)

func storageScope(p *proto.StorageParams) (storage.ScopePolicy, string, error) {
	if p == nil {
		return storage.ScopePolicy{}, "", invalidRequestError("storage parameters required")
	}
	scope := strings.TrimSpace(strings.ToLower(p.Scope))
	if scope == "" {
		scope = "remote_state"
	}
	if scope != "remote_state" && scope != "local" {
		return storage.ScopePolicy{}, "", invalidRequestError("storage scope must be local or remote_state")
	}
	return storage.Default().RemoteState, scope, nil
}

func loadStoragePolicy(state string) (storage.Policy, string, error) {
	path := filepath.Join(state, "storage-policy.json")
	if st, err := os.Lstat(path); err == nil {
		if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() || !pathOwnedByCurrentUser(st) {
			return storage.Policy{}, path, fmt.Errorf("storage policy is not a private owned regular file")
		}
	} else if !os.IsNotExist(err) {
		return storage.Policy{}, path, err
	}
	p, err := storage.Load(path)
	if err != nil {
		return storage.Policy{}, path, err
	}
	return p, path, nil
}

// readMetaReadOnly validates ownership/object type without repairing file
// permissions. readMeta uses secureRecordFile, which chmods legacy records;
// status and doctor must not mutate those records while inspecting them.
func readMetaReadOnly(dir string) (*jobMeta, error) {
	path := filepath.Join(dir, "meta.json")
	st, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if st.Mode()&os.ModeSymlink != 0 || !st.Mode().IsRegular() || !pathOwnedByCurrentUser(st) {
		return nil, fmt.Errorf("%s is not an owned regular file", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m jobMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	if m.ID == "" {
		return nil, processStateError("invalid job metadata")
	}
	if err := validateJobID(m.ID); err != nil || m.ID != filepath.Base(dir) {
		return nil, processStateError("invalid job metadata id")
	}
	return &m, nil
}

func storageStatus(p *proto.StorageParams, state string) (*proto.StorageScope, error) {
	policy, policyPath, err := loadStoragePolicy(state)
	if err != nil {
		return nil, fmt.Errorf("storage policy: %w", err)
	}
	scope, name, err := storageScope(p)
	if err != nil {
		return nil, err
	}
	if name == "remote_state" {
		scope = policy.RemoteState
	} else {
		scope = policy.Local
	}
	root, err := filepath.Abs(filepath.Join(state, "jobs"))
	if err != nil {
		return nil, err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() || !pathOwnedByCurrentUser(rootInfo) {
		return nil, fmt.Errorf("job root is not a private directory")
	}
	used, err := safeTreeSize(root)
	if err != nil {
		return nil, err
	}
	free := filesystemFreeBytes(root)
	pressure, target := gcPressure(scope, used, free)
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	jobs, running := 0, 0
	for _, e := range entries {
		if !e.IsDir() || validateJobID(e.Name()) != nil {
			continue
		}
		dir := filepath.Join(root, e.Name())
		st, e1 := os.Lstat(dir)
		if e1 != nil || st.Mode()&os.ModeSymlink != 0 || !st.IsDir() || !pathOwnedByCurrentUser(st) {
			continue
		}
		meta, e1 := readMetaReadOnly(dir)
		if e1 != nil {
			continue
		}
		jobs++
		if jobAlive(meta, dir) {
			running++
		}
	}
	return &proto.StorageScope{Name: name, Root: root, UsedBytes: used, FreeBytes: free, MaxBytes: scope.MaxBytes,
		TargetBytes: target, MinFreeBytes: scope.MinFreeBytes, HighWatermark: scope.HighWatermark,
		LowWatermark: scope.LowWatermark, RetentionSec: scope.RetentionSec, KeepLastJobs: scope.KeepLastJobs,
		JobCount: jobs, RunningJobs: running, Pressure: pressure, PolicySource: policyPath}, nil
}

func storageGC(p *proto.StorageParams, state string) (*proto.StorageGCReport, error) {
	if p.MaxScanJobs < 0 || p.MaxScanJobs > storage.HardKeepLastJobs || p.MaxDeleteJobs < 0 || p.MaxDeleteJobs > storage.HardKeepLastJobs || p.MaxDeleteBytes < 0 || p.MaxDeleteBytes > storage.HardMaxBytes {
		return nil, limitExceededError("storage gc bounds are outside the hard limit")
	}
	policy, _, err := loadStoragePolicy(state)
	if err != nil {
		return nil, fmt.Errorf("storage policy: %w", err)
	}
	scope, name, err := storageScope(p)
	if err != nil {
		return nil, err
	}
	if name == "remote_state" {
		scope = policy.RemoteState
	} else {
		scope = policy.Local
	}
	// A caller may further lower a configured cleanup budget, but may never
	// widen it through the remote API. Zero means "use the persisted/default
	// budget", matching the package GC options convention.
	cleanup := policy.Cleanup
	if cleanup.MaxScanJobs <= 0 {
		cleanup.MaxScanJobs = defaultGCMaxScanJobs
	}
	if cleanup.MaxDeleteJobs <= 0 {
		cleanup.MaxDeleteJobs = defaultGCMaxDeleteJobs
	}
	if cleanup.MaxDeleteBytes <= 0 {
		cleanup.MaxDeleteBytes = defaultGCMaxDeleteBytes
	}
	if (p.MaxScanJobs > 0 && p.MaxScanJobs > cleanup.MaxScanJobs) ||
		(p.MaxDeleteJobs > 0 && p.MaxDeleteJobs > cleanup.MaxDeleteJobs) ||
		(p.MaxDeleteBytes > 0 && p.MaxDeleteBytes > cleanup.MaxDeleteBytes) {
		return nil, limitExceededError("storage gc bounds exceed the configured cleanup policy")
	}
	opts := GCOptions{DryRun: p.DryRun, MaxScanJobs: p.MaxScanJobs, MaxDeleteJobs: p.MaxDeleteJobs, MaxDeleteBytes: p.MaxDeleteBytes}
	if opts.MaxScanJobs == 0 {
		opts.MaxScanJobs = cleanup.MaxScanJobs
	}
	if opts.MaxDeleteJobs == 0 {
		opts.MaxDeleteJobs = cleanup.MaxDeleteJobs
	}
	if opts.MaxDeleteBytes == 0 {
		opts.MaxDeleteBytes = cleanup.MaxDeleteBytes
	}
	gc, err := runStorageGC(state, scope, opts)
	if err != nil {
		return nil, err
	}
	return storageGCProto(gc), nil
}

func storageGCProto(in *GCReport) *proto.StorageGCReport {
	if in == nil {
		return nil
	}
	out := &proto.StorageGCReport{Root: in.Root, UsedBytes: in.UsedBytes, FreeBytes: in.FreeBytes, TargetBytes: in.TargetBytes,
		Scanned: in.Scanned, ScanTruncated: in.ScanTruncated, Pressure: in.Pressure, DryRun: in.DryRun, FreedBytes: in.FreedBytes,
		Skipped: append([]string(nil), in.Skipped...), Errors: append([]string(nil), in.Errors...)}
	conv := func(items []GCCandidate) []proto.StorageGCItem {
		r := make([]proto.StorageGCItem, 0, len(items))
		for _, x := range items {
			r = append(r, proto.StorageGCItem{ID: x.ID, Bytes: x.Bytes, Reason: x.Reason, StartedAt: x.StartedAt, EndedAt: x.EndedAt})
		}
		return r
	}
	out.Candidates, out.Removed = conv(in.Candidates), conv(in.Removed)
	return out
}

func storageDoctor(p *proto.StorageParams, state string) (*proto.StorageDoctorReport, error) {
	if _, _, err := storageScope(p); err != nil {
		return nil, err
	}
	report := &proto.StorageDoctorReport{}
	stateAbs, err := filepath.Abs(state)
	if err != nil {
		return nil, err
	}
	report.Root = filepath.Join(stateAbs, "jobs")
	stateInfo, stateErr := os.Lstat(stateAbs)
	if stateErr != nil || stateInfo.Mode()&os.ModeSymlink != 0 || !stateInfo.IsDir() || !pathOwnedByCurrentUser(stateInfo) {
		report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "state_root_invalid", Severity: "error", Path: stateAbs, Message: "state root is missing, not a private owned directory, or is a symlink", Action: "create a private owner-only state directory"})
		return report, nil
	}
	if stateInfo.Mode().Perm() != 0o700 {
		report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "state_root_permissions", Severity: "warning", Path: stateAbs, Message: "state root permissions are broader than 0700", Action: "chmod the state root to 0700"})
	}
	jobRootInfo, jobRootErr := os.Lstat(report.Root)
	if jobRootErr != nil || jobRootInfo.Mode()&os.ModeSymlink != 0 || !jobRootInfo.IsDir() || !pathOwnedByCurrentUser(jobRootInfo) {
		report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "job_root_invalid", Severity: "error", Path: report.Root, Message: "jobs root is missing, not a private owned directory, or is a symlink", Action: "create or repair the jobs root with owner-only permissions"})
		return report, nil
	}
	if jobRootInfo.Mode().Perm() != 0o700 {
		report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "job_root_permissions", Severity: "warning", Path: report.Root, Message: "jobs root permissions are broader than 0700", Action: "chmod the jobs root to 0700"})
	}
	root := report.Root
	if _, _, err := loadStoragePolicy(state); err != nil {
		report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "policy_invalid", Severity: "error", Path: filepath.Join(state, "storage-policy.json"), Message: err.Error(), Action: "restore a valid policy or remove the file to use defaults"})
	}
	if policyPath := filepath.Join(state, "storage-policy.json"); func() bool { st, e := os.Lstat(policyPath); return e == nil && st.Mode().Perm() != 0o600 }() {
		report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "policy_permissions", Severity: "warning", Path: "storage-policy.json", Message: "storage policy permissions are broader than 0600", Action: "chmod the policy file to 0600"})
	}
	if free := filesystemFreeBytes(root); free >= 0 {
		if used, e1 := safeTreeSize(root); e1 == nil {
			policy, _, _ := loadStoragePolicy(state)
			selected := policy.RemoteState
			if strings.EqualFold(strings.TrimSpace(p.Scope), "local") {
				selected = policy.Local
			}
			if pressure, _ := gcPressure(selected, used, free); pressure {
				report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "storage_pressure", Severity: "warning", Message: "storage is above the configured high watermark or minimum free space", Action: "run storage_gc dry-run, then execute a bounded cleanup"})
			}
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(root, name)
		if strings.HasPrefix(name, ".rdev-gc-") {
			st, e1 := os.Lstat(path)
			if e1 != nil || st.Mode()&os.ModeSymlink != 0 || !pathOwnedByCurrentUser(st) {
				report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "unsafe_gc_tombstone", Severity: "error", Path: name, Message: "GC tombstone is missing, a symlink, or not owned by the current user", Action: "do not remove automatically; inspect and quarantine it manually"})
				continue
			}
			if now.Sub(st.ModTime()) > time.Hour {
				report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "stale_gc_tombstone", Severity: "warning", Path: name, Message: "owner-safe GC tombstone is older than one hour", Action: "inspect and remove only after confirming no GC process is active"})
			}
			continue
		}
		if name == lockDirName {
			continue
		}
		if validateJobID(name) != nil || !e.IsDir() {
			if name != lockDirName && !strings.HasPrefix(name, ".rdev-gc-") {
				report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "unknown_state_entry", Severity: "warning", Path: name, Message: "unmanaged entry is present under the jobs root", Action: "inspect manually; automatic GC will not remove it"})
			}
			continue
		}
		st, e1 := os.Lstat(path)
		if e1 != nil {
			continue
		}
		if st.Mode()&os.ModeSymlink != 0 || !st.IsDir() || !pathOwnedByCurrentUser(st) {
			report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "job_entry_unsafe", Severity: "error", Path: name, Message: "job entry is not a private owned directory", Action: "quarantine manually; automatic GC will skip it"})
			continue
		}
		if _, e1 := readMetaReadOnly(path); e1 != nil {
			report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "job_metadata_invalid", Severity: "warning", Path: name, Message: "job metadata cannot be read", Action: "inspect record; automatic GC will skip it"})
		}
	}
	// A lock file that can be acquired is not active; report only stale files.
	lockRoot := filepath.Join(filepath.Dir(root), lockDirName)
	if lockEntries, e1 := os.ReadDir(lockRoot); e1 == nil {
		for _, e := range lockEntries {
			if !strings.HasSuffix(e.Name(), ".lock") {
				continue
			}
			path := filepath.Join(lockRoot, e.Name())
			st, e2 := os.Lstat(path)
			if e2 != nil || st.Mode()&os.ModeSymlink != 0 || !pathOwnedByCurrentUser(st) {
				continue
			}
			if now.Sub(st.ModTime()) <= time.Hour {
				continue
			}
			acquired, _ := probeExistingJobLock(filepath.Join(root, strings.TrimSuffix(e.Name(), ".lock")))
			if acquired {
				report.Findings = append(report.Findings, proto.StorageDoctorFinding{Code: "stale_job_lock", Severity: "warning", Path: e.Name(), Message: "lock file is old and not held by another process", Action: "it is safe to remove after confirming no concurrent agent"})
			}
		}
	}
	report.OK = len(report.Findings) == 0
	return report, nil
}

func doStorage(op string, p *proto.StorageParams, state string) (*proto.StorageResult, error) {
	if p == nil {
		return nil, invalidRequestError("storage parameters required")
	}
	switch op {
	case proto.OpStorageStatus:
		v, err := storageStatus(p, state)
		if err != nil {
			return nil, err
		}
		return &proto.StorageResult{Status: v}, nil
	case proto.OpStorageGC:
		v, err := storageGC(p, state)
		if err != nil {
			return nil, err
		}
		return &proto.StorageResult{GC: v}, nil
	case proto.OpStorageDoctor:
		v, err := storageDoctor(p, state)
		if err != nil {
			return nil, err
		}
		return &proto.StorageResult{Doctor: v}, nil
	default:
		return nil, invalidRequestError("unknown storage operation")
	}
}
