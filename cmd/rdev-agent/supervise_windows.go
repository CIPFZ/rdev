package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	statepkg "github.com/CIPFZ/rdev/internal/state"
	"github.com/CIPFZ/rdev/internal/storage"
	"github.com/CIPFZ/rdev/internal/winutil"
	"golang.org/x/sys/windows"
)

const superviseFlag = "-supervise"

func runSupervisor(dir string, argv []string) { os.Exit(superviseWindows(dir, argv)) }
func superviseWindows(dir string, argv []string) int {
	root := filepath.Dir(filepath.Dir(dir))
	lease, err := statepkg.AcquireWriter(root)
	if err != nil {
		return 125
	}
	defer lease.Close()
	if len(argv) == 0 || secureJobDir(dir) != nil {
		return 125
	}
	identity, err := processIdentity(os.Getpid())
	if err != nil {
		return 125
	}
	if err = writeJSON(filepath.Join(dir, "supervisor-ready.json"), map[string]any{"pid": os.Getpid(), "identity": identity}); err != nil {
		return 125
	}
	parent, _ := strconv.Atoi(os.Getenv(supervisorParentEnv))
	parentIdentity := os.Getenv("RDEV_SUPERVISOR_PARENT_IDENTITY")
	var meta jobMeta
	for {
		if err = readJSON(filepath.Join(dir, "meta.json"), &meta); err == nil && meta.PID == os.Getpid() && meta.ProcessIdentity == identity {
			break
		}
		if parentIdentity == "" || !processMatches(parent, parentIdentity) {
			return 125
		}
		time.Sleep(10 * time.Millisecond)
	}
	policy, err := storage.Load(filepath.Join(dir, "storage-policy.json"))
	if err != nil {
		return 125
	}
	// The parent supplied file handles for early supervisor diagnostics. Go
	// opens them without FILE_SHARE_DELETE, which would prevent publishing the
	// captured child logs while this supervisor remains alive. Child output
	// below uses independent pipes and bounded sinks.
	_ = os.Stdout.Close()
	_ = os.Stderr.Close()
	status := func(code int, killed bool, resource string, cause error, so, se *storage.Sink) {
		v := map[string]any{"exit_code": code, "ended_at": time.Now().UTC().Format(time.RFC3339Nano), "killed": killed, "resource_limit": resource}
		if cause != nil {
			v["error"] = cause.Error()
		}
		if so != nil {
			logErr := errors.Join(winutil.AtomicWrite(filepath.Join(dir, "stdout"), so.Bytes()), winutil.AtomicWrite(filepath.Join(dir, "stderr"), se.Bytes()))
			if logErr != nil {
				v["error"] = errors.Join(cause, logErr).Error()
			}
			v["stdout_ledger"], v["stderr_ledger"] = ledgerProto(so.Ledger()), ledgerProto(se.Ledger())
		}
		_ = withJobLock(dir, func() error {
			if !jobExists(dir) {
				return nil
			}
			return writeJSON(filepath.Join(dir, "status.json"), v)
		})
	}
	stopRequested := func() bool {
		var stop struct {
			Identity string `json:"identity"`
		}
		return readJSON(filepath.Join(dir, "stop.json"), &stop) == nil && stop.Identity == identity
	}
	if stopRequested() {
		status(-1, true, "", nil, nil, nil)
		return 1
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = withoutEnvValue(withoutEnvValue(withoutEnvValue(os.Environ(), supervisorParentEnv), supervisorLeaseEnv), "RDEV_SUPERVISOR_PARENT_IDENTITY")
	cmd.WaitDelay = 2 * time.Second
	so, se := storage.NewSink(policy.PerJob.MaxStdoutBytes, policy.PerJob.OnLogLimit), storage.NewSink(policy.PerJob.MaxStderrBytes, policy.PerJob.OnLogLimit)
	cmd.Stdout, cmd.Stderr = so, se
	tree, err := winutil.StartTree(cmd, windowsJobName(dir, identity))
	if err != nil {
		status(-1, false, "", err, so, se)
		return 127
	}
	defer tree.Close()
	childIdentity := tree.Identity
	err = withJobLock(dir, func() error {
		return writeJSON(filepath.Join(dir, "child.json"), map[string]any{"child_pid": tree.PID, "process_identity": childIdentity})
	})
	if err != nil {
		_ = tree.Kill()
		_ = cmd.Wait()
		status(-1, false, "", err, so, se)
		return 125
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var wall <-chan time.Time
	if meta.EffectiveResources.WallTimeoutSec > 0 {
		timer := time.NewTimer(time.Duration(meta.EffectiveResources.WallTimeoutSec) * time.Second)
		defer timer.Stop()
		wall = timer.C
	}
	killed, limit := false, ""
wait:
	for {
		select {
		case <-tree.Done:
			killed = stopRequested()
			break wait
		case <-wall:
			limit = "wall_timeout"
			break wait
		case <-ticker.C:
			if stopRequested() {
				killed = true
				break wait
			}
			if policy.PerJob.OnLogLimit == "stop_job" && (so.LimitReached() || se.LimitReached()) {
				limit = "log_bytes"
				break wait
			}
			// Publish a readable snapshot while the job is running so job_logs is
			// useful on Windows too; the terminal write remains authoritative.
			_ = winutil.AtomicWrite(filepath.Join(dir, "stdout"), so.Bytes())
			_ = winutil.AtomicWrite(filepath.Join(dir, "stderr"), se.Bytes())
			_ = writeJSON(filepath.Join(dir, "ledger.json"), map[string]any{"stdout_ledger": ledgerProto(so.Ledger()), "stderr_ledger": ledgerProto(se.Ledger())})
		}
	}
	_ = tree.Kill()
	err = cmd.Wait()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		code = -1
	}
	if killed || limit != "" {
		code = -1
	}
	status(code, killed, limit, nil, so, se)
	if code < 0 {
		return 1
	}
	return code
}

func jobStop(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.GraceSec < 0 || p.GraceSec > hardExecTimeoutSec {
		return nil, limitExceededError("grace_sec is outside the hard limit")
	}
	if p.Signal != "" && !strings.EqualFold(p.Signal, "TERM") && !strings.EqualFold(p.Signal, "KILL") {
		return nil, invalidRequestError("signal must be TERM or KILL")
	}
	lease, err := statepkg.AcquireWriter(state)
	if err != nil {
		return nil, stateWriteError(err)
	}
	defer lease.Close()
	dir, err := validatedJobDir(state, p.ID)
	if err != nil {
		return nil, err
	}
	meta, err := readMeta(dir)
	if err != nil {
		return nil, err
	}
	if !jobAlive(meta, dir) {
		return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
	}
	// Keep the exact supervisor process object open throughout admission. The
	// named child job also embeds its immutable creation time.
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.SYNCHRONIZE, false, uint32(meta.PID))
	if err != nil {
		return nil, processStateError("job supervisor is unavailable")
	}
	defer windows.CloseHandle(h)
	identity, err := winutil.HandleProcessIdentity(h)
	if err != nil || identity != meta.ProcessIdentity {
		return nil, processStateError("job process identity no longer matches")
	}
	if err = withJobLock(dir, func() error {
		if !jobExists(dir) {
			return os.ErrNotExist
		}
		return writeJSON(filepath.Join(dir, "stop.json"), map[string]any{"identity": identity})
	}); err != nil {
		return nil, err
	}
	if strings.EqualFold(p.Signal, "KILL") {
		e := winutil.KillNamedTree(windowsJobName(dir, identity))
		// The child might not have started yet. The durable stop record fences
		// that window and is consumed by the supervisor before/after startup.
		if e != nil && !errors.Is(e, windows.ERROR_FILE_NOT_FOUND) {
			return nil, fmt.Errorf("stop Windows job: %w", e)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && jobAlive(meta, dir) {
		time.Sleep(20 * time.Millisecond)
	}
	return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
}

func ledgerProto(l storage.Ledger) proto.LogLedger {
	return proto.LogLedger{OriginalBytes: l.OriginalBytes, RetainedBytes: l.RetainedBytes, DroppedBytes: l.DroppedBytes, Truncated: l.Truncated, FirstTruncatedAt: l.FirstTruncatedAt, LimitBytes: l.LimitBytes, Policy: l.Policy}
}
