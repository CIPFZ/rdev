// Starting, stopping, and waiting for jobs.
//
// These are the operations that touch live processes. Two properties matter and
// are easy to break:
//
//   - A job outlives the agent. jobStart detaches with setsid and re-execs this
//     binary as a supervisor, so an exit code survives an ssh drop.
//   - A job is addressed by its recorded pgid, never by grepping ps output, which
//     is what makes jobStop reach the whole tree reliably.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
	statepkg "github.com/CIPFZ/rdev/internal/state"
	"github.com/CIPFZ/rdev/internal/storage"
)

// Wait polling bounds. The interval backs off so a job that runs for an hour
// costs a handful of stat calls per minute instead of ten per second, while a
// short job is still noticed almost immediately.
const (
	waitPollMin = 200 * time.Millisecond
	waitPollMax = 3 * time.Second
	// maxWaitSec caps a single wait. Beyond this the agent returns TimedOut so
	// the request cannot be stranded by a job that never finishes.
	maxWaitSec     = proto.MaxTimeoutSeconds
	defaultWaitSec = proto.DefaultJobWaitSeconds
)

const supervisorParentEnv = "RDEV_SUPERVISOR_PARENT_PID"
const supervisorLeaseEnv = "RDEV_SUPERVISOR_STATE_LEASE_FD"

func setEnvValue(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return append(out, prefix+value)
}

func withoutEnvValue(env []string, key string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env))
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return out
}

func jobStart(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.DurableStart {
		return nil, errDurableStartNeedsIdentity
	}
	return jobStartWithIdentity(p, state, nil, false)
}
func jobStartWithIdentity(p *proto.JobParams, state string, identity *jobStartIdentity, replay bool) (*proto.JobResult, error) {
	if p.Spec == nil || len(p.Spec.Argv) == 0 {
		return nil, invalidRequestError("job spec with argv required")
	}
	lease, err := statepkg.AcquireWriter(state)
	if err != nil {
		return nil, stateWriteError(err)
	}
	defer lease.Close()
	// The jobs directory is shared by multiple agent processes. The directory
	// creation itself is the uniqueness commit point; Mkdir (rather than
	// MkdirAll) lets a deliberately repeated ID fail closed instead of opening
	// an existing record and mixing its logs with a new process.
	if _, err := secureJobRoot(state); err != nil {
		return nil, err
	}
	// Admission is serialized across agent processes. Counting only published
	// supervisors is racy: two starters could both pass JobCount before either
	// metadata record becomes visible. The reservation directory is created
	// under this lock and is counted conservatively until its transaction
	// publishes meta.json.
	var effective proto.ResourceEnvelope
	var id, dir string
	reserved := false
	err = withJobLock(filepath.Join(state, "jobs", ".admission"), func() error {
		var err error
		if identity != nil {
			reserved, err = reserveJobStart(state, *identity, !replay)
			if err != nil {
				return err
			}
			id, dir = identity.JobID, jobDir(state, identity.JobID)
			if reserved {
				return nil
			}
		}
		effective, err = enforceJobEnvelope(p, state)
		if err != nil {
			return limitExceededError(err.Error())
		}
		for attempt := 0; attempt < 32; attempt++ {
			if identity == nil {
				id = jobIDGenerator()
			}
			dir = jobDir(state, id)
			if err := os.Mkdir(dir, 0o755); err != nil {
				if errors.Is(err, os.ErrExist) {
					if identity != nil {
						return proto.NewError(proto.CodeAmbiguousOutcome, identity.OperationID, proto.StatePossiblyExecuted)
					}
					continue
				}
				return err
			}
			if err := secureJobDir(dir); err != nil {
				_ = os.RemoveAll(dir)
				return err
			}
			return nil
		}
		return processStateError("could not allocate a unique job id")
	})
	if err != nil {
		return nil, err
	}
	var result *proto.JobResult
	err = withJobLock(dir, func() error {
		if reserved {
			result, err = recoveredJobStart(dir, identity)
			return err
		}
		result, err = startJobTransaction(p, id, dir, effective, identity)
		return err
	})
	if err != nil {
		if identity == nil {
			removeJobLock(dir)
		}
		return nil, err
	}
	return result, nil
}

var errJobIDCollision = errors.New("job id already exists")

// writeJobMeta is replaceable only by package tests. Keeping the fault seam at
// the publication boundary lets tests prove that a failed metadata rename
// rolls back the already-started supervisor, without weakening normal file
// permissions or relying on a root/non-root distinction.
var writeJobMeta = func(path string, v any) error { return writeJSON(path, v) }

// startJobTransaction is called while holding the job lock. No externally
// visible job record is committed until meta.json is atomically published. If
// any post-Start step fails, the whole process group is killed and reaped before
// the directory is removed, so metadata failures cannot strand a runnable job.
func startJobTransaction(p *proto.JobParams, id, dir string, effective proto.ResourceEnvelope, identity *jobStartIdentity) (*proto.JobResult, error) {
	cmd, err := buildCmd(p.Spec)
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	self, err := os.Executable()
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("locate agent binary: %w", err)
	}
	inner := append([]string(nil), cmd.Args...)
	cmd.Path = self
	cmd.Args = append([]string{self, superviseFlag, dir, "--"}, inner...)
	cmd.Env = setEnvValue(cmd.Env, supervisorParentEnv, strconv.Itoa(os.Getpid()))
	state := filepath.Dir(filepath.Dir(dir))
	lease, err := statepkg.AcquireWriter(state)
	if err != nil {
		return nil, stateWriteError(err)
	}
	defer lease.Close()
	if err := configureSupervisor(cmd, lease); err != nil {
		return nil, err
	}
	policy, err := storage.Load(filepath.Join(state, "storage-policy.json"))
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	if err := storage.Save(filepath.Join(dir, "storage-policy.json"), policy); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	_ = stdout.Chmod(0o600)
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		stdout.Close()
		os.RemoveAll(dir)
		return nil, err
	}
	_ = stderr.Chmod(0o600)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		stdout.Close()
		stderr.Close()
		os.RemoveAll(dir)
		return nil, processStartError(err)
	}
	// The child inherited its own handles. Release the parent's copies before
	// publishing metadata so they cannot block Windows log replacement.
	stdout.Close()
	stderr.Close()
	if err := awaitSupervisor(cmd, dir); err != nil {
		_ = rollbackStartedJob(cmd, dir)
		return nil, processStartError(err)
	}
	processToken, err := processIdentity(cmd.Process.Pid)
	if err != nil {
		_ = rollbackStartedJob(cmd, dir)
		return nil, processStartError(fmt.Errorf("record process identity: %w", err))
	}

	meta := &jobMeta{
		SignalRelay:        true,
		SchemaVersion:      statepkg.CurrentSchemaVersion,
		ID:                 id,
		Label:              p.Label,
		Argv:               p.Spec.Argv,
		Cwd:                p.Spec.Cwd,
		PID:                cmd.Process.Pid,
		ProcessIdentity:    processToken,
		StartedAt:          time.Now().UTC().Format(time.RFC3339Nano),
		StoragePolicy:      policy.PerJob,
		RequestedResources: resourceOrZero(p.Resources),
		EffectiveResources: effective,
	}
	if identity != nil {
		meta.StartOperationID, meta.StartPrincipalID, meta.StartDigest = identity.OperationID, identity.PrincipalID, identity.Digest
	}
	if err := writeJobMeta(filepath.Join(dir, "meta.json"), meta); err != nil {
		rbErr := rollbackStartedJob(cmd, dir)
		return nil, errors.Join(err, rbErr)
	}

	// The serving agent reaps the supervisor when it remains alive. The
	// supervisor independently writes status.json, so this is only a fast path.
	go func() { _ = cmd.Wait() }()
	return &proto.JobResult{Info: metaToInfo(meta, dir)}, nil
}

func resourceOrZero(r *proto.ResourceEnvelope) proto.ResourceEnvelope {
	if r == nil {
		return proto.ResourceEnvelope{}
	}
	return *r
}

// jobWait blocks until the job leaves the running state, or the wait budget
// expires.
//
// This replaces caller-side polling: one call covers a long batch instead of a
// status check every few seconds. The job is never affected by the wait, so a
// TimedOut reply just means "ask again".
func jobWait(p *proto.JobParams, state string) (*proto.JobResult, error) {
	return jobWaitContext(context.Background(), defaultWaitHub, p, state)
}

// jobWaitMany waits on several jobs in one call.
//
// Waiting on N parallel jobs used to mean N serial round trips, each re-sending
// the same context and each blocking its own budget. One call now covers the
// batch: the shared deadline is what makes it cheaper, not just tidier.
//
// A job that cannot be read (unknown id) is reported per-job rather than failing
// the call, since the other jobs still have useful answers.
func jobWaitMany(p *proto.JobParams, state string) (*proto.JobResult, error) {
	return jobWaitContext(context.Background(), defaultWaitHub, p, state)
}

// finishWait assembles the wait reply, attaching trailing output when asked.
func finishWait(p *proto.JobParams, dir string, info *proto.JobInfo, start time.Time, timedOut bool) *proto.JobResult {
	res := &proto.JobResult{
		Info:     info,
		TimedOut: timedOut,
		WaitedMS: time.Since(start).Milliseconds(),
	}
	if p.TailOnExit > 0 {
		// Best effort: a missing log file should not fail the wait, since the
		// status is the answer the caller actually needs.
		if logs, err := readTail(filepath.Join(dir, "stdout"), p.TailOnExit); err == nil {
			res.Logs = logs
		}
	}
	return res
}
