// Concurrency tests for job record mutation.
//
// Job records are written by several agents and, within one agent, by several
// goroutines. These tests drive the paths that read a record and then act on it,
// which is where an unlocked implementation produces a wrong answer.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// startFinishedJob starts a trivial job and waits for it to finish, returning its
// id. A finished job is the interesting input for removal: a running one is
// skipped by an earlier branch.
func startFinishedJob(t *testing.T, state string) string {
	t.Helper()
	resp := handleSafely(&proto.Request{
		Op:  proto.OpJobStart,
		Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "true"}}},
	}, state)
	if !resp.OK {
		t.Fatalf("job start: %s", resp.Err)
	}
	id := resp.Job.Info.ID
	if _, err := jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 10}, state); err != nil {
		t.Fatalf("job wait: %v", err)
	}
	return id
}

func newJobState(t *testing.T) string {
	t.Helper()
	state := t.TempDir()
	if err := os.MkdirAll(filepath.Join(state, "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	return state
}

func measuredDirSize(t *testing.T, dir string) int64 {
	t.Helper()
	size, err := dirSize(dir)
	if err != nil {
		t.Fatalf("measure %s: %v", dir, err)
	}
	return size
}

func TestJobRecencyOrderHandlesMixedPrecisionAndTies(t *testing.T) {
	whole := &proto.JobInfo{ID: "job-z", StartedAt: "2026-08-28T12:00:00Z"}
	fractional := &proto.JobInfo{ID: "job-a", StartedAt: "2026-08-28T12:00:00.1Z"}
	if !jobInfoNewer(fractional, whole) {
		t.Error("fractional timestamp should be newer than the whole-second legacy record")
	}
	if jobInfoNewer(whole, fractional) {
		t.Error("whole-second legacy record should not sort after a later fractional timestamp")
	}

	tiedA := &proto.JobInfo{ID: "job-a", StartedAt: whole.StartedAt}
	tiedD := &proto.JobInfo{ID: "job-d", StartedAt: whole.StartedAt}
	if !jobInfoNewer(tiedD, tiedA) || jobInfoNewer(tiedA, tiedD) {
		t.Error("equal timestamps should use descending job ID as a deterministic tie-breaker")
	}
}

func TestDirSizeFailsClosedWhenRecordCannotBeWalked(t *testing.T) {
	if _, err := dirSize(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("missing job record was measured as zero bytes instead of returning an error")
	}
}

// Removing a job twice must not leak a filesystem error. The second caller asked
// for the job to be absent and it is, so the answer is a structured "missing".
//
// This is the serial half of the bug: it reproduces without any concurrency,
// because the failing read is simply the one that runs after the delete.
func TestJobRmTwiceIsIdempotent(t *testing.T) {
	state := newJobState(t)
	id := startFinishedJob(t, state)

	first := handleSafely(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: id}}, state)
	if !first.OK {
		t.Fatalf("first rm failed: %s", first.Err)
	}
	if len(first.Job.Removed) != 1 {
		t.Errorf("first rm removed %v, want the job", first.Job.Removed)
	}

	second := handleSafely(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: id}}, state)
	if !second.OK {
		t.Fatalf("second rm surfaced an error instead of reporting the job missing: %s", second.Err)
	}
	if len(second.Job.Removed) != 0 {
		t.Errorf("second rm claimed to remove %v, but the first one already did", second.Job.Removed)
	}
	if len(second.Job.Missing) != 1 || second.Job.Missing[0] != id {
		t.Errorf("second rm Missing = %v, want [%s]", second.Job.Missing, id)
	}
	if second.Job.FreedBytes != 0 {
		t.Errorf("second rm claimed %d freed bytes for a job it did not remove", second.Job.FreedBytes)
	}
}

// Concurrent removals of one job must produce exactly one winner.
//
// This is the half that does not announce itself: os.RemoveAll returns nil for a
// path that is already gone, so every unlocked racer that read the record before
// the delete reported success and added the same directory's size to freed_bytes.
// Six concurrent calls reported six removals.
func TestConcurrentJobRmHasOneWinner(t *testing.T) {
	// Repeated because the interleaving is what is being tested; a single round
	// can pass by luck even against an unlocked implementation.
	for round := 0; round < 20; round++ {
		state := newJobState(t)
		id := startFinishedJob(t, state)
		size := measuredDirSize(t, jobDir(state, id))

		const racers = 6
		var wg sync.WaitGroup
		// Released together, so the calls overlap rather than queueing.
		start := make(chan struct{})
		results := make([]*proto.Response, racers)
		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				results[i] = handleSafely(&proto.Request{
					Op: proto.OpJobRm, Job: &proto.JobParams{ID: id},
				}, state)
			}(i)
		}
		close(start)
		wg.Wait()

		var removed, missing int
		var freed int64
		for i, r := range results {
			if !r.OK {
				t.Fatalf("round %d: racer %d surfaced an error: %s", round, i, r.Err)
			}
			removed += len(r.Job.Removed)
			missing += len(r.Job.Missing)
			freed += r.Job.FreedBytes
		}
		if removed != 1 {
			t.Fatalf("round %d: %d racers claimed to remove the job, want exactly 1", round, removed)
		}
		if missing != racers-1 {
			t.Fatalf("round %d: %d racers reported the job missing, want %d", round, missing, racers-1)
		}
		// The whole point of one winner: the reclaimed total is the job's real size,
		// not a multiple of it.
		if freed != size {
			t.Fatalf("round %d: freed_bytes summed to %d across racers, want %d (the job's actual size)",
				round, freed, size)
		}
		if _, err := os.Stat(jobDir(state, id)); !os.IsNotExist(err) {
			t.Fatalf("round %d: job directory survived the removals", round)
		}
	}
}

// A sweep and a targeted removal race on the same records. Neither may report a
// job the other one deleted, and no filesystem error may escape.
func TestConcurrentSweepAndRmDoNotDoubleCount(t *testing.T) {
	for round := 0; round < 10; round++ {
		state := newJobState(t)
		const jobs = 5
		ids := make([]string, jobs)
		sizes := make(map[string]int64, jobs)
		for i := range ids {
			ids[i] = startFinishedJob(t, state)
			sizes[ids[i]] = measuredDirSize(t, jobDir(state, ids[i]))
		}
		var want int64
		for _, s := range sizes {
			want += s
		}

		// A sweeper claiming every job, racing a targeted rm for each one.
		var wg sync.WaitGroup
		start := make(chan struct{})
		var mu sync.Mutex
		var results []*proto.Response
		record := func(r *proto.Response) {
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// keep_last covers everything present; an age filter would spare these,
			// since they just ended.
			record(handleSafely(&proto.Request{
				Op: proto.OpJobRm, Job: &proto.JobParams{KeepLast: 0, OlderThanSec: 0},
			}, state))
		}()
		for _, id := range ids {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				<-start
				record(handleSafely(&proto.Request{
					Op: proto.OpJobRm, Job: &proto.JobParams{ID: id},
				}, state))
			}(id)
		}
		close(start)
		wg.Wait()

		seen := map[string]int{}
		var freed int64
		for _, r := range results {
			if !r.OK {
				// A filterless sweep is rejected by validation, which is a usage
				// error rather than a race. Anything else is the bug under test.
				if r.Job == nil && r.Error != nil && r.Error.Code == proto.CodeInvalidRequest {
					continue
				}
				t.Fatalf("round %d: a racer surfaced an error: %s", round, r.Err)
			}
			for _, id := range r.Job.Removed {
				seen[id]++
			}
			freed += r.Job.FreedBytes
		}
		for id, n := range seen {
			if n != 1 {
				t.Fatalf("round %d: job %s reported removed %d times", round, id, n)
			}
		}
		if len(seen) != jobs {
			t.Fatalf("round %d: %d of %d jobs were removed", round, len(seen), jobs)
		}
		if freed != want {
			t.Fatalf("round %d: freed_bytes = %d, want %d", round, freed, want)
		}
	}
}

// The sweep's own concurrency: several sweepers over the same directory must
// still remove each job once. This fixture deliberately gives every job the
// same whole-second StartedAt, matching records written before sub-second
// timestamps were introduced. Without the ID tie-breaker, ReadDir order keeps
// job-a instead of the defined newest job-d. job-d's two-byte payload makes the
// old failure exact and deterministic: freed_bytes is want+2, the same symptom
// that made the original process-based fixture flaky near a PID digit boundary.
func TestConcurrentSweepsHaveOneWinnerPerJob(t *testing.T) {
	for round := 0; round < 10; round++ {
		state := newJobState(t)
		ids := []string{"job-a", "job-b", "job-c", "job-d"}
		jobs := len(ids)
		const keep = 1
		sizes := make(map[string]int64, jobs)
		stamp := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
		for _, id := range ids {
			dir := jobDir(state, id)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := writeJSON(filepath.Join(dir, "meta.json"), &jobMeta{
				ID: id, Argv: []string{"true"}, PID: 999999, StartedAt: stamp,
			}); err != nil {
				t.Fatal(err)
			}
			if err := writeJSON(filepath.Join(dir, "status.json"), map[string]any{
				"exit_code": 0, "ended_at": stamp,
			}); err != nil {
				t.Fatal(err)
			}
			if id == "job-d" {
				if err := os.WriteFile(filepath.Join(dir, "payload"), []byte("xx"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			sizes[id] = measuredDirSize(t, dir)
		}
		// Equal timestamps use descending ID as the documented total order, so
		// job-d is the one keep_last record and a/b/c are the exact reclaim set.
		var want int64
		for _, id := range ids[:jobs-keep] {
			want += sizes[id]
		}

		const sweepers = 4
		var wg sync.WaitGroup
		start := make(chan struct{})
		results := make([]*proto.Response, sweepers)
		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				results[i] = handleSafely(&proto.Request{
					Op: proto.OpJobRm, Job: &proto.JobParams{KeepLast: keep},
				}, state)
			}(i)
		}
		close(start)
		wg.Wait()

		seen := map[string]int{}
		var freed int64
		for i, r := range results {
			if !r.OK {
				t.Fatalf("round %d: sweeper %d errored: %s", round, i, r.Err)
			}
			for _, id := range r.Job.Removed {
				seen[id]++
			}
			freed += r.Job.FreedBytes
		}
		for id, n := range seen {
			if n != 1 {
				t.Fatalf("round %d: job %s removed %d times by concurrent sweeps", round, id, n)
			}
		}
		if len(seen) != jobs-keep {
			t.Fatalf("round %d: %d jobs removed, want %d", round, len(seen), jobs-keep)
		}
		if _, err := os.Stat(jobDir(state, "job-d")); err != nil {
			t.Fatalf("round %d: deterministic keep_last winner job-d was removed: %v", round, err)
		}
		if freed != want {
			t.Fatalf("round %d: freed_bytes = %d, want %d", round, freed, want)
		}
	}
}

// A running job is never removed, and the lock is what makes that hold under
// concurrency: deleting a live job's records would leave the process running with
// no way to observe or stop it.
func TestConcurrentRmNeverRemovesRunningJob(t *testing.T) {
	state := newJobState(t)
	resp := handleSafely(&proto.Request{
		Op:  proto.OpJobStart,
		Job: &proto.JobParams{Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "sleep 5"}}},
	}, state)
	if !resp.OK {
		t.Fatalf("job start: %s", resp.Err)
	}
	id := resp.Job.Info.ID
	t.Cleanup(func() {
		jobStop(&proto.JobParams{ID: id, Signal: "KILL"}, state)
	})

	const racers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]*proto.Response, racers)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = handleSafely(&proto.Request{
				Op: proto.OpJobRm, Job: &proto.JobParams{ID: id},
			}, state)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, r := range results {
		if !r.OK {
			t.Fatalf("racer %d errored: %s", i, r.Err)
		}
		if len(r.Job.Removed) > 0 {
			t.Fatalf("racer %d removed a running job's records, orphaning the process", i)
		}
		if len(r.Job.Skipped) != 1 {
			t.Errorf("racer %d Skipped = %v, want the running job", i, r.Job.Skipped)
		}
	}
	if !jobExists(jobDir(state, id)) {
		t.Error("running job's records are gone")
	}
}

// A job that finishes while it is being removed must not leave a directory
// holding only status.json: job_list skips a directory with no meta.json, so the
// job would vanish rather than report its exit code.
//
// Drives the supervisor's status write against a concurrent removal by starting
// short jobs and removing them at the moment they exit.
func TestRemovalDoesNotResurrectPartialJob(t *testing.T) {
	state := newJobState(t)
	const jobs = 12
	ids := make([]string, 0, jobs)
	for i := 0; i < jobs; i++ {
		resp := handleSafely(&proto.Request{
			Op: proto.OpJobStart,
			Job: &proto.JobParams{
				Spec: &proto.ExecParams{Argv: []string{"sh", "-c", "exit 3"}},
			},
		}, state)
		if !resp.OK {
			t.Fatalf("job start: %s", resp.Err)
		}
		ids = append(ids, resp.Job.Info.ID)
	}

	// Remove without waiting: each rm lands somewhere around its supervisor's
	// status write, which is the interleaving under test.
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			handleSafely(&proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: id}}, state)
		}(id)
	}
	wg.Wait()

	// Give any supervisor that had not yet written its status time to try.
	for _, id := range ids {
		jobWait(&proto.JobParams{ID: id, WaitTimeoutSec: 5}, state)
	}

	entries, err := os.ReadDir(filepath.Join(state, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(state, "jobs", e.Name())
		if _, err := os.Stat(filepath.Join(dir, "meta.json")); os.IsNotExist(err) {
			names, _ := os.ReadDir(dir)
			var got []string
			for _, n := range names {
				got = append(got, n.Name())
			}
			t.Errorf("job %s has no meta.json but still holds %v: a write landed after its removal",
				e.Name(), got)
		}
	}
}

// Lock files must not be mistaken for jobs. They live outside jobs/, so a listing
// neither reports them nor counts them in Total.
func TestLockFilesAreNotVisibleAsJobs(t *testing.T) {
	state := newJobState(t)
	const jobs = 3
	for i := 0; i < jobs; i++ {
		startFinishedJob(t, state)
	}

	res, err := jobList(&proto.JobParams{}, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.List) != jobs {
		t.Errorf("listed %d jobs, want %d", len(res.List), jobs)
	}
	if res.Total != jobs {
		t.Errorf("Total = %d, want %d: something other than a job is being counted", res.Total, jobs)
	}

	// And the locks are where the removal path cannot delete them mid-sweep.
	if _, err := os.Stat(filepath.Join(state, lockDirName)); err != nil {
		t.Errorf("expected a lock directory beside jobs/, got %v", err)
	}
}

// The lock must exclude goroutines within one process, not just separate
// processes. A flock belongs to the open file description, so an implementation
// that cached one descriptor would let same-process callers walk straight through
// each other's critical sections -- and same-process concurrency is the common
// case, since every request runs in its own goroutine.
func TestJobLockExcludesGoroutinesInOneProcess(t *testing.T) {
	state := newJobState(t)
	dir := jobDir(state, "20260101-000000-deadbeef")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	inside, maxInside := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			withJobLock(dir, func() error {
				mu.Lock()
				inside++
				if inside > maxInside {
					maxInside = inside
				}
				mu.Unlock()

				// Hold long enough that an unsynchronized peer would be observed.
				for j := 0; j < 1000; j++ {
					_ = fmt.Sprint(j)
				}

				mu.Lock()
				inside--
				mu.Unlock()
				return nil
			})
		}()
	}
	wg.Wait()

	if maxInside != 1 {
		t.Errorf("%d goroutines were inside the lock at once, want 1", maxInside)
	}
}
