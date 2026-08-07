// Reclaiming job records.
//
// A job's stdout and stderr are unbounded files, so a machine running batches
// accumulates them until the disk fills, and job_list slows down because it reads
// every directory. This is the only path that deletes them.
//
// A running job is never removed: deleting its records would leave the process
// alive with no way to observe or stop it, which is worse than the disk usage.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

// jobRm deletes job records, either one by ID or a filtered sweep.
//
// A job's stdout and stderr are unbounded files, so a machine running batches
// accumulates them until the disk fills; job_list also slows down because it
// reads every directory. This is the reclaim path.
//
// A running job is never removed, whichever mode is used: deleting its records
// would leave the process alive with no way to observe or stop it, which is worse
// than the disk usage. Such jobs come back in Skipped so the caller knows why
// nothing happened.
func jobRm(p *proto.JobParams, state string) (*proto.JobResult, error) {
	if p.ID != "" {
		return jobRmOne(p.ID, state)
	}
	if p.OlderThanSec <= 0 && p.KeepLast <= 0 {
		return nil, errors.New("job_rm needs an id, older_than_sec, or keep_last")
	}
	return jobRmSweep(p, state)
}

func jobRmOne(id, state string) (*proto.JobResult, error) {
	dir := jobDir(state, id)
	meta, err := readMeta(dir)
	if err != nil {
		return nil, fmt.Errorf("job %s: %w", id, err)
	}
	info := metaToInfo(meta, dir)
	if info.State == proto.JobRunning {
		return &proto.JobResult{Skipped: []string{id}, Info: info}, nil
	}

	size := dirSize(dir)
	if err := os.RemoveAll(dir); err != nil {
		return nil, fmt.Errorf("remove job %s: %w", id, err)
	}
	return &proto.JobResult{Removed: []string{id}, FreedBytes: size}, nil
}

func jobRmSweep(p *proto.JobParams, state string) (*proto.JobResult, error) {
	root := filepath.Join(state, "jobs")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return &proto.JobResult{}, nil
		}
		return nil, err
	}

	type candidate struct {
		id   string
		dir  string
		info *proto.JobInfo
	}
	var finished []candidate
	res := &proto.JobResult{}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		meta, err := readMeta(dir)
		if err != nil {
			continue // half-written or foreign directory
		}
		info := metaToInfo(meta, dir)
		if info.State == proto.JobRunning {
			res.Skipped = append(res.Skipped, info.ID)
			continue
		}
		finished = append(finished, candidate{id: info.ID, dir: dir, info: info})
	}

	// Newest first, so KeepLast retains the most recent jobs.
	sort.Slice(finished, func(i, j int) bool {
		return finished[i].info.StartedAt > finished[j].info.StartedAt
	})

	now := time.Now()
	for i, c := range finished {
		// Both filters must agree: with keep_last=5 and older_than_sec=3600, a
		// recent job inside the keep window stays even if it is old, and a job
		// beyond the window stays if it has not aged out yet. Requiring both makes
		// the combination conservative rather than surprising.
		if p.KeepLast > 0 && i < p.KeepLast {
			continue
		}
		if p.OlderThanSec > 0 && !endedBefore(c.info, now, time.Duration(p.OlderThanSec)*time.Second) {
			continue
		}
		size := dirSize(c.dir)
		if err := os.RemoveAll(c.dir); err != nil {
			continue // a failed removal is not worth failing the whole sweep
		}
		res.Removed = append(res.Removed, c.id)
		res.FreedBytes += size
	}
	return res, nil
}

// endedBefore reports whether a finished job aged past d.
//
// EndedAt is missing for a job whose supervisor died without recording a status,
// so StartedAt is the fallback: it is always present and, for a job that is no
// longer running, is a safe lower bound on when the work stopped.
func endedBefore(info *proto.JobInfo, now time.Time, d time.Duration) bool {
	stamp := info.EndedAt
	if stamp == "" {
		stamp = info.StartedAt
	}
	t, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		return false // an unparseable timestamp should not cause deletion
	}
	return now.Sub(t) > d
}

// dirSize sums the job directory's files so the caller learns what was freed.
func dirSize(dir string) int64 {
	var total int64
	filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
