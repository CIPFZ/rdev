package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestJobHistoryRejectsUnrequestedCrossOwnerResponseFields(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	for _, ref := range []JobRef{{ID: "a-job", Host: "h", Owner: "a\x00p"}, {ID: "b-job", Host: "h", Owner: "b\x00p"}} {
		if err := s.Jobs.Put(ref); err != nil {
			t.Fatal(err)
		}
	}
	request := &proto.Request{Op: proto.OpJobStatus, Job: &proto.JobParams{ID: "a-job"}}
	response := &proto.Response{OK: true, Job: &proto.JobResult{Info: &proto.JobInfo{ID: "a-job", State: proto.JobRunning}, List: []*proto.JobInfo{{ID: "b-job", State: proto.JobRunning}}}}
	if err := s.RecordJobResponse("h", "a\x00p", request, response); err == nil {
		t.Fatal("extra response field bypassed owner/target validation")
	}
	if _, err := s.Events.Query("a\x00p", "h", "b-job", JobEventCursor{}, 0); err == nil {
		t.Fatal("other owner's response entered caller's history")
	}
}

func TestJobHistoryRejectsInvalidSnapshotsWithoutOverwriting(t *testing.T) {
	event := testJobObservation("a\x00p", "job", proto.JobRunning)
	event.Stream, _ = proto.NewOperationID()
	event.Sequence, event.At = 1, time.Now().UTC()
	valid, _ := json.Marshal(jobEventSnapshot{Schema: 1, Events: []ownedJobEvent{event}})
	for name, data := range map[string][]byte{
		"null":          []byte("null"),
		"future":        bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":2`), 1),
		"duplicate":     bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":1,"Schema":1`), 1),
		"unknown":       bytes.Replace(valid, []byte(`"schema":1`), []byte(`"schema":1,"extra":true`), 1),
		"invalid_state": bytes.Replace(valid, []byte(`"state":"running"`), []byte(`"state":"invented"`), 1),
		"null_owner":    bytes.Replace(valid, []byte(`"owner":"a\u0000p"`), []byte(`"owner":null`), 1),
		"public":        valid,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "events")
			mode := os.FileMode(0600)
			if name == "public" {
				mode = 0644
			}
			if err := os.WriteFile(path, data, mode); err != nil {
				t.Fatal(err)
			}
			if err := NewJobHistory().ConfigurePersistence(path); err == nil {
				t.Fatal("invalid history accepted")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(data, after) {
				t.Fatal("rejected history overwritten")
			}
		})
	}
}

func testJobObservation(owner, id, state string) ownedJobEvent {
	return ownedJobEvent{Owner: owner, JobEvent: JobEvent{Host: "h", JobID: id, State: state, PID: 123, Operation: proto.OpJobStatus}}
}

func TestJobEventHistoryCoalescesAndRecoversOwnerScopedCursor(t *testing.T) {
	h := NewJobHistory()
	path := filepath.Join(t.TempDir(), "events")
	if err := h.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	var commits atomic.Int32
	h.persist = func(path string, events []ownedJobEvent) error {
		commits.Add(1)
		return saveJobEvents(path, events)
	}
	start := testJobObservation("a\x00p", "job1", proto.JobRunning)
	if _, err := h.Record([]ownedJobEvent{start}); err != nil {
		t.Fatal(err)
	}
	first, err := h.Query(start.Owner, "h", "job1", JobEventCursor{}, 1)
	if err != nil || len(first.Events) != 1 || first.Cursor.Sequence != 1 || first.Truncated {
		t.Fatal("initial event missing", err)
	}
	terminal := start
	terminal.State = proto.JobExited
	terminal.ExitCode = 7
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := h.Record([]ownedJobEvent{terminal}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if commits.Load() != 2 {
		t.Fatal("shared observation persisted duplicate events")
	}
	// A late running status cannot reverse terminal history.
	if _, err := h.Record([]ownedJobEvent{start}); err != nil {
		t.Fatal(err)
	}
	restarted := NewJobHistory()
	if err := restarted.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	page, err := restarted.Query(start.Owner, "h", "job1", first.Cursor, 1)
	if err != nil || len(page.Events) != 1 || page.Events[0].ExitCode != 7 || page.Cursor.Sequence != 2 || page.Truncated || page.More {
		t.Fatal("durable cursor replay failed", err)
	}
	for _, query := range [][3]string{{"a\x00other-project", "h", "job1"}, {start.Owner, "other-host", "job1"}, {start.Owner, "h", "other-job"}} {
		if page, err := restarted.Query(query[0], query[1], query[2], first.Cursor, 1); err == nil || len(page.Events) != 0 {
			t.Fatal("history query crossed scope")
		}
	}
}

func TestJobHistoryRetentionReportsGapWithoutEvictingOtherOwner(t *testing.T) {
	h := NewJobHistory()
	b := testJobObservation("b\x00p", "owned-b", proto.JobRunning)
	if _, err := h.Record([]ownedJobEvent{b}); err != nil {
		t.Fatal(err)
	}
	a := testJobObservation("a\x00p", "hot-job", proto.JobRunning)
	if _, err := h.Record([]ownedJobEvent{a}); err != nil {
		t.Fatal(err)
	}
	first, _ := h.Query(a.Owner, "h", a.JobID, JobEventCursor{}, 1)
	for i := 0; i < maxEventsPerJob+10; i++ {
		a.State = []string{proto.JobUnknown, proto.JobRunning}[i%2]
		if _, err := h.Record([]ownedJobEvent{a}); err != nil {
			t.Fatal(err)
		}
	}
	page, err := h.Query(a.Owner, "h", a.JobID, first.Cursor, 3)
	if err != nil || !page.Truncated || !page.More || len(page.Events) != 3 || page.Events[0].Sequence <= 1 {
		t.Fatal("bounded history silently hid a cursor gap", err)
	}
	for i := 0; i < maxOwnerJobEvents+5; i++ {
		if _, err := h.Record([]ownedJobEvent{testJobObservation(a.Owner, fmt.Sprintf("job-%d", i), proto.JobRunning)}); err != nil {
			t.Fatal(err)
		}
	}
	if page, err := h.Query(b.Owner, "h", b.JobID, JobEventCursor{}, 0); err != nil || len(page.Events) != 1 || page.Truncated {
		t.Fatal("one owner's quota evicted another owner's history", err)
	}
	if _, err := h.Query(a.Owner, "h", a.JobID, first.Cursor, 0); err == nil {
		t.Fatal("expired history reported complete")
	}
	// A new retained history for a fully evicted job has another stream ID.
	if _, err := h.Record([]ownedJobEvent{a}); err != nil {
		t.Fatal(err)
	}
	page, err = h.Query(a.Owner, "h", a.JobID, first.Cursor, 0)
	if err != nil || !page.Truncated || page.Cursor.Stream == first.Cursor.Stream {
		t.Fatal("eviction reset cursor without declaring history loss")
	}
}

func TestJobHistoryPersistenceFailurePreservesActiveSnapshot(t *testing.T) {
	h := NewJobHistory()
	path := filepath.Join(t.TempDir(), "events")
	if err := h.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	event := testJobObservation("a\x00p", "job", proto.JobRunning)
	if _, err := h.Record([]ownedJobEvent{event}); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	h.persist = func(string, []ownedJobEvent) error { return errors.New("pre-rename failure") }
	event.State = proto.JobExited
	if _, err := h.Record([]ownedJobEvent{event}); err == nil {
		t.Fatal("failed event publication acknowledged")
	}
	page, err := h.Query(event.Owner, "h", event.JobID, JobEventCursor{}, 0)
	if err != nil || len(page.Events) != 1 || page.Events[0].State != proto.JobRunning {
		t.Fatal("failed event changed active history")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("failed event changed disk history")
	}
	h.persist = func(path string, events []ownedJobEvent) error {
		if err := saveJobEvents(path, events); err != nil {
			return err
		}
		return &jobHistoryUncertain{errors.New("late fsync uncertainty")}
	}
	if _, err := h.Record([]ownedJobEvent{event}); err == nil {
		t.Fatal("uncertain event durability acknowledged")
	}
	if _, err := h.Query(event.Owner, "h", event.JobID, JobEventCursor{}, 0); !errors.Is(err, ErrJobHistoryUnavailable) {
		t.Fatal("uncertain history remained queryable")
	}
	restarted := NewJobHistory()
	if err := restarted.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	page, err = restarted.Query(event.Owner, "h", event.JobID, JobEventCursor{}, 0)
	if err != nil || len(page.Events) != 2 || page.Events[1].State != proto.JobExited {
		t.Fatal("restart did not preserve committed terminal history")
	}
}
