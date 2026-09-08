package broker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CIPFZ/rdev/internal/proto"
)

func TestJobRegistryMutationPublishesOnlyAfterDurableSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs")
	r := NewJobRegistry()
	if err := r.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	a := JobRef{ID: "same-id", Host: "first-host", Owner: "a\x00p"}
	b := JobRef{ID: a.ID, Host: "other-host", Owner: "b\x00p"}
	for _, ref := range []JobRef{a, b} {
		if err := r.Put(ref); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := os.ReadFile(path)
	entered, release := make(chan struct{}), make(chan struct{})
	r.persist = func(string, []JobRef) error {
		close(entered)
		<-release
		return errors.New("injected pre-rename failure")
	}
	done := make(chan error, 1)
	go func() { done <- r.RemoveOwned(a.Host, a.Owner, a.ID) }()
	<-entered
	read := make(chan bool, 1)
	go func() { got, ok := r.GetHost(b.Host, b.ID); read <- ok && got == b }()
	select {
	case ok := <-read:
		if !ok {
			t.Fatal("other owner's snapshot changed")
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("durable write blocked another owner's query")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("persistence failure hidden")
	}
	if got, ok := r.GetHost(a.Host, a.ID); !ok || got != a {
		t.Fatal("failed deletion changed active ownership")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("failed mutation changed disk snapshot")
	}
	r.persist = saveJobs
	if err := r.RemoveOwned(a.Host, b.Owner, a.ID); err == nil {
		t.Fatal("cross-owner deletion accepted")
	}
	if err := r.RemoveOwned(a.Host, a.Owner, a.ID); err != nil {
		t.Fatal(err)
	}
	if got, ok := r.GetHost(b.Host, b.ID); !ok || got != b {
		t.Fatal("same ID on another host was removed")
	}
	next := NewJobRegistry()
	if err := next.Load(path); err != nil {
		t.Fatal(err)
	}
	if _, ok := next.GetHost(a.Host, a.ID); ok {
		t.Fatal("acknowledged removal was not durable")
	}
	if got, ok := next.GetHost(b.Host, b.ID); !ok || got != b {
		t.Fatal("other host not preserved on restart")
	}
}

func TestJobRegistryUncertainCommitCannotBeOverwrittenOnShutdown(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs")
	r := NewJobRegistry()
	if err := r.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	ref := JobRef{ID: "job", Host: "h", Owner: "a\x00p"}
	r.persist = func(path string, jobs []JobRef) error {
		if err := saveJobs(path, jobs); err != nil {
			return err
		}
		return &jobCommitUncertain{errors.New("late directory sync failure")}
	}
	if err := r.Put(ref); err == nil {
		t.Fatal("uncertain commit acknowledged")
	}
	if _, ok := r.GetHost(ref.Host, ref.ID); ok {
		t.Fatal("uncertain registry used for authorization")
	}
	if !errors.Is(r.Save(path), ErrJobRegistryUnavailable) {
		t.Fatal("shutdown overwrote possibly committed snapshot")
	}
	if err := r.ValidateRequest(ref.Host, ref.Owner, &proto.Request{Op: proto.OpJobList, Job: &proto.JobParams{}}); !errors.Is(err, ErrJobRegistryUnavailable) {
		t.Fatal("failed registry still authorized jobs")
	}
	// Unrelated remote control remains usable when only job state is unavailable.
	if err := r.RecordResponse("h", "a\x00p", &proto.Request{Op: proto.OpPing}, &proto.Response{OK: true, Ping: &proto.PingResult{}}); err != nil {
		t.Fatal("job persistence failure affected ping")
	}
	next := NewJobRegistry()
	if err := next.Load(path); err != nil {
		t.Fatal(err)
	}
	if got, ok := next.GetHost(ref.Host, ref.ID); !ok || got != ref {
		t.Fatal("possibly committed ownership lost")
	}
}

func TestJobRegistryStrictPrivateRecoveryAndLegacyMigration(t *testing.T) {
	valid := `[{"ID":"job","Owner":"a\u0000p","Host":"h"}]`
	for name, data := range map[string]string{"null": "null", "null_jobs": `{"schema":1,"jobs":null}`, "future": `{"schema":2,"jobs":[]}`, "duplicate_schema": `{"schema":1,"schema":1,"jobs":[]}`, "duplicate_owner": `[{"ID":"job","Owner":"a\u0000p","Owner":"b\u0000p","Host":"h"}]`, "duplicate_job": "[" + strings.Trim(valid, "[]") + "," + strings.Trim(valid, "[]") + "]", "bad_owner": `[{"ID":"job","Owner":"o","Host":"h"}]`, "public": "", "oversize": strings.Repeat("x", maxJobRegistryBytes+1), "unknown": `{"schema":1,"jobs":[],"extra":1}`} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs")
			mode := os.FileMode(0600)
			if name == "public" {
				data = valid
				mode = 0644
			}
			if err := os.WriteFile(path, []byte(data), mode); err != nil {
				t.Fatal(err)
			}
			r := NewJobRegistry()
			if err := r.ConfigurePersistence(path); err == nil {
				t.Fatal("invalid recovery accepted")
			}
			after, _ := os.ReadFile(path)
			if string(after) != data {
				t.Fatal("startup failure overwrote invalid state")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "jobs")
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	r := NewJobRegistry()
	if err := r.ConfigurePersistence(path); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if !bytes.HasPrefix(data, []byte(`{"schema":1,`)) {
		t.Fatal("legacy ownership did not migrate")
	}
	if _, ok := r.GetHost("h", "job"); !ok {
		t.Fatal("legacy migration lost ownership")
	}
	link := path + ".link"
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if err := NewJobRegistry().ConfigurePersistence(link); err == nil {
		t.Fatal("symlink registry accepted")
	}
}

func TestJobRecoveryNeverDeletesAnUnconfirmedRemoteRecord(t *testing.T) {
	s := NewService(nil)
	defer s.Close(context.Background())
	refs := []JobRef{{ID: "same", Host: "unavailable", Owner: "a\x00p"}, {ID: "same", Host: "available", Owner: "b\x00p"}}
	for _, ref := range refs {
		if err := s.Jobs.Put(ref); err != nil {
			t.Fatal(err)
		}
	}
	s.SetDispatcher(func(_ context.Context, host string, req *proto.Request) (*proto.Response, error) {
		if host == "unavailable" {
			return nil, errors.New("SSH unavailable")
		}
		return &proto.Response{OK: true, Job: &proto.JobResult{Info: &proto.JobInfo{ID: req.Job.ID}}}, nil
	})
	s.RecoverJobs(t.Context())
	for _, ref := range refs {
		if got, ok := s.Jobs.GetHost(ref.Host, ref.ID); !ok || got != ref {
			t.Fatal("recovery destroyed ownership")
		}
	}
	s.SetDispatcher(func(context.Context, string, *proto.Request) (*proto.Response, error) {
		return &proto.Response{OK: false}, nil
	})
	s.RecoverJobs(t.Context())
	if len(s.Jobs.Snapshot()) != 2 {
		t.Fatal("negative remote response destroyed ownership")
	}
}

func TestJobRequestScopeIsImmutableAndRemovalCannotChangeTarget(t *testing.T) {
	r := NewJobRegistry()
	for _, ref := range []JobRef{{ID: "first", Host: "h", Owner: "a\x00p"}, {ID: "second", Host: "h", Owner: "a\x00p"}, {ID: "other", Host: "h", Owner: "b\x00p"}} {
		if err := r.Put(ref); err != nil {
			t.Fatal(err)
		}
	}
	for _, params := range []*proto.JobParams{nil, {Limit: 1}, {IDs: []string{"first"}, Limit: 1}} {
		req := &proto.Request{Op: proto.OpJobList, Job: params}
		bound, err := r.BindRequest("h", "a\x00p", req)
		if err != nil {
			t.Fatal(err)
		}
		if !bound.Job.FilterIDs || len(bound.Job.IDs) == 0 {
			t.Fatal("list scope missing")
		}
		bound.Job.IDs[0] = "changed"
		if params != nil && (params.FilterIDs || len(params.IDs) > 0 && params.IDs[0] != "first") {
			t.Fatal("caller parameters mutated")
		}
	}
	if _, err := r.BindRequest("h", "a\x00p", &proto.Request{Op: proto.OpJobList, Job: &proto.JobParams{IDs: []string{"other"}}}); err == nil {
		t.Fatal("cross-owner subset accepted")
	}
	req := &proto.Request{Op: proto.OpJobRm, Job: &proto.JobParams{ID: "first"}}
	for _, result := range []*proto.JobResult{{Removed: []string{"second"}}, {Missing: []string{"other"}}} {
		if err := r.RecordResponse("h", "a\x00p", req, &proto.Response{OK: true, Job: result}); err == nil {
			t.Fatal("remote changed approved removal target")
		}
	}
	if len(r.Snapshot()) != 3 {
		t.Fatal("invalid removal changed ownership")
	}
	for _, result := range []*proto.JobResult{
		{Info: &proto.JobInfo{ID: "other"}},
		{Info: &proto.JobInfo{ID: "second"}},
		{Waited: []*proto.WaitedJob{{ID: "first", Info: &proto.JobInfo{ID: "other"}}}},
	} {
		if err := r.RecordResponse("h", "a\x00p", &proto.Request{Op: proto.OpJobWait, Job: &proto.JobParams{ID: "first"}}, &proto.Response{OK: true, Job: result}); err == nil {
			t.Fatal("shared response escaped requested job scope")
		}
	}
}
