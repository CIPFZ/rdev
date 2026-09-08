package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAuditSinkStallCannotBlockRequestsOrGrowQueue(t *testing.T) {
	a := NewAuditLog(8)
	sink, _, err := newAuditSink(filepath.Join(t.TempDir(), "audit"), 1<<20, 8)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	sink.syncFile = func(f *os.File) error { once.Do(func() { close(entered); <-release }); return f.Sync() }
	a.sink = sink
	go sink.run()
	a.Append(AuditEvent{Owner: "first", Operation: "ping"})
	<-entered
	published := make(chan struct{})
	go func() {
		for i := 0; i < 4096; i++ {
			a.Append(AuditEvent{Owner: "flood", Operation: "ping"})
		}
		close(published)
	}()
	select {
	case <-published:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("blocked audit I/O blocked request append")
	}
	st := a.SinkStatus()
	if st.Dropped == 0 || st.Pending > auditQueueCapacity+64 {
		t.Fatalf("queue not bounded/visible: %+v", st)
	}
	// A concurrently waiting explicit barrier must not hold the memory mutex.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	flushDone := make(chan error, 1)
	go func() { flushDone <- a.Flush(ctx) }()
	a.Append(AuditEvent{Owner: "other", Operation: "ping"})
	if len(a.QueryOwner(time.Time{}, "other")) != 1 {
		t.Fatal("stalled sink lost live owner query")
	}
	if err := <-flushDone; err == nil {
		t.Fatal("stalled barrier claimed durability")
	}
	close(release)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAuditSinkRecoversTornTailAndRotationFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	first := NewAuditLog(32)
	if err := first.ConfigureFile(path, 1024); err != nil {
		t.Fatal(err)
	}
	first.Append(AuditEvent{Owner: "a", Operation: "ping"})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"schema":1,"torn":`); err != nil {
		t.Fatal(err)
	}
	f.Close()
	a := NewAuditLog(32)
	if err := a.ConfigureFile(path, 1024); err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.SinkStatus().Recovered != 1 {
		t.Fatal("torn-record recovery not visible")
	}
	a.Append(AuditEvent{Owner: "a", Operation: "ping"})
	flushAudit(t, a)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if !json.Valid([]byte(line)) {
			t.Fatal("new record appended after corrupt JSON tail")
		}
	}
	// A real directory at the rotation destination forces rename validation to
	// fail. Live events remain queryable and the sink reports lost disk events.
	if err := os.Mkdir(path+".1", 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		a.Append(AuditEvent{Owner: "a", Operation: "ping"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Flush(ctx); err == nil {
		t.Fatal("rotation failure hidden")
	}
	st := a.SinkStatus()
	if st.Errors == 0 || st.Dropped == 0 || st.LastError != "rotation_failed" {
		t.Fatalf("failure not visible: %+v", st)
	}
	if len(a.QueryOwner(time.Time{}, "a")) == 0 {
		t.Fatal("disk failure broke owner queries")
	}
	if err := os.Remove(path + ".1"); err != nil {
		t.Fatal(err)
	}
	a.Append(AuditEvent{Owner: "recovered", Operation: "ping"})
	_ = a.Flush(ctx)
	if err := a.Close(); err == nil {
		t.Fatal("historical sink error disappeared")
	}
	b := NewAuditLog(32)
	if err := b.ConfigureFile(path, 1024); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if len(b.QueryOwner(time.Time{}, "recovered")) != 1 {
		t.Fatal("writer did not recover after rotation obstacle removal")
	}
	if len(b.QueryOwner(time.Time{}, "unrelated")) != 0 {
		t.Fatal("recovery crossed owner scopes")
	}
	for _, segment := range []string{path, path + ".1"} {
		st, err := os.Stat(segment)
		if err != nil || st.Size() > 1024 {
			t.Fatal("rotation exceeded storage budget")
		}
	}
}

func TestAuditSinkRejectsUnsafeAndOversizedRecovery(t *testing.T) {
	for _, kind := range []string{"symlink", "permissions", "oversize", "rotated-oversize"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "audit")
			switch kind {
			case "symlink":
				target := filepath.Join(dir, "target")
				os.WriteFile(target, []byte("keep"), 0600)
				os.Symlink(target, path)
			case "permissions":
				os.WriteFile(path, nil, 0644)
			case "oversize":
				os.WriteFile(path, make([]byte, 1025), 0600)
			case "rotated-oversize":
				os.WriteFile(path+".1", make([]byte, 1025), 0600)
			}
			a := NewAuditLog(8)
			if err := a.ConfigureFile(path, 1024); err == nil {
				a.Close()
				t.Fatal("unsafe audit recovery accepted")
			}
		})
	}
}
