package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAuditContinuitySurvivesCrashAndLaterCleanExits(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	// Leave the durable open marker behind, just as process death would. A
	// complete segment boundary alone is insufficient evidence of a clean exit.
	first, _, err := newAuditSink(path, 1<<20, 16)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.file.Close(); err != nil {
		t.Fatal(err)
	}
	second := NewAuditLog(16)
	if err := second.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	if st := second.SinkStatus(); !st.Incomplete || st.UncleanRecoveries != 1 {
		t.Fatal("crash tail uncertainty lost", st)
	}
	second.Append(AuditEvent{Owner: "a", Operation: "ping"})
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	marker, known, err := readAuditContinuity(path + ".continuity")
	if err != nil || !known || marker.Active || !marker.Incomplete || marker.UncleanRecoveries != 1 {
		t.Fatal("clean exit erased earlier gap", marker, err)
	}
	third := NewAuditLog(16)
	if err := third.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	defer third.Close()
	if st := third.SinkStatus(); !st.Incomplete || st.UncleanRecoveries != 1 {
		t.Fatal("clean restart reset persistent gap", st)
	}
}

func TestAuditContinuityCleanExitAndLegacyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	for range 2 {
		a := NewAuditLog(16)
		if err := a.ConfigureFile(path, 1<<20); err != nil {
			t.Fatal(err)
		}
		if st := a.SinkStatus(); st.Incomplete || st.UncleanRecoveries != 0 {
			t.Fatal("clean history marked interrupted", st)
		}
		a.Append(AuditEvent{Owner: "a", Operation: "ping"})
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// Existing async segments without a continuity marker have unknown history.
	if err := os.Remove(path + ".continuity"); err != nil {
		t.Fatal(err)
	}
	a := NewAuditLog(16)
	if err := a.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if st := a.SinkStatus(); !st.Incomplete || st.UncleanRecoveries != 0 {
		t.Fatal("legacy async tail silently treated as complete", st)
	}
}

func TestAuditContinuityCloseUsesCallerDeadlineAndPreservesFailedDrain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	a := NewAuditLog(16)
	sink, _, err := newAuditSink(path, 1<<20, 16)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	sink.syncFile = func(f *os.File) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return f.Sync()
	}
	a.sink = sink
	go sink.run()
	a.Append(AuditEvent{Owner: "a", Operation: "ping"})
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := a.CloseContext(ctx, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("audit close ignored caller deadline", err)
	}
	state, _, err := readAuditContinuity(path + ".continuity")
	if err != nil || !state.Active {
		t.Fatal("stalled writer claimed a clean durable close")
	}
	close(release)
	<-sink.done
	state, _, err = readAuditContinuity(path + ".continuity")
	if err != nil || state.Active || !state.Incomplete {
		t.Fatal("failed broker drain was forgotten", state, err)
	}
}

func TestAuditContinuityRejectsCorruptionBeforeRepairingTail(t *testing.T) {
	for _, data := range []string{
		`null`,
		`{"schema":1,"active":false,"incomplete":false,"unclean_recoveries":0}`,
		`{"schema":1,"active":true,"incomplete":false,"unclean_recoveries":0,"seal":{"active_sha256":"bad","rotated_sha256":"bad"}}`,
		`{"schema":1,"active":null,"incomplete":false,"unclean_recoveries":0}`,
		`{"schema":1,"active":true,"incomplete":false,"unclean_recoveries":null}`,
		`{"schema":1,"active":false,"active":true,"incomplete":false,"unclean_recoveries":0}`,
		`{"schema":1,"active":true,"incomplete":false,"unclean_recoveries":1}`,
		`{"schema":1,"active":true,"incomplete":false,"unclean_recoveries":0,"unknown":1}`,
	} {
		path := filepath.Join(t.TempDir(), "audit")
		torn := []byte(`{"torn":`)
		if err := os.WriteFile(path, torn, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path+".continuity", []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := newAuditSink(path, 1024, 16); err == nil {
			t.Fatal("invalid marker accepted")
		}
		after, _ := os.ReadFile(path)
		if string(after) != string(torn) {
			t.Fatal("invalid marker caused audit evidence to be overwritten")
		}
	}
}

func TestAuditContinuityCloseFailureCannotEraseOpenMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	a := NewAuditLog(16)
	if err := a.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	marker := path + ".continuity"
	if err := os.Rename(marker, marker+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(marker, 0700); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err == nil {
		t.Fatal("marker publication failure hidden")
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(marker+".saved", marker); err != nil {
		t.Fatal(err)
	}
	b := NewAuditLog(16)
	if err := b.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if st := b.SinkStatus(); !st.Incomplete || st.UncleanRecoveries != 1 {
		t.Fatal("close failure erased uncertainty", st)
	}
}

func TestAuditContinuityDetectsWriterIgnoringMarker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit")
	a := NewAuditLog(16)
	if err := a.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	a.Append(AuditEvent{Owner: "a", Operation: "ping"})
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	event, _ := json.Marshal(AuditEvent{Schema: 1, Owner: AuditOwnerID("a"), Operation: "ping", At: time.Now()})
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(append(event, '\n')); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	b := NewAuditLog(16)
	if err := b.ConfigureFile(path, 1<<20); err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if st := b.SinkStatus(); !st.Incomplete || st.UncleanRecoveries != 0 {
		t.Fatal("predecessor segment change bypassed continuity check", st)
	}
}
