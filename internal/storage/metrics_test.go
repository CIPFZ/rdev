package storage

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestObserverBoundsPressureEventsAndLabels(t *testing.T) {
	o := NewObserver()
	for i := 0; i < MaxPressureEvents+10; i++ {
		o.ObserveUsage("host-attacker-controlled", int64(i), 1, 2, true)
		o.ObserveUsage("remote_state", int64(i), 1, 2, i%2 == 0)
	}
	s := o.Snapshot()
	if len(s.PressureEvents) != MaxPressureEvents {
		t.Fatalf("events=%d, want %d", len(s.PressureEvents), MaxPressureEvents)
	}
	if s.Local.UsedBytes != 0 || s.RemoteState.BudgetBytes != 2 {
		t.Fatalf("unknown scope mutated metrics: %+v", s)
	}
	for _, e := range s.PressureEvents {
		if e.Scope != "remote_state" || (e.State != "entered" && e.State != "cleared") {
			t.Fatalf("unsafe event label: %+v", e)
		}
	}
}

func TestObserverAccountingAndPersistence(t *testing.T) {
	o := NewObserver()
	o.ObserveLedger(Ledger{OriginalBytes: 10, RetainedBytes: 6, DroppedBytes: 4})
	o.ObserveGC(3, 2, 9, 1)
	s := o.Snapshot()
	if s.Logs.OriginalBytes != 10 || s.Logs.RetainedBytes != 6 || s.Logs.DroppedBytes != 4 || s.GC.ScannedJobs != 3 || s.GC.RemovedJobs != 2 || s.GC.FreedBytes != 9 || s.GC.Errors != 1 {
		t.Fatalf("accounting=%+v", s)
	}
	path := filepath.Join(t.TempDir(), "metrics.json")
	if err := SaveMetrics(path, s); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadMetrics(path)
	if err != nil || !reflect.DeepEqual(loaded.GC, s.GC) {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
}

func TestObserverNilAndInvalidUpdatesNeverBlock(t *testing.T) {
	var o *Observer
	o.ObserveUsage("remote_state", 1, 1, 1, true)
	o.ObserveLedger(Ledger{OriginalBytes: 1})
	o.ObserveGC(1, 1, 1, 1)
	if got := o.Snapshot(); got.SchemaVersion != MetricsSchemaVersion {
		t.Fatalf("nil snapshot schema=%d", got.SchemaVersion)
	}
}
