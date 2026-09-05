package proto

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestOperationRegistryIsCompleteAndConservative(t *testing.T) {
	want := []string{
		OpPing, OpExec, OpReadFile, OpWriteFile, OpJobStart, OpJobList,
		OpJobStatus, OpJobLogs, OpJobStop, OpJobWait, OpJobRm, OpList, OpCancel,
		OpStorageStatus, OpStorageGC, OpStorageDoctor,
		OpStateInspect, OpStateMigrate, OpStateRepair,
	}
	if got := len(Operations()); got != len(want) {
		t.Fatalf("registry has %d operations, want %d", got, len(want))
	}
	classes := map[OperationClass]int{}
	for _, name := range want {
		descriptor, ok := LookupOperation(name)
		if !ok {
			t.Fatalf("wire operation %q is not registered", name)
		}
		if descriptor.Name != name || descriptor.Retry == "" || descriptor.Execution == "" || descriptor.Disconnect == "" {
			t.Errorf("incomplete descriptor for %q: %+v", name, descriptor)
		}
		classes[descriptor.Class]++
		if descriptor.Class == ClassMutating && descriptor.Retry != RetryDeduplicated {
			t.Errorf("mutation %q retry=%q, want deduplicated", name, descriptor.Retry)
		}
	}
	for _, class := range []OperationClass{ClassReadOnly, ClassIdempotent, ClassMutating} {
		if classes[class] == 0 {
			t.Errorf("operation class %q has no operation", class)
		}
	}
	if descriptor, _ := LookupOperation(OpExec); descriptor.Disconnect != DisconnectCancel {
		t.Errorf("exec disconnect=%q, want cancel", descriptor.Disconnect)
	}
	if descriptor, _ := LookupOperation(OpJobStart); descriptor.Disconnect != DisconnectContinue {
		t.Errorf("job_start disconnect=%q, want detached continuation", descriptor.Disconnect)
	}
	if descriptor, _ := LookupOperation(OpJobWait); descriptor.Execution != ExecutionWatcher || descriptor.Disconnect != DisconnectObserveOnly {
		t.Errorf("job_wait semantics=%+v", descriptor)
	}
}

func TestOperationRegistryCannotBeMutatedByCaller(t *testing.T) {
	descriptor, ok := LookupOperation(OpExec)
	if !ok || len(descriptor.RequiredFeatures) == 0 {
		t.Fatal("exec descriptor missing features")
	}
	descriptor.RequiredFeatures[0] = Feature("poisoned")
	again, _ := LookupOperation(OpExec)
	if again.RequiredFeatures[0] == Feature("poisoned") {
		t.Fatal("caller mutated the shared operation registry")
	}
}

func TestUnknownOperationFailsClosed(t *testing.T) {
	if _, ok := LookupOperation("future_dangerous_op"); ok {
		t.Fatal("unknown operation was registered")
	}
	_, err := RequireOperation("future_dangerous_op")
	var envelope *ErrorEnvelope
	if !errors.As(err, &envelope) || envelope.Code != CodeUnknownOperation || envelope.Retryable || envelope.ExecutionState != StateNotSent {
		t.Fatalf("unknown operation error=%#v", err)
	}
}

func TestProtocolAndFeatureNegotiation(t *testing.T) {
	tests := []struct {
		local, remote ProtocolRange
		version       int
		ok            bool
	}{
		{ProtocolRange{2, 3}, ProtocolRange{2, 3}, 3, true},
		{ProtocolRange{2, 3}, ProtocolRange{1, 2}, 2, true},
		{ProtocolRange{3, 3}, ProtocolRange{1, 2}, 0, false},
		{ProtocolRange{0, 3}, ProtocolRange{2, 3}, 0, false},
	}
	for _, test := range tests {
		version, ok := NegotiateVersion(test.local, test.remote)
		if version != test.version || ok != test.ok {
			t.Errorf("NegotiateVersion(%+v,%+v)=(%d,%v), want (%d,%v)", test.local, test.remote, version, ok, test.version, test.ok)
		}
	}

	local := []Feature{FeatureStreaming, FeatureCancel, FeatureCancel, Feature("future")}
	remote := []Feature{FeatureCancel, FeatureStreaming, Feature("remote_future")}
	if got, want := NegotiateFeatures(local, remote), []Feature{FeatureStreaming, FeatureCancel}; !reflect.DeepEqual(got, want) {
		t.Fatalf("features=%v, want %v", got, want)
	}
	if SupportsFeature(NegotiateFeatures(local, remote), Feature("future")) {
		t.Fatal("unknown feature was treated as supported")
	}
	negotiated, err := NegotiateHello(
		CurrentHello(),
		HelloParams{MinVersion: 2, MaxVersion: 2, Features: []Feature{FeatureCancel, Feature("future")}},
	)
	if err != nil || negotiated.Version != 2 || !reflect.DeepEqual(negotiated.Features, []Feature{FeatureCancel}) {
		t.Fatalf("hello negotiation=(%+v,%v)", negotiated, err)
	}
	if _, err := NegotiateHello(CurrentHello(), HelloParams{MinVersion: 1, MaxVersion: 1}); err == nil {
		t.Fatal("hello without a protocol intersection succeeded")
	}

	p := &PingResult{Version: 2, MinVersion: 1}
	if !p.Compatible(Version) {
		t.Fatal("N-1 peer with a range intersection should be compatible")
	}
	if (&PingResult{Version: 1, MinVersion: 1}).Compatible(Version) {
		t.Fatal("peer without a protocol intersection was accepted")
	}
}

func TestMissingRequiredFeatures(t *testing.T) {
	descriptor, _ := LookupOperation(OpExec)
	missing := MissingFeatures(descriptor, []Feature{FeatureOperationID, FeatureDeduplication})
	if !reflect.DeepEqual(missing, []Feature{FeatureCancel, FeatureDeadline}) {
		t.Fatalf("missing=%v", missing)
	}
}

func TestErrorEnvelopeIsRegistryBackedAndStable(t *testing.T) {
	envelope := NewError(CodeAmbiguousOutcome, "op_0123456789abcdef", StatePossiblyExecuted)
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"code":"transport.ambiguous_outcome"`, `"retryable":false`, `"execution_state":"possibly_executed"`, `"terminal":true`} {
		if !strings.Contains(string(encoded), field) {
			t.Errorf("error envelope %s missing %s", encoded, field)
		}
	}

	tampered := *envelope
	tampered.Message = "/secret/path --token=hunter2"
	if err := tampered.Validate(); err == nil {
		t.Fatal("arbitrary error text bypassed the fixed registry")
	}
	unknown := NewError(ErrorCode("future.secret"), "", StateNotSent)
	if unknown.Code != CodeInternalFailure || strings.Contains(unknown.Message, "future.secret") {
		t.Fatalf("unknown error did not safely degrade: %+v", unknown)
	}
}

func TestStreamStateMachineHasExactlyOneTerminal(t *testing.T) {
	state, seq, err := AdvanceStreamState(StreamNew, 0, EventAccepted, 1)
	if err != nil || state != StreamAccepted || seq != 1 {
		t.Fatalf("accepted=(%q,%d,%v)", state, seq, err)
	}
	state, seq, err = AdvanceStreamState(state, seq, EventData, 2)
	if err != nil || state != StreamOpen || seq != 2 {
		t.Fatalf("data=(%q,%d,%v)", state, seq, err)
	}
	state, seq, err = AdvanceStreamState(state, seq, EventProgress, 3)
	if err != nil || state != StreamOpen || seq != 3 {
		t.Fatalf("progress=(%q,%d,%v)", state, seq, err)
	}
	state, seq, err = AdvanceStreamState(state, seq, EventFinal, 4)
	if err != nil || state != StreamTerminal || seq != 4 {
		t.Fatalf("final=(%q,%d,%v)", state, seq, err)
	}
	if _, _, err := AdvanceStreamState(state, seq, EventFinal, 5); err == nil {
		t.Fatal("second terminal event was accepted")
	}
	if _, _, err := AdvanceStreamState(StreamNew, 0, EventData, 1); err == nil {
		t.Fatal("data before accepted was accepted")
	}
	if terminal, _, err := AdvanceStreamState(StreamNew, 0, EventError, 1); err != nil || terminal != StreamTerminal {
		t.Fatalf("pre-accepted error=(%q,%v)", terminal, err)
	}
	if _, _, err := AdvanceStreamState(StreamAccepted, 1, EventData, 3); err == nil {
		t.Fatal("sequence gap was accepted")
	}
	if _, _, err := AdvanceStreamState(StreamNew, 0, EventAccepted, 2); err == nil {
		t.Fatal("stream whose first sequence was not one was accepted")
	}
}

func TestTruncationAccounting(t *testing.T) {
	metadata, err := NewTruncation(100, 40)
	if err != nil {
		t.Fatal(err)
	}
	if !metadata.Truncated || metadata.DroppedBytes != 60 || metadata.OriginalBytes != metadata.RetainedBytes+metadata.DroppedBytes {
		t.Fatalf("metadata=%+v", metadata)
	}
	if err := metadata.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Truncation{
		{OriginalBytes: -1},
		{OriginalBytes: 1, RetainedBytes: 2},
		{OriginalBytes: 5, RetainedBytes: 4, DroppedBytes: 2, Truncated: true},
		{OriginalBytes: 5, RetainedBytes: 5, Truncated: true},
	} {
		if err := bad.Validate(); err == nil {
			t.Errorf("invalid metadata accepted: %+v", bad)
		}
	}
}

func TestLimitsRejectNegativeOverflowAndHardCapBypass(t *testing.T) {
	resolved, err := ResolveLimits(Limits{})
	if err != nil || resolved != DefaultLimitSet() {
		t.Fatalf("defaults=(%+v,%v), want %+v", resolved, err, DefaultLimitSet())
	}
	resolved, err = ResolveLimits(Limits{OutputBytes: 1024, WaitWatchers: 1})
	if err != nil || resolved.OutputBytes != 1024 || resolved.WaitWatchers != 1 {
		t.Fatalf("lower overrides=(%+v,%v)", resolved, err)
	}
	base := DefaultLimitSet()
	if lowered, err := LowerLimits(base, Limits{OutputBytes: 1024}); err != nil || lowered.OutputBytes != 1024 {
		t.Fatalf("lowered limits=(%+v,%v)", lowered, err)
	}
	if _, err := LowerLimits(base, Limits{OutputBytes: base.OutputBytes + 1}); err == nil {
		t.Fatal("caller raised a process limit")
	}
	badBase := base
	badBase.OutputBytes = AbsoluteOutputBytes + 1
	if _, err := LowerLimits(badBase, Limits{}); err == nil {
		t.Fatal("base above the absolute cap was accepted")
	}
	for _, requested := range []Limits{
		{OutputBytes: -1},
		{OutputBytes: AbsoluteOutputBytes + 1},
		{RequestFrameBytes: math.MaxInt64},
	} {
		if _, err := ResolveLimits(requested); err == nil {
			t.Errorf("unsafe limit accepted: %+v", requested)
		}
	}
	if _, err := CheckedAdd(math.MaxInt64, 1); err == nil {
		t.Fatal("addition overflow accepted")
	}
	if _, err := CheckedMultiply(math.MaxInt64, 2); err == nil {
		t.Fatal("multiplication overflow accepted")
	}
	if got, err := CheckedMultiply(1024, 1024); err != nil || got != 1<<20 {
		t.Fatalf("safe multiply=(%d,%v)", got, err)
	}
}

func TestOperationIDGenerationAndValidation(t *testing.T) {
	first, err := NewOperationID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewOperationID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("operation IDs collided")
	}
	if err := ValidateOperationID(first); err != nil {
		t.Fatalf("generated ID invalid: %v", err)
	}
	for _, invalid := range []string{"short", strings.Repeat("a", 129), "op_0123456789\nsecret"} {
		if err := ValidateOperationID(invalid); err == nil {
			t.Errorf("invalid operation ID %q accepted", invalid)
		}
	}
}

func TestCanonicalRequestDigestIgnoresTransportIdentityButBindsSemantics(t *testing.T) {
	first := &Request{
		ID: "1", OperationID: "op_0123456789abcdef", ClientID: "client-a", Replay: false,
		Op: OpExec, DeadlineUnixMilli: 1234,
		Exec: &ExecParams{Argv: []string{"env"}, Env: map[string]string{"B": "2", "A": "1"}},
	}
	second := &Request{
		ID: "99", OperationID: "op_fedcba9876543210", ClientID: "client-b", Replay: true,
		Op: OpExec, DeadlineUnixMilli: 1234,
		Exec: &ExecParams{Argv: []string{"env"}, Env: map[string]string{"A": "1", "B": "2"}},
	}
	digest1, err := CanonicalRequestDigest(first)
	if err != nil {
		t.Fatal(err)
	}
	digest2, err := CanonicalRequestDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if digest1 != digest2 || len(digest1) != 64 {
		t.Fatalf("stable digests=(%q,%q)", digest1, digest2)
	}
	second.Exec.Argv = []string{"env", "changed"}
	digest3, err := CanonicalRequestDigest(second)
	if err != nil {
		t.Fatal(err)
	}
	if digest3 == digest1 {
		t.Fatal("semantic request change did not change digest")
	}
	second.Op = "unknown"
	if _, err := CanonicalRequestDigest(second); err == nil {
		t.Fatal("unknown operation was digested instead of failing closed")
	}
}
