package proto

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
)

// OperationClass is the security-relevant effect class of a wire operation.
// It is deliberately conservative: an operation whose arguments can make it
// mutating is classified as mutating for every request.
type OperationClass string

const (
	ClassReadOnly   OperationClass = "read_only"
	ClassIdempotent OperationClass = "idempotent"
	ClassMutating   OperationClass = "mutating"
)

// RetryPolicy declares when the client may send the same operation again.
type RetryPolicy string

const (
	RetryNever        RetryPolicy = "never"
	RetrySafe         RetryPolicy = "safe"
	RetryDeduplicated RetryPolicy = "deduplicated"
)

// ExecutionMode describes the lifetime and resource lane of an operation.
type ExecutionMode string

const (
	ExecutionControl    ExecutionMode = "control"
	ExecutionImmediate  ExecutionMode = "immediate"
	ExecutionForeground ExecutionMode = "foreground"
	ExecutionDetached   ExecutionMode = "detached"
	ExecutionWatcher    ExecutionMode = "watcher"
)

// DisconnectPolicy defines what happens to remote work when the requesting
// connection disappears.
type DisconnectPolicy string

const (
	DisconnectCancel      DisconnectPolicy = "cancel"
	DisconnectComplete    DisconnectPolicy = "complete"
	DisconnectContinue    DisconnectPolicy = "continue_detached"
	DisconnectObserveOnly DisconnectPolicy = "observe_only"
)

// OperationDescriptor is the single shared source of request semantics.
type OperationDescriptor struct {
	Name             string
	Class            OperationClass
	Retry            RetryPolicy
	Execution        ExecutionMode
	Disconnect       DisconnectPolicy
	RequiredFeatures []Feature
	UsesJobParams    bool
}

var operationRegistry = map[string]OperationDescriptor{
	OpPing:            operation(OpPing, ClassReadOnly, RetrySafe, ExecutionControl, DisconnectComplete),
	OpExec:            operation(OpExec, ClassMutating, RetryDeduplicated, ExecutionForeground, DisconnectCancel, FeatureOperationID, FeatureDeduplication, FeatureCancel, FeatureDeadline),
	OpReadFile:        operation(OpReadFile, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureTruncation),
	OpWriteFile:       operation(OpWriteFile, ClassMutating, RetryDeduplicated, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureDeduplication),
	OpJobStart:        jobOperation(OpJobStart, ClassMutating, RetryDeduplicated, ExecutionDetached, DisconnectContinue, FeatureOperationID, FeatureDeduplication),
	OpJobList:         jobOperation(OpJobList, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectObserveOnly, FeatureOperationID),
	OpJobStatus:       jobOperation(OpJobStatus, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectObserveOnly, FeatureOperationID),
	OpJobLogs:         jobOperation(OpJobLogs, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectObserveOnly, FeatureOperationID, FeatureTruncation),
	OpJobStop:         jobOperation(OpJobStop, ClassMutating, RetryDeduplicated, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureDeduplication),
	OpJobWait:         jobOperation(OpJobWait, ClassReadOnly, RetrySafe, ExecutionWatcher, DisconnectObserveOnly, FeatureOperationID),
	OpJobRm:           jobOperation(OpJobRm, ClassMutating, RetryDeduplicated, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureDeduplication),
	OpStorageStatus:   operation(OpStorageStatus, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectObserveOnly, FeatureOperationID),
	OpStorageGC:       operation(OpStorageGC, ClassMutating, RetryDeduplicated, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureDeduplication),
	OpStorageDoctor:   operation(OpStorageDoctor, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectObserveOnly, FeatureOperationID),
	OpStateInspect:    operation(OpStateInspect, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectObserveOnly, FeatureOperationID),
	OpStateMigrate:    operation(OpStateMigrate, ClassMutating, RetryDeduplicated, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureDeduplication),
	OpStateRepair:     operation(OpStateRepair, ClassMutating, RetryDeduplicated, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureDeduplication),
	OpCapabilityProbe: operation(OpCapabilityProbe, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectObserveOnly, FeatureOperationID),
	OpList:            operation(OpList, ClassReadOnly, RetrySafe, ExecutionImmediate, DisconnectComplete, FeatureOperationID, FeatureTruncation),
	OpCancel:          operation(OpCancel, ClassIdempotent, RetrySafe, ExecutionControl, DisconnectComplete, FeatureOperationID, FeatureCancel),
}

func jobOperation(name string, class OperationClass, retry RetryPolicy, execution ExecutionMode, disconnect DisconnectPolicy, features ...Feature) OperationDescriptor {
	descriptor := operation(name, class, retry, execution, disconnect, features...)
	descriptor.UsesJobParams = true
	return descriptor
}

func operation(name string, class OperationClass, retry RetryPolicy, execution ExecutionMode, disconnect DisconnectPolicy, features ...Feature) OperationDescriptor {
	return OperationDescriptor{
		Name: name, Class: class, Retry: retry, Execution: execution,
		Disconnect: disconnect, RequiredFeatures: append([]Feature(nil), features...),
	}
}

// LookupOperation returns an immutable copy of an operation descriptor.
func LookupOperation(name string) (OperationDescriptor, bool) {
	descriptor, ok := operationRegistry[name]
	if !ok {
		return OperationDescriptor{}, false
	}
	descriptor.RequiredFeatures = append([]Feature(nil), descriptor.RequiredFeatures...)
	return descriptor, true
}

// Operations returns every descriptor in stable name order.
func Operations() []OperationDescriptor {
	result := make([]OperationDescriptor, 0, len(operationRegistry))
	for name := range operationRegistry {
		descriptor, _ := LookupOperation(name)
		result = append(result, descriptor)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// RequireOperation fails closed for an unknown operation.
func RequireOperation(name string) (OperationDescriptor, error) {
	if descriptor, ok := LookupOperation(name); ok {
		return descriptor, nil
	}
	return OperationDescriptor{}, NewError(CodeUnknownOperation, "", StateNotSent)
}

// Feature is a negotiated protocol capability. Unknown features are ignored;
// security-sensitive callers must check the required known set explicitly.
type Feature string

const (
	FeatureOperationID   Feature = "operation_id"
	FeatureDeduplication Feature = "deduplication"
	FeatureErrorEnvelope Feature = "error_envelope"
	FeatureCancel        Feature = "cancel"
	FeatureDeadline      Feature = "deadline"
	FeatureStreaming     Feature = "streaming"
	FeatureStreamCredit  Feature = "stream_credit"
	FeatureTruncation    Feature = "truncation_metadata"
)

var supportedFeatures = [...]Feature{
	FeatureOperationID,
	FeatureDeduplication,
	FeatureErrorEnvelope,
	FeatureCancel,
	FeatureDeadline,
	FeatureStreaming,
	FeatureStreamCredit,
	FeatureTruncation,
}

func SupportedFeatures() []Feature {
	return append([]Feature(nil), supportedFeatures[:]...)
}

func IsKnownFeature(feature Feature) bool {
	for _, candidate := range supportedFeatures {
		if feature == candidate {
			return true
		}
	}
	return false
}

// HelloParams is sent with ping by protocol-3 clients.
type HelloParams struct {
	MinVersion int       `json:"min_version"`
	MaxVersion int       `json:"max_version"`
	Features   []Feature `json:"features,omitempty"`
}

func CurrentHello() HelloParams {
	return HelloParams{MinVersion: MinVersion, MaxVersion: Version, Features: SupportedFeatures()}
}

func (h HelloParams) ProtocolRange() ProtocolRange {
	return ProtocolRange{Min: h.MinVersion, Max: h.MaxVersion}
}

type Negotiation struct {
	Version  int       `json:"version"`
	Features []Feature `json:"features,omitempty"`
}

// NegotiateHello selects the highest common protocol version and the known
// feature intersection. Required operation features are checked separately so
// a legacy peer can still serve operations with an explicitly safe fallback.
func NegotiateHello(local, remote HelloParams) (Negotiation, error) {
	version, ok := NegotiateVersion(local.ProtocolRange(), remote.ProtocolRange())
	if !ok {
		return Negotiation{}, NewError(CodeUnsupportedFeature, "", StateNotSent)
	}
	return Negotiation{Version: version, Features: NegotiateFeatures(local.Features, remote.Features)}, nil
}

// ProtocolRange is an inclusive range of wire versions.
type ProtocolRange struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

func (r ProtocolRange) Validate() error {
	if r.Min <= 0 || r.Max <= 0 || r.Min > r.Max {
		return errors.New("invalid protocol range")
	}
	return nil
}

// NegotiateVersion selects the highest common version.
func NegotiateVersion(local, remote ProtocolRange) (int, bool) {
	if local.Validate() != nil || remote.Validate() != nil {
		return 0, false
	}
	low := local.Min
	if remote.Min > low {
		low = remote.Min
	}
	high := local.Max
	if remote.Max < high {
		high = remote.Max
	}
	if low > high {
		return 0, false
	}
	return high, true
}

// NegotiateFeatures returns the known intersection in stable local order.
func NegotiateFeatures(local, remote []Feature) []Feature {
	remoteSet := make(map[Feature]struct{}, len(remote))
	for _, feature := range remote {
		if IsKnownFeature(feature) {
			remoteSet[feature] = struct{}{}
		}
	}
	seen := make(map[Feature]struct{}, len(local))
	result := make([]Feature, 0, len(local))
	for _, feature := range local {
		if !IsKnownFeature(feature) {
			continue
		}
		if _, duplicate := seen[feature]; duplicate {
			continue
		}
		if _, ok := remoteSet[feature]; ok {
			result = append(result, feature)
			seen[feature] = struct{}{}
		}
	}
	return result
}

func SupportsFeature(features []Feature, wanted Feature) bool {
	if !IsKnownFeature(wanted) {
		return false
	}
	for _, feature := range features {
		if feature == wanted {
			return true
		}
	}
	return false
}

// MissingFeatures reports the required operation features not negotiated.
func MissingFeatures(descriptor OperationDescriptor, negotiated []Feature) []Feature {
	var missing []Feature
	for _, required := range descriptor.RequiredFeatures {
		if !SupportsFeature(negotiated, required) {
			missing = append(missing, required)
		}
	}
	return missing
}

type ErrorCode string
type ErrorCategory string
type RetryDisposition string

const (
	CategoryConfig    ErrorCategory = "config"
	CategoryAuth      ErrorCategory = "auth"
	CategoryTransport ErrorCategory = "transport"
	CategoryProtocol  ErrorCategory = "protocol"
	CategoryPolicy    ErrorCategory = "policy"
	CategoryResource  ErrorCategory = "resource"
	CategoryStorage   ErrorCategory = "storage"
	CategoryRemote    ErrorCategory = "remote"
	CategoryProcess   ErrorCategory = "process"
	CategoryInternal  ErrorCategory = "internal"
)

const (
	RetryDispositionSafe            RetryDisposition = "safe"
	RetryDispositionAfterBackoff    RetryDisposition = "after_backoff"
	RetryDispositionAfterUserAction RetryDisposition = "after_user_action"
	RetryDispositionUnsafe          RetryDisposition = "unsafe"
	RetryDispositionNever           RetryDisposition = "never"
)

const (
	CodeUnknownOperation     ErrorCode = "protocol.unknown_operation"
	CodeUnsupportedFeature   ErrorCode = "protocol.unsupported_feature"
	CodeInvalidFrame         ErrorCode = "protocol.invalid_frame"
	CodeFrameTooLarge        ErrorCode = "protocol.frame_too_large"
	CodeInvalidEvent         ErrorCode = "protocol.invalid_event"
	CodeInvalidRequest       ErrorCode = "request.invalid"
	CodeOperationIDConflict  ErrorCode = "request.operation_id_conflict"
	CodeCanceled             ErrorCode = "request.canceled"
	CodeDeadlineExceeded     ErrorCode = "request.deadline_exceeded"
	CodeTransportUnavailable ErrorCode = "transport.unavailable"
	CodeAmbiguousOutcome     ErrorCode = "transport.ambiguous_outcome"
	CodeLimitExceeded        ErrorCode = "resource.limit_exceeded"
	CodeQueueFull            ErrorCode = "resource.queue_full"
	CodeWatcherLimit         ErrorCode = "resource.watcher_limit"
	CodeSlowConsumer         ErrorCode = "resource.slow_consumer"
	CodeObjectNotFound       ErrorCode = "object.not_found"
	CodeProcessStartFailure  ErrorCode = "process.start_failed"
	CodeProcessInvalidState  ErrorCode = "process.invalid_state"
	CodeInternalFailure      ErrorCode = "internal.failure"
)

type ErrorDescriptor struct {
	Code      ErrorCode
	Category  ErrorCategory
	Message   string
	Retry     RetryDisposition
	Retryable bool
	Terminal  bool
}

var errorRegistry = map[ErrorCode]ErrorDescriptor{
	CodeUnknownOperation:     errorDescriptor(CodeUnknownOperation, CategoryProtocol, "unknown operation", RetryDispositionNever, false, true),
	CodeUnsupportedFeature:   errorDescriptor(CodeUnsupportedFeature, CategoryProtocol, "required protocol feature is unavailable", RetryDispositionAfterUserAction, false, true),
	CodeInvalidFrame:         errorDescriptor(CodeInvalidFrame, CategoryProtocol, "invalid protocol frame", RetryDispositionNever, false, true),
	CodeFrameTooLarge:        errorDescriptor(CodeFrameTooLarge, CategoryProtocol, "protocol frame exceeds the hard limit", RetryDispositionNever, false, true),
	CodeInvalidEvent:         errorDescriptor(CodeInvalidEvent, CategoryProtocol, "invalid stream event", RetryDispositionNever, false, true),
	CodeInvalidRequest:       errorDescriptor(CodeInvalidRequest, CategoryProtocol, "invalid request", RetryDispositionNever, false, true),
	CodeOperationIDConflict:  errorDescriptor(CodeOperationIDConflict, CategoryProtocol, "operation identity conflicts with an earlier request", RetryDispositionNever, false, true),
	CodeCanceled:             errorDescriptor(CodeCanceled, CategoryProcess, "operation canceled", RetryDispositionNever, false, true),
	CodeDeadlineExceeded:     errorDescriptor(CodeDeadlineExceeded, CategoryProcess, "operation deadline exceeded", RetryDispositionNever, false, true),
	CodeTransportUnavailable: errorDescriptor(CodeTransportUnavailable, CategoryTransport, "transport unavailable", RetryDispositionAfterBackoff, true, true),
	CodeAmbiguousOutcome:     errorDescriptor(CodeAmbiguousOutcome, CategoryTransport, "operation outcome is ambiguous", RetryDispositionUnsafe, false, true),
	CodeLimitExceeded:        errorDescriptor(CodeLimitExceeded, CategoryResource, "resource limit exceeded", RetryDispositionNever, false, true),
	CodeQueueFull:            errorDescriptor(CodeQueueFull, CategoryResource, "request queue is full", RetryDispositionAfterBackoff, true, true),
	CodeWatcherLimit:         errorDescriptor(CodeWatcherLimit, CategoryResource, "job wait watcher limit reached", RetryDispositionAfterBackoff, true, true),
	CodeSlowConsumer:         errorDescriptor(CodeSlowConsumer, CategoryResource, "stream consumer is too slow", RetryDispositionNever, false, true),
	CodeObjectNotFound:       errorDescriptor(CodeObjectNotFound, CategoryStorage, "requested object was not found", RetryDispositionNever, false, true),
	CodeProcessStartFailure:  errorDescriptor(CodeProcessStartFailure, CategoryProcess, "process could not be started", RetryDispositionNever, false, true),
	CodeProcessInvalidState:  errorDescriptor(CodeProcessInvalidState, CategoryProcess, "process is not in the required state", RetryDispositionNever, false, true),
	CodeInternalFailure:      errorDescriptor(CodeInternalFailure, CategoryInternal, "internal failure", RetryDispositionNever, false, true),
}

func errorDescriptor(code ErrorCode, category ErrorCategory, message string, retry RetryDisposition, retryable, terminal bool) ErrorDescriptor {
	return ErrorDescriptor{Code: code, Category: category, Message: message, Retry: retry, Retryable: retryable, Terminal: terminal}
}

func LookupError(code ErrorCode) (ErrorDescriptor, bool) {
	descriptor, ok := errorRegistry[code]
	return descriptor, ok
}

// ExecutionState states how far an operation is known to have progressed.
type ExecutionState string

const (
	StateNotSent          ExecutionState = "not_sent"
	StateSent             ExecutionState = "sent"
	StateAccepted         ExecutionState = "accepted"
	StatePossiblyExecuted ExecutionState = "possibly_executed"
	StateCompleted        ExecutionState = "completed"
	StateFailed           ExecutionState = "failed"
	StateCanceled         ExecutionState = "canceled"
	StateAmbiguous        ExecutionState = "ambiguous"
)

func validExecutionState(state ExecutionState) bool {
	switch state {
	case StateNotSent, StateSent, StateAccepted, StatePossiblyExecuted, StateCompleted, StateFailed, StateCanceled, StateAmbiguous:
		return true
	default:
		return false
	}
}

func ValidExecutionState(state ExecutionState) bool { return validExecutionState(state) }

// ErrorEnvelope is the stable error shape shared by every protocol surface.
// Message comes from the fixed registry; arbitrary remote text is intentionally
// absent so argv, paths, output, and credentials cannot enter the envelope.
type ErrorEnvelope struct {
	Code           ErrorCode        `json:"code"`
	Category       ErrorCategory    `json:"category"`
	Message        string           `json:"message"`
	Retry          RetryDisposition `json:"retry"`
	Retryable      bool             `json:"retryable"`
	ExecutionState ExecutionState   `json:"execution_state"`
	OperationID    string           `json:"operation_id,omitempty"`
	Terminal       bool             `json:"terminal"`
	Truncation     *Truncation      `json:"truncation,omitempty"`
}

func NewError(code ErrorCode, operationID string, state ExecutionState) *ErrorEnvelope {
	descriptor, ok := LookupError(code)
	if !ok {
		descriptor = errorRegistry[CodeInternalFailure]
	}
	return &ErrorEnvelope{
		Code: descriptor.Code, Category: descriptor.Category, Message: descriptor.Message,
		Retry: descriptor.Retry, Retryable: descriptor.Retryable,
		ExecutionState: state, OperationID: operationID, Terminal: descriptor.Terminal,
	}
}

func (e *ErrorEnvelope) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *ErrorEnvelope) Validate() error {
	if e == nil {
		return errors.New("nil error envelope")
	}
	descriptor, ok := LookupError(e.Code)
	if !ok {
		return errors.New("unknown error code")
	}
	if e.Category != descriptor.Category || e.Message != descriptor.Message || e.Retry != descriptor.Retry || e.Retryable != descriptor.Retryable || e.Terminal != descriptor.Terminal {
		return errors.New("error envelope does not match registry")
	}
	if !validExecutionState(e.ExecutionState) {
		return errors.New("invalid execution state")
	}
	if e.OperationID != "" {
		if err := ValidateOperationID(e.OperationID); err != nil {
			return err
		}
	}
	if e.Truncation != nil {
		if err := e.Truncation.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// EventKind identifies one frame in a streamed response.
type EventKind string

const (
	EventAccepted EventKind = "accepted"
	EventData     EventKind = "data"
	EventProgress EventKind = "progress"
	EventFinal    EventKind = "final"
	EventError    EventKind = "error"
)

type StreamState string

const (
	StreamNew      StreamState = "new"
	StreamAccepted StreamState = "accepted"
	StreamOpen     StreamState = "open"
	StreamTerminal StreamState = "terminal"
)

// AdvanceStreamState validates ordering, monotonic sequence numbers, and the
// exactly-one-terminal invariant. An error event may terminate before accepted
// to represent cancel-before-accepted and admission failures.
func AdvanceStreamState(state StreamState, lastSeq uint64, kind EventKind, seq uint64) (StreamState, uint64, error) {
	if state == StreamTerminal {
		return state, lastSeq, NewError(CodeInvalidEvent, "", StateCompleted)
	}
	if (lastSeq == 0 && seq != 1) || (lastSeq != 0 && seq != lastSeq+1) {
		return state, lastSeq, NewError(CodeInvalidEvent, "", StateAccepted)
	}
	switch kind {
	case EventAccepted:
		if state != StreamNew {
			return state, lastSeq, NewError(CodeInvalidEvent, "", StateAccepted)
		}
		return StreamAccepted, seq, nil
	case EventData, EventProgress:
		if state != StreamAccepted && state != StreamOpen {
			return state, lastSeq, NewError(CodeInvalidEvent, "", StateAccepted)
		}
		return StreamOpen, seq, nil
	case EventFinal:
		if state != StreamAccepted && state != StreamOpen {
			return state, lastSeq, NewError(CodeInvalidEvent, "", StateCompleted)
		}
		return StreamTerminal, seq, nil
	case EventError:
		if state != StreamNew && state != StreamAccepted && state != StreamOpen {
			return state, lastSeq, NewError(CodeInvalidEvent, "", StatePossiblyExecuted)
		}
		return StreamTerminal, seq, nil
	default:
		return state, lastSeq, NewError(CodeInvalidEvent, "", StateAccepted)
	}
}

// Truncation reports exact byte accounting before and after retention.
type Truncation struct {
	Truncated     bool  `json:"truncated"`
	OriginalBytes int64 `json:"original_bytes"`
	RetainedBytes int64 `json:"retained_bytes"`
	DroppedBytes  int64 `json:"dropped_bytes"`
}

func NewTruncation(original, retained int64) (Truncation, error) {
	if original < 0 || retained < 0 || retained > original {
		return Truncation{}, errors.New("invalid truncation byte counts")
	}
	dropped := original - retained
	return Truncation{Truncated: dropped != 0, OriginalBytes: original, RetainedBytes: retained, DroppedBytes: dropped}, nil
}

func (t Truncation) Validate() error {
	if t.OriginalBytes < 0 || t.RetainedBytes < 0 || t.DroppedBytes < 0 || t.RetainedBytes > t.OriginalBytes {
		return errors.New("invalid truncation byte counts")
	}
	if t.OriginalBytes-t.RetainedBytes != t.DroppedBytes || t.Truncated != (t.DroppedBytes != 0) {
		return errors.New("inconsistent truncation metadata")
	}
	return nil
}

const (
	// AbsoluteFrameBytes is the common NDJSON request/response ceiling.
	AbsoluteFrameBytes         int64 = 8 << 20
	AbsoluteRequestFrameBytes  int64 = AbsoluteFrameBytes
	AbsoluteResponseFrameBytes int64 = AbsoluteFrameBytes
	AbsoluteReadBytes          int64 = 4 << 20
	AbsoluteOutputBytes        int64 = 512 << 10
	AbsoluteLineBytes          int64 = 1 << 20
	AbsoluteWaitSeconds        int64 = 3600
	AbsoluteWaitWatchers       int64 = 128
	AbsoluteQueueDepth         int64 = 256
	AbsoluteStreamWindowBytes  int64 = 1 << 20
	AbsoluteStderrBytes        int64 = 64 << 10
	AbsoluteDedupeEntries      int64 = 4096
	AbsoluteDedupeTTLSeconds   int64 = 15 * 60
)

// Limits is the unified set of configurable process-local protocol budgets.
// Zero means use the default. Every nonzero override may only lower a default,
// never exceed the absolute hard limit.
type Limits struct {
	RequestFrameBytes  int64 `json:"request_frame_bytes"`
	ResponseFrameBytes int64 `json:"response_frame_bytes"`
	ReadBytes          int64 `json:"read_bytes"`
	OutputBytes        int64 `json:"output_bytes"`
	LineBytes          int64 `json:"line_bytes"`
	WaitSeconds        int64 `json:"wait_seconds"`
	WaitWatchers       int64 `json:"wait_watchers"`
	QueueDepth         int64 `json:"queue_depth"`
	StreamWindowBytes  int64 `json:"stream_window_bytes"`
	StderrBytes        int64 `json:"stderr_bytes"`
	DedupeEntries      int64 `json:"dedupe_entries"`
	DedupeTTLSeconds   int64 `json:"dedupe_ttl_seconds"`
}

var defaultLimits = Limits{
	RequestFrameBytes:  8 << 20,
	ResponseFrameBytes: 4 << 20,
	ReadBytes:          1 << 20,
	OutputBytes:        256 << 10,
	LineBytes:          256 << 10,
	WaitSeconds:        300,
	WaitWatchers:       32,
	QueueDepth:         64,
	StreamWindowBytes:  256 << 10,
	StderrBytes:        32 << 10,
	DedupeEntries:      1024,
	DedupeTTLSeconds:   5 * 60,
}

var hardLimits = Limits{
	RequestFrameBytes: AbsoluteRequestFrameBytes, ResponseFrameBytes: AbsoluteResponseFrameBytes,
	ReadBytes: AbsoluteReadBytes, OutputBytes: AbsoluteOutputBytes, LineBytes: AbsoluteLineBytes,
	WaitSeconds: AbsoluteWaitSeconds, WaitWatchers: AbsoluteWaitWatchers, QueueDepth: AbsoluteQueueDepth,
	StreamWindowBytes: AbsoluteStreamWindowBytes, StderrBytes: AbsoluteStderrBytes,
	DedupeEntries: AbsoluteDedupeEntries, DedupeTTLSeconds: AbsoluteDedupeTTLSeconds,
}

func DefaultLimitSet() Limits { return defaultLimits }

func AbsoluteLimitSet() Limits { return hardLimits }

// ResolveLimits applies zero-as-default and rejects negative or over-hard-cap
// values. It never clamps an unsafe value into apparent success.
func ResolveLimits(requested Limits) (Limits, error) {
	return resolveLimitsAgainst(requested, defaultLimits, hardLimits)
}

// LowerLimits applies per-request overrides beneath an already-resolved
// process limit set. It rejects any attempt to raise a budget, even when that
// value would remain beneath the absolute system hard cap.
func LowerLimits(base, requested Limits) (Limits, error) {
	if _, err := resolveLimitsAgainst(base, base, hardLimits); err != nil {
		return Limits{}, fmt.Errorf("invalid base limits: %w", err)
	}
	return resolveLimitsAgainst(requested, base, base)
}

func resolveLimitsAgainst(requested, fallback, hard Limits) (Limits, error) {
	result := Limits{}
	fields := []struct {
		name                string
		requested, fallback int64
		hard                int64
		set                 func(int64)
	}{
		{"request_frame_bytes", requested.RequestFrameBytes, fallback.RequestFrameBytes, hard.RequestFrameBytes, func(v int64) { result.RequestFrameBytes = v }},
		{"response_frame_bytes", requested.ResponseFrameBytes, fallback.ResponseFrameBytes, hard.ResponseFrameBytes, func(v int64) { result.ResponseFrameBytes = v }},
		{"read_bytes", requested.ReadBytes, fallback.ReadBytes, hard.ReadBytes, func(v int64) { result.ReadBytes = v }},
		{"output_bytes", requested.OutputBytes, fallback.OutputBytes, hard.OutputBytes, func(v int64) { result.OutputBytes = v }},
		{"line_bytes", requested.LineBytes, fallback.LineBytes, hard.LineBytes, func(v int64) { result.LineBytes = v }},
		{"wait_seconds", requested.WaitSeconds, fallback.WaitSeconds, hard.WaitSeconds, func(v int64) { result.WaitSeconds = v }},
		{"wait_watchers", requested.WaitWatchers, fallback.WaitWatchers, hard.WaitWatchers, func(v int64) { result.WaitWatchers = v }},
		{"queue_depth", requested.QueueDepth, fallback.QueueDepth, hard.QueueDepth, func(v int64) { result.QueueDepth = v }},
		{"stream_window_bytes", requested.StreamWindowBytes, fallback.StreamWindowBytes, hard.StreamWindowBytes, func(v int64) { result.StreamWindowBytes = v }},
		{"stderr_bytes", requested.StderrBytes, fallback.StderrBytes, hard.StderrBytes, func(v int64) { result.StderrBytes = v }},
		{"dedupe_entries", requested.DedupeEntries, fallback.DedupeEntries, hard.DedupeEntries, func(v int64) { result.DedupeEntries = v }},
		{"dedupe_ttl_seconds", requested.DedupeTTLSeconds, fallback.DedupeTTLSeconds, hard.DedupeTTLSeconds, func(v int64) { result.DedupeTTLSeconds = v }},
	}
	for _, field := range fields {
		value, err := ResolveLimit(field.name, field.requested, field.fallback, field.hard)
		if err != nil {
			return Limits{}, err
		}
		field.set(value)
	}
	return result, nil
}

func ResolveLimit(name string, requested, fallback, hard int64) (int64, error) {
	if fallback <= 0 || hard <= 0 || fallback > hard {
		return 0, fmt.Errorf("%s has invalid default or hard limit", name)
	}
	if requested < 0 {
		return 0, fmt.Errorf("%s must not be negative", name)
	}
	if requested == 0 {
		return fallback, nil
	}
	if requested > hard {
		return 0, fmt.Errorf("%s exceeds the absolute hard limit", name)
	}
	return requested, nil
}

func CheckedAdd(a, b int64) (int64, error) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, errors.New("size overflow")
	}
	return a + b, nil
}

func CheckedMultiply(a, b int64) (int64, error) {
	if a < 0 || b < 0 || (a != 0 && b > math.MaxInt64/a) {
		return 0, errors.New("size overflow")
	}
	return a * b, nil
}

// NewOperationID creates a 128-bit random, process-independent identity.
func NewOperationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate operation id: %w", err)
	}
	return "op_" + hex.EncodeToString(raw[:]), nil
}

func ValidateOperationID(id string) error {
	if len(id) < 16 || len(id) > 128 {
		return errors.New("invalid operation id length")
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '.' || r == '_' || r == ':' || r == '-' {
			continue
		}
		return errors.New("invalid operation id character")
	}
	return nil
}

// CanonicalRequestDigest hashes all semantic request fields while excluding
// connection-local request ID, stable operation ID, client identity, and replay
// marker. encoding/json provides deterministic ordering for string-keyed maps.
func CanonicalRequestDigest(request *Request) (string, error) {
	if request == nil {
		return "", errors.New("nil request")
	}
	if _, err := RequireOperation(request.Op); err != nil {
		return "", err
	}
	canonical := struct {
		Op                string         `json:"op"`
		DeadlineUnixMilli int64          `json:"deadline_unix_milli,omitempty"`
		StreamWindowBytes int64          `json:"stream_window_bytes,omitempty"`
		Hello             *HelloParams   `json:"hello,omitempty"`
		Cancel            *CancelParams  `json:"cancel,omitempty"`
		Exec              *ExecParams    `json:"exec,omitempty"`
		Read              *ReadParams    `json:"read,omitempty"`
		Cat               *WriteParams   `json:"write,omitempty"`
		Job               *JobParams     `json:"job,omitempty"`
		List              *ListParams    `json:"list,omitempty"`
		Storage           *StorageParams `json:"storage,omitempty"`
	}{
		Op: request.Op, DeadlineUnixMilli: request.DeadlineUnixMilli, StreamWindowBytes: request.StreamWindowBytes,
		Hello: request.Hello, Cancel: request.Cancel, Exec: request.Exec,
		Read: request.Read, Cat: request.Cat, Job: request.Job, List: request.List, Storage: request.Storage,
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("canonical request: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
