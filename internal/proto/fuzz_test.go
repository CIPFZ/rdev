package proto

import (
	"encoding/json"
	"testing"
)

func FuzzErrorEnvelope(f *testing.F) {
	for _, code := range []ErrorCode{CodeAmbiguousOutcome, CodeInvalidRequest} {
		b, _ := json.Marshal(NewError(code, "", StateNotSent))
		f.Add(b)
	}
	f.Add([]byte(`{"code":"future","message":"untrusted"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		var envelope ErrorEnvelope
		if json.Unmarshal(data, &envelope) != nil || envelope.Validate() != nil {
			return
		}
		descriptor, ok := LookupError(envelope.Code)
		if !ok || envelope.Message != descriptor.Message || !ValidExecutionState(envelope.ExecutionState) {
			t.Fatal("unregistered error accepted")
		}
		// Arbitrary peer text must never survive validation, including when the
		// other fields came from an otherwise valid registered envelope.
		envelope.Message += " untrusted peer text"
		if envelope.Validate() == nil {
			t.Fatal("untrusted message accepted")
		}
	})
}
