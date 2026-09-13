package agentrepair

import "testing"

func TestAuthorizationBindsPlanAndConfirmation(t *testing.T) {
	p := Plan{Host: "dev", CurrentDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CandidateDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	tx := Transaction{ID: "tx-1", PlanDigest: Digest(p), Phase: Planned}
	if err := tx.Authorize(p, false); err == nil {
		t.Fatal("accepted without confirmation")
	}
	if err := tx.Authorize(p, true); err != nil {
		t.Fatal(err)
	}
	if err := tx.Authorize(Plan{Host: "other", CurrentDigest: p.CurrentDigest, CandidateDigest: p.CandidateDigest}, true); err == nil {
		t.Fatal("accepted mismatched plan")
	}
}

func TestTransitionsAreStrictAndAbortRecoverable(t *testing.T) {
	tx := Transaction{ID: "tx", Phase: Planned}
	if err := tx.Advance(Prepared); err == nil {
		t.Fatal("skipped authorization")
	}
	if err := tx.Advance(Authorized); err != nil {
		t.Fatal(err)
	}
	if err := tx.Advance(Prepared); err != nil {
		t.Fatal(err)
	}
	if err := tx.Advance(Aborted); err != nil {
		t.Fatal(err)
	}
	if err := tx.Advance(Committed); err == nil {
		t.Fatal("advanced after abort")
	}
}
