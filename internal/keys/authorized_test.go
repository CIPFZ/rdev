package keys

import "testing"

func TestAuthorizedKeyAppendIsExactAndIdempotent(t *testing.T) {
	old := "ssh-ed25519 AAAAold old-device\nssh-ed25519 AAAAother keep\n"
	n, changed, err := AddAuthorizedKey(old, "ssh-ed25519 AAAAold", "forged-comment")
	if err != nil || changed || n != old {
		t.Fatalf("forged comment changed key: %v %v", changed, err)
	}
	n, changed, err = AddAuthorizedKey(old, "ssh-ed25519 AAAAnew", "rdev:device:host")
	if err != nil || !changed || n == old {
		t.Fatalf("append failed: %v %v", changed, err)
	}
	n2, changed, err := AddAuthorizedKey(n, "ssh-ed25519 AAAAnew", "other-comment")
	if err != nil || changed || n2 != n {
		t.Fatalf("append not idempotent: %v %v", changed, err)
	}
}

func TestAuthorizedKeyRemovalPreservesOthers(t *testing.T) {
	old := "# keep\nssh-ed25519 AAAAold old\nssh-ed25519 AAAAother other\n"
	n, removed, err := RemoveAuthorizedKey(old, "ssh-ed25519 AAAAold rdev:wrong-comment")
	if err != nil || !removed || n != "# keep\nssh-ed25519 AAAAother other\n" {
		t.Fatalf("remove=%v err=%v output=%q", removed, err, n)
	}
	_, removed, _ = RemoveAuthorizedKey(n, "ssh-ed25519 AAAAmissing")
	if removed {
		t.Fatal("removed unrelated key")
	}
}
