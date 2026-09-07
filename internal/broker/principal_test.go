package broker

import "testing"

func TestPrincipalTokenBindsOwner(t *testing.T) {
	a := Owner{ClientID: "a", ProjectID: "p"}
	b := Owner{ClientID: "b", ProjectID: "p"}
	tok := PrincipalToken("test-secret", a)
	if !ValidatePrincipalToken("test-secret", a, tok) {
		t.Fatal("valid principal token rejected")
	}
	if ValidatePrincipalToken("test-secret", b, tok) {
		t.Fatal("token was reusable for another owner")
	}
	if ValidatePrincipalToken("wrong", a, tok) {
		t.Fatal("token accepted with wrong secret")
	}
}
