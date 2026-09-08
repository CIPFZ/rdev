package broker

import "testing"

func TestPrincipalTokenBindsOwner(t *testing.T) {
	a := Owner{ClientID: "a", ProjectID: "p"}
	b := Owner{ClientID: "b", ProjectID: "p"}
	tok := PrincipalToken("01234567890123456789012345678901", a)
	if !ValidatePrincipalToken("01234567890123456789012345678901", a, tok) {
		t.Fatal("valid principal token rejected")
	}
	if ValidatePrincipalToken("01234567890123456789012345678901", b, tok) {
		t.Fatal("token was reusable for another owner")
	}
	if ValidatePrincipalToken("wrong", a, tok) {
		t.Fatal("token accepted with wrong secret")
	}
}
