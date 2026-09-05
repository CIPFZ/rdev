package broker

import "testing"

func TestOwnerValidation(t *testing.T) {
	if err := (Owner{ClientID: "c", ProjectID: "p"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Owner{ClientID: "c"}).Validate(); err == nil {
		t.Fatal("missing project must fail")
	}
	if (Owner{ClientID: "c", ProjectID: "p"}).Key() == (Owner{ClientID: "c", ProjectID: "q"}).Key() {
		t.Fatal("owner key collision")
	}
}
