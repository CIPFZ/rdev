package proto

import "testing"

func TestNegotiateBrokerVersion(t *testing.T) {
	if got := NegotiateBrokerVersion(BrokerHello{Version: 3, MinVersion: 1}, BrokerHello{Version: 2, MinVersion: 2}); got != 2 { t.Fatalf("got %d", got) }
	if err := ValidateBrokerHello(BrokerHello{Version: 1, MinVersion: 1}, BrokerHello{Version: 2, MinVersion: 2}); err == nil { t.Fatal("expected incompatibility") }
}

func TestValidateBrokerHelloRejectsMalformedRange(t *testing.T) {
	if err := ValidateBrokerHello(BrokerHello{Version: 1, MinVersion: 1}, BrokerHello{Version: 1, MinVersion: 2}); err == nil { t.Fatal("expected malformed range") }
}
