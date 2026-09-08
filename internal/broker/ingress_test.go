package broker

import "testing"

func TestIngressOwnerAndByteBudgetsAreIsolatedAndReleased(t *testing.T) {
	i := NewIngress()
	a := Owner{ClientID: "a", ProjectID: "p"}
	b := Owner{ClientID: "a", ProjectID: "other-project"}
	var leases []*IngressLease
	for range MaxOwnerConnections {
		l, err := i.Open()
		if err != nil || l.Bind(a) != nil {
			t.Fatal("owner connection admission failed", err)
		}
		leases = append(leases, l)
		defer l.Close()
	}
	excess, err := i.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer excess.Close()
	if err := excess.Bind(a); err == nil {
		t.Fatal("owner connection budget exceeded")
	}
	if err := excess.Bind(b); err != nil {
		t.Fatal("one owner's connection limit blocked another project", err)
	}
	if err := leases[0].Reserve(MaxOwnerIngressBytes); err != nil {
		t.Fatal(err)
	}
	if err := leases[1].Reserve(1); err == nil {
		t.Fatal("owner byte budget exceeded")
	}
	if err := excess.Reserve(1024); err != nil {
		t.Fatal("owner byte budget blocked other project", err)
	}
	if got := i.Snapshot(b.Key()); got.Connections != 1 || got.Bytes != 1024 {
		t.Fatal("snapshot crossed owner", got)
	}
	leases[0].Close()
	leases[0].Release(MaxOwnerIngressBytes)
	leases[0].Close()
	if err := leases[1].Reserve(MaxOwnerIngressBytes); err != nil {
		t.Fatal("closed connection leaked byte budget", err)
	}
}

func TestAnonymousIngressAdmissionIsBounded(t *testing.T) {
	i := NewIngress()
	var all []*IngressLease
	for range MaxUnauthenticatedConnections {
		l, err := i.Open()
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, l)
		defer l.Close()
	}
	if _, err := i.Open(); err == nil {
		t.Fatal("unbounded unauthenticated connections")
	}
	if err := all[0].Reserve(MaxBrokerHelloBytes + 1); err == nil {
		t.Fatal("unbounded unauthenticated JSON")
	}
	if err := all[0].Bind(Owner{ClientID: "known", ProjectID: "p"}); err != nil {
		t.Fatal(err)
	}
	l, err := i.Open()
	if err != nil {
		t.Fatal("authentication did not release handshake slot", err)
	}
	l.Close()
}

func TestDetachedIngressSurvivesLastSocketAndReleasesOnce(t *testing.T) {
	i := NewIngress()
	owner := Owner{ClientID: "observer", ProjectID: "project"}
	l, _ := i.Open()
	if err := l.Bind(owner); err != nil {
		t.Fatal(err)
	}
	release, err := i.Hold(owner.Key(), MaxOwnerIngressBytes)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	got := i.Snapshot(owner.Key())
	if got.Connections != 0 || got.Bytes != MaxOwnerIngressBytes || got.ObservationBytes != got.Bytes {
		t.Fatal("last socket erased detached observer charge", got)
	}
	if _, err := i.Hold(owner.Key(), 1); err == nil {
		t.Fatal("detached observer exceeded owner budget")
	}
	other, err := i.Hold(Owner{ClientID: owner.ClientID, ProjectID: "other"}.Key(), 1024)
	if err != nil {
		t.Fatal("detached quota crossed project", err)
	}
	other()
	release()
	release()
	if got := i.Snapshot(owner.Key()); got != (IngressSnapshot{}) {
		t.Fatal("detached charge leaked", got)
	}
	if len(i.owners) != 0 || i.bytes != 0 {
		t.Fatal("detached cleanup leaked owners or global bytes")
	}
}
